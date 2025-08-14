//! Graceful shutdown and resource management for Seastar-RS
//!
//! Provides coordinated shutdown of all subsystems and proper resource cleanup.

pub mod resources;
pub mod signals;

pub use resources::*;
pub use signals::{install_signal_handlers, setup_panic_shutdown_hook, wait_for_shutdown_signal, ShutdownToken};

use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::{broadcast, Notify};
use crate::{Result, Error};

/// Shutdown phase identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShutdownPhase {
    /// Stop accepting new connections and requests
    StopAccepting = 0,
    /// Drain existing connections and finish processing
    DrainConnections = 1,
    /// Stop all timers and scheduled tasks
    StopTimers = 2,
    /// Shutdown metrics and monitoring
    StopMetrics = 3,
    /// Shutdown SMP and reactors
    StopReactors = 4,
    /// Final cleanup of resources
    FinalCleanup = 5,
}

/// Resource that can be shut down gracefully
#[async_trait::async_trait]
pub trait ShutdownResource: Send {
    /// Get a unique name for this resource
    fn name(&self) -> &str;
    
    /// Get the shutdown phase this resource should be shut down in
    fn shutdown_phase(&self) -> ShutdownPhase;
    
    /// Shutdown this resource
    async fn shutdown(&mut self) -> Result<()>;
    
    /// Get shutdown timeout for this resource
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// Handle to a shutdown resource
pub struct ShutdownHandle {
    name: String,
    phase: ShutdownPhase,
    resource: Arc<Mutex<Box<dyn ShutdownResource>>>,
    shutdown_complete: Arc<Notify>,
    is_shutdown: Arc<AtomicBool>,
}

impl ShutdownHandle {
    pub fn new<T>(resource: T) -> Self 
    where
        T: ShutdownResource + 'static,
    {
        let name = resource.name().to_string();
        let phase = resource.shutdown_phase();
        
        Self {
            name,
            phase,
            resource: Arc::new(Mutex::new(Box::new(resource))),
            shutdown_complete: Arc::new(Notify::new()),
            is_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
    
    pub fn name(&self) -> &str {
        &self.name
    }
    
    pub fn phase(&self) -> ShutdownPhase {
        self.phase
    }
    
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::Relaxed)
    }
    
    pub async fn shutdown(&self) -> Result<()> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        tracing::info!("Shutting down resource: {}", self.name);
        let start = Instant::now();
        
        let timeout = {
            let resource = self.resource.lock().await;
            resource.shutdown_timeout()
        };
        
        // Perform shutdown with timeout
        let shutdown_result = tokio::time::timeout(timeout, async {
            let mut resource = self.resource.lock().await;
            resource.shutdown().await
        }).await;
        
        match shutdown_result {
            Ok(Ok(())) => {
                self.is_shutdown.store(true, Ordering::Relaxed);
                self.shutdown_complete.notify_waiters();
                tracing::info!("Resource {} shut down successfully in {:?}", self.name, start.elapsed());
                Ok(())
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to shutdown resource {}: {}", self.name, e);
                Err(e)
            }
            Err(_) => {
                tracing::error!("Timeout shutting down resource {} after {:?}", self.name, timeout);
                Err(Error::Internal(format!("Resource {} shutdown timeout", self.name)))
            }
        }
    }
    
    pub async fn wait_shutdown(&self) {
        if !self.is_shutdown() {
            self.shutdown_complete.notified().await;
        }
    }
}

/// Central shutdown coordinator
pub struct ShutdownCoordinator {
    resources: Mutex<HashMap<String, ShutdownHandle>>,
    shutdown_initiated: AtomicBool,
    shutdown_complete: AtomicBool,
    shutdown_signal_sender: broadcast::Sender<ShutdownPhase>,
    phase_counter: AtomicU32,
    global_timeout: Duration,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new() -> Self {
        let (shutdown_signal_sender, _) = broadcast::channel(100);
        
        Self {
            resources: Mutex::new(HashMap::new()),
            shutdown_initiated: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_signal_sender,
            phase_counter: AtomicU32::new(0),
            global_timeout: Duration::from_secs(300), // 5 minutes total
        }
    }
    
    /// Register a resource for shutdown management
    pub async fn register<T>(&self, resource: T) -> Result<()>
    where
        T: ShutdownResource + 'static,
    {
        let handle = ShutdownHandle::new(resource);
        let name = handle.name().to_string();
        
        let mut resources = self.resources.lock().await;
        
        if resources.contains_key(&name) {
            return Err(Error::AlreadyExists(format!("Resource '{}' already registered", name)));
        }
        
        tracing::debug!("Registered shutdown resource: {} (phase: {:?})", name, handle.phase());
        resources.insert(name, handle);
        Ok(())
    }
    
    /// Get a shutdown signal receiver
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<ShutdownPhase> {
        self.shutdown_signal_sender.subscribe()
    }
    
    /// Check if shutdown has been initiated
    pub fn is_shutdown_initiated(&self) -> bool {
        self.shutdown_initiated.load(Ordering::Relaxed)
    }
    
    /// Check if shutdown is complete
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete.load(Ordering::Relaxed)
    }
    
    /// Initiate graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        if self.shutdown_initiated.compare_exchange(
            false, 
            true, 
            Ordering::SeqCst, 
            Ordering::Relaxed
        ).is_err() {
            tracing::warn!("Shutdown already initiated");
            return Ok(());
        }
        
        tracing::info!("Initiating graceful shutdown");
        let shutdown_start = Instant::now();
        
        // Collect and group resources by shutdown phase
        let mut phase_groups: HashMap<ShutdownPhase, Vec<ShutdownHandle>> = HashMap::new();
        
        {
            let resources = self.resources.lock().await;
            for handle in resources.values() {
                phase_groups.entry(handle.phase())
                    .or_insert_with(Vec::new)
                    .push(handle.clone());
            }
        }
        
        // Shutdown resources phase by phase
        let mut phases = phase_groups.keys().copied().collect::<Vec<_>>();
        phases.sort();
        
        for phase in phases {
            if let Some(phase_resources) = phase_groups.get(&phase) {
                tracing::info!("Starting shutdown phase: {:?} ({} resources)", phase, phase_resources.len());
                
                // Send phase signal
                let _ = self.shutdown_signal_sender.send(phase);
                self.phase_counter.store(phase as u32, Ordering::Relaxed);
                
                // Shutdown all resources in this phase concurrently
                let mut shutdown_tasks = Vec::new();
                
                for resource in phase_resources {
                    let handle = (*resource).clone();
                    let task = tokio::spawn(async move {
                        handle.shutdown().await
                    });
                    shutdown_tasks.push(task);
                }
                
                // Wait for all resources in this phase to complete
                let mut phase_results = Vec::new();
                for task in shutdown_tasks {
                    match task.await {
                        Ok(result) => phase_results.push(result),
                        Err(e) => {
                            tracing::error!("Shutdown task panicked: {}", e);
                            phase_results.push(Err(Error::Internal("Shutdown task panic".to_string())));
                        }
                    }
                }
                
                // Check for any failures in this phase
                let mut phase_errors = Vec::new();
                for result in phase_results {
                    if let Err(e) = result {
                        phase_errors.push(e);
                    }
                }
                
                if !phase_errors.is_empty() {
                    tracing::error!("Phase {:?} completed with {} errors", phase, phase_errors.len());
                    for error in &phase_errors {
                        tracing::error!("  - {}", error);
                    }
                } else {
                    tracing::info!("Phase {:?} completed successfully", phase);
                }
            }
        }
        
        self.shutdown_complete.store(true, Ordering::Relaxed);
        let shutdown_duration = shutdown_start.elapsed();
        
        if shutdown_duration > self.global_timeout {
            tracing::warn!("Shutdown took longer than expected: {:?} (timeout: {:?})", 
                shutdown_duration, self.global_timeout);
        } else {
            tracing::info!("Graceful shutdown completed in {:?}", shutdown_duration);
        }
        
        Ok(())
    }
    
    /// Force immediate shutdown (emergency)
    pub async fn force_shutdown(&self) -> Result<()> {
        tracing::warn!("Force shutdown initiated - resources may not be cleaned up properly");
        
        self.shutdown_initiated.store(true, Ordering::SeqCst);
        
        // Send final cleanup signal
        let _ = self.shutdown_signal_sender.send(ShutdownPhase::FinalCleanup);
        
        // Give a brief moment for cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        self.shutdown_complete.store(true, Ordering::SeqCst);
        Ok(())
    }
    
    /// Get current shutdown phase
    pub fn current_phase(&self) -> Option<ShutdownPhase> {
        if !self.is_shutdown_initiated() {
            return None;
        }
        
        match self.phase_counter.load(Ordering::Relaxed) {
            0 => Some(ShutdownPhase::StopAccepting),
            1 => Some(ShutdownPhase::DrainConnections),
            2 => Some(ShutdownPhase::StopTimers),
            3 => Some(ShutdownPhase::StopMetrics),
            4 => Some(ShutdownPhase::StopReactors),
            5 => Some(ShutdownPhase::FinalCleanup),
            _ => None,
        }
    }
    
    /// Wait for shutdown to complete
    pub async fn wait_shutdown(&self) {
        while !self.is_shutdown_complete() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    
    /// Set global shutdown timeout
    pub fn set_global_timeout(&mut self, timeout: Duration) {
        self.global_timeout = timeout;
    }
}

impl Clone for ShutdownHandle {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            phase: self.phase,
            resource: self.resource.clone(),
            shutdown_complete: self.shutdown_complete.clone(),
            is_shutdown: self.is_shutdown.clone(),
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Global shutdown coordinator instance
static GLOBAL_SHUTDOWN: once_cell::sync::Lazy<ShutdownCoordinator> = 
    once_cell::sync::Lazy::new(ShutdownCoordinator::new);

/// Get the global shutdown coordinator
pub fn global_shutdown_coordinator() -> &'static ShutdownCoordinator {
    &GLOBAL_SHUTDOWN
}

/// Register a resource with the global shutdown coordinator
pub async fn register_shutdown_resource<T>(resource: T) -> Result<()>
where
    T: ShutdownResource + 'static,
{
    global_shutdown_coordinator().register(resource).await
}

/// Initiate global graceful shutdown
pub async fn shutdown() -> Result<()> {
    global_shutdown_coordinator().shutdown().await
}

/// Force immediate shutdown globally
pub async fn force_shutdown() -> Result<()> {
    global_shutdown_coordinator().force_shutdown().await
}

/// Check if global shutdown has been initiated
pub fn is_shutdown_initiated() -> bool {
    global_shutdown_coordinator().is_shutdown_initiated()
}

/// Check if global shutdown is complete
pub fn is_shutdown_complete() -> bool {
    global_shutdown_coordinator().is_shutdown_complete()
}

/// Get a receiver for shutdown signals
pub fn shutdown_receiver() -> broadcast::Receiver<ShutdownPhase> {
    global_shutdown_coordinator().shutdown_receiver()
}

/// Wait for global shutdown to complete
pub async fn wait_shutdown() {
    global_shutdown_coordinator().wait_shutdown().await;
}

/// Resource wrapper for types that don't implement ShutdownResource
pub struct ResourceWrapper<T> {
    name: String,
    phase: ShutdownPhase,
    resource: Option<T>,
    shutdown_fn: Option<Box<dyn Fn(&mut T) -> Result<()> + Send + Sync>>,
}

impl<T> ResourceWrapper<T> 
where
    T: Send + 'static,
{
    pub fn new(name: String, phase: ShutdownPhase, resource: T) -> Self {
        Self {
            name,
            phase,
            resource: Some(resource),
            shutdown_fn: None,
        }
    }
    
    pub fn with_shutdown_fn<F>(mut self, shutdown_fn: F) -> Self
    where
        F: Fn(&mut T) -> Result<()> + Send + Sync + 'static,
    {
        self.shutdown_fn = Some(Box::new(shutdown_fn));
        self
    }
}

#[async_trait::async_trait]
impl<T> ShutdownResource for ResourceWrapper<T>
where
    T: Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        self.phase
    }
    
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(mut resource) = self.resource.take() {
            if let Some(ref shutdown_fn) = self.shutdown_fn {
                shutdown_fn(&mut resource)?;
            }
        }
        Ok(())
    }
}

/// Convenience macro for registering shutdown resources
#[macro_export]
macro_rules! register_for_shutdown {
    ($name:expr, $phase:expr, $resource:expr) => {
        $crate::shutdown::register_shutdown_resource(
            $crate::shutdown::ResourceWrapper::new(
                $name.to_string(),
                $phase,
                $resource,
            )
        )
    };
    ($name:expr, $phase:expr, $resource:expr, $shutdown_fn:expr) => {
        $crate::shutdown::register_shutdown_resource(
            $crate::shutdown::ResourceWrapper::new(
                $name.to_string(),
                $phase,
                $resource,
            ).with_shutdown_fn($shutdown_fn)
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    
    struct TestResource {
        name: String,
        phase: ShutdownPhase,
        shutdown_called: Arc<AtomicBool>,
    }
    
    impl TestResource {
        fn new(name: String, phase: ShutdownPhase) -> (Self, Arc<AtomicBool>) {
            let shutdown_called = Arc::new(AtomicBool::new(false));
            let resource = Self {
                name,
                phase,
                shutdown_called: shutdown_called.clone(),
            };
            (resource, shutdown_called)
        }
    }
    
    #[async_trait::async_trait]
    impl ShutdownResource for TestResource {
        fn name(&self) -> &str {
            &self.name
        }
        
        fn shutdown_phase(&self) -> ShutdownPhase {
            self.phase
        }
        
        async fn shutdown(&mut self) -> Result<()> {
            self.shutdown_called.store(true, Ordering::Relaxed);
            Ok(())
        }
    }
    
    #[tokio::test]
    async fn test_shutdown_coordinator() {
        let coordinator = ShutdownCoordinator::new();
        
        let (resource1, shutdown1) = TestResource::new("test1".to_string(), ShutdownPhase::StopAccepting);
        let (resource2, shutdown2) = TestResource::new("test2".to_string(), ShutdownPhase::DrainConnections);
        
        coordinator.register(resource1).await.unwrap();
        coordinator.register(resource2).await.unwrap();
        
        assert!(!coordinator.is_shutdown_initiated());
        assert!(!shutdown1.load(Ordering::Relaxed));
        assert!(!shutdown2.load(Ordering::Relaxed));
        
        coordinator.shutdown().await.unwrap();
        
        assert!(coordinator.is_shutdown_complete());
        assert!(shutdown1.load(Ordering::Relaxed));
        assert!(shutdown2.load(Ordering::Relaxed));
    }
    
    #[tokio::test]
    async fn test_shutdown_phases() {
        let coordinator = ShutdownCoordinator::new();
        
        let (early_resource, early_shutdown) = TestResource::new("early".to_string(), ShutdownPhase::StopAccepting);
        let (late_resource, late_shutdown) = TestResource::new("late".to_string(), ShutdownPhase::FinalCleanup);
        
        coordinator.register(early_resource).await.unwrap();
        coordinator.register(late_resource).await.unwrap();
        
        coordinator.shutdown().await.unwrap();
        
        assert!(early_shutdown.load(Ordering::Relaxed));
        assert!(late_shutdown.load(Ordering::Relaxed));
    }
    
    #[tokio::test]
    async fn test_resource_wrapper() {
        let coordinator = ShutdownCoordinator::new();
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_clone = shutdown_called.clone();
        
        let wrapper = ResourceWrapper::new(
            "wrapped_resource".to_string(),
            ShutdownPhase::StopTimers,
            42u32,
        ).with_shutdown_fn(move |_resource| {
            shutdown_called_clone.store(true, Ordering::Relaxed);
            Ok(())
        });
        
        coordinator.register(wrapper).await.unwrap();
        coordinator.shutdown().await.unwrap();
        
        assert!(shutdown_called.load(Ordering::Relaxed));
    }
}