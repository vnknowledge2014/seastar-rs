//! Symmetric multiprocessing (SMP) support for Seastar-RS
//!
//! Provides sharded architecture for scaling across multiple CPU cores.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use crossbeam::channel::{Receiver, Sender, unbounded, bounded};
use crate::task::{Task, FutureTask};
use crate::reactor::{Reactor, ReactorConfig};
use crate::{Error, Result};

/// Identifier for a shard (CPU core)
pub type ShardId = usize;

/// Get the total number of available shards
pub fn shard_count() -> usize {
    num_cpus::get()
}

/// Get the current shard ID
pub fn this_shard_id() -> ShardId {
    // In a real implementation, this would be stored in thread-local storage
    0
}

/// A shard represents a single CPU core with its own reactor
pub struct Shard {
    id: ShardId,
    message_sender: Sender<ShardMessage>,
    message_receiver: Receiver<ShardMessage>,
    stats: Arc<ShardStats>,
}

/// A worker that runs on a single shard
pub struct ShardWorker {
    pub shard_id: ShardId,
    reactor: Reactor,
    message_receiver: Receiver<ShardMessage>,
    stats: Arc<ShardStats>,
    shutdown_flag: Arc<AtomicBool>,
}

impl ShardWorker {
    /// Create a new shard worker
    pub fn new(
        shard_id: ShardId,
        config: ReactorConfig,
        message_receiver: Receiver<ShardMessage>,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Result<Self> {
        let reactor = Reactor::new(config)?;
        let stats = Arc::new(ShardStats {
            shard_id,
            tasks_executed: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            cpu_time: AtomicU64::new(0),
            uptime: Duration::default(),
            cpu_usage: 0.0,
            memory_usage: 0,
        });
        
        Ok(Self {
            shard_id,
            reactor,
            message_receiver,
            stats,
            shutdown_flag,
        })
    }
    
    /// Run the shard worker event loop
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Shard {} starting", self.shard_id);
        
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let iteration_start = Instant::now();
            
            // Process inter-shard messages
            self.process_messages().await?;
            
            // Run one reactor iteration
            tokio::task::yield_now().await;
            
            // Update statistics
            self.update_stats(iteration_start);
            
            // Brief yield to prevent busy loop
            if iteration_start.elapsed() < Duration::from_micros(100) {
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        }
        
        tracing::info!("Shard {} shutting down", self.shard_id);
        Ok(())
    }
    
    /// Process inter-shard messages
    async fn process_messages(&mut self) -> Result<()> {
        while let Ok(message) = self.message_receiver.try_recv() {
            match message {
                ShardMessage::SubmitTask(task) => {
                    let handle = self.reactor.submit_task(task);
                    tracing::debug!("Shard {} received task: {:?}", self.shard_id, handle.id);
                    self.stats.tasks_executed.fetch_add(1, Ordering::Relaxed);
                }
                
                ShardMessage::ScheduleWork(work) => {
                    work();
                    tracing::debug!("Shard {} executed work function", self.shard_id);
                }
                
                ShardMessage::Shutdown => {
                    tracing::info!("Shard {} received shutdown signal", self.shard_id);
                    self.shutdown_flag.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                
            }
            
            self.stats.messages_processed.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    /// Update shard statistics
    fn update_stats(&self, _iteration_start: Instant) {
        // In a real implementation, this would collect actual CPU and memory stats
    }
    
    /// Get shard statistics
    pub fn stats(&self) -> Arc<ShardStats> {
        self.stats.clone()
    }
}

/// Messages that can be sent between shards
enum ShardMessage {
    SubmitTask(Box<dyn Task + Send>),
    ScheduleWork(Box<dyn FnOnce() + Send>),
    Shutdown,
}

/// Statistics for a shard
#[derive(Debug, Default)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub tasks_executed: AtomicU64,
    pub messages_processed: AtomicU64,
    pub cpu_time: AtomicU64,
    pub uptime: Duration,
    pub cpu_usage: f64,
    pub memory_usage: usize,
}

impl Clone for ShardStats {
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id,
            tasks_executed: AtomicU64::new(self.tasks_executed.load(Ordering::Relaxed)),
            messages_processed: AtomicU64::new(self.messages_processed.load(Ordering::Relaxed)),
            cpu_time: AtomicU64::new(self.cpu_time.load(Ordering::Relaxed)),
            uptime: self.uptime,
            cpu_usage: self.cpu_usage,
            memory_usage: self.memory_usage,
        }
    }
}

/// Configuration for the SMP system
#[derive(Debug, Clone)]
pub struct SmpConfig {
    /// Number of shards to use (0 = auto-detect)
    pub num_shards: usize,
    
    /// Reactor configuration for each shard
    pub reactor_config: ReactorConfig,
    
    /// Enable shard affinity (pin threads to specific CPU cores)
    pub shard_affinity: bool,
    
    /// Channel capacity for inter-shard communication
    pub channel_capacity: usize,
    
    /// Enable load balancing across shards
    pub load_balancing: bool,
    
    /// Load balancing check interval
    pub load_balance_interval: Duration,
}

impl Default for SmpConfig {
    fn default() -> Self {
        Self {
            num_shards: 0, // Auto-detect
            reactor_config: ReactorConfig::default(),
            shard_affinity: true,
            channel_capacity: 1000,
            load_balancing: true,
            load_balance_interval: Duration::from_millis(100),
        }
    }
}

impl Shard {
    /// Create a new shard
    pub fn new(id: ShardId) -> Self {
        let (sender, receiver) = unbounded();
        let stats = Arc::new(ShardStats {
            shard_id: id,
            tasks_executed: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            cpu_time: AtomicU64::new(0),
            uptime: Duration::default(),
            cpu_usage: 0.0,
            memory_usage: 0,
        });
        
        Self {
            id,
            message_sender: sender,
            message_receiver: receiver,
            stats,
        }
    }
    
    /// Create a new shard with bounded channel
    pub fn new_with_capacity(id: ShardId, capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        let stats = Arc::new(ShardStats {
            shard_id: id,
            tasks_executed: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            cpu_time: AtomicU64::new(0),
            uptime: Duration::default(),
            cpu_usage: 0.0,
            memory_usage: 0,
        });
        
        Self {
            id,
            message_sender: sender,
            message_receiver: receiver,
            stats,
        }
    }
    
    /// Get the shard ID
    pub fn id(&self) -> ShardId {
        self.id
    }
    
    /// Get shard statistics
    pub fn stats(&self) -> Arc<ShardStats> {
        self.stats.clone()
    }
    
    /// Submit a task to this shard
    pub fn submit_task(&self, task: Box<dyn Task + Send>) -> Result<()> {
        self.message_sender
            .send(ShardMessage::SubmitTask(task))
            .map_err(|_| Error::Internal("Failed to submit task to shard".to_string()))?;
        Ok(())
    }
    
    /// Schedule work on this shard
    pub fn schedule_work<F>(&self, work: F) -> Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.message_sender
            .send(ShardMessage::ScheduleWork(Box::new(work)))
            .map_err(|_| Error::Internal("Failed to schedule work on shard".to_string()))?;
        Ok(())
    }
    
    /// Spawn a future as a task on this shard
    pub fn spawn<F>(&self, future: F) -> Result<()>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = Box::new(FutureTask::new(future));
        self.submit_task(task)
    }
    
    /// Shutdown this shard
    pub fn shutdown(&self) -> Result<()> {
        self.message_sender
            .send(ShardMessage::Shutdown)
            .map_err(|_| Error::Internal("Failed to shutdown shard".to_string()))?;
        Ok(())
    }
    
    /// Process messages sent to this shard
    pub fn process_messages(&self) -> Result<Vec<Box<dyn Task + Send>>> {
        let mut tasks = Vec::new();
        
        while let Ok(message) = self.message_receiver.try_recv() {
            match message {
                ShardMessage::SubmitTask(task) => {
                    tasks.push(task);
                }
                ShardMessage::ScheduleWork(work) => {
                    work();
                }
                ShardMessage::Shutdown => {
                    return Ok(tasks);
                }
            }
        }
        
        Ok(tasks)
    }
}

/// Enhanced SMP service with per-shard reactors
pub struct EnhancedSmpService {
    config: SmpConfig,
    shard_workers: Vec<JoinHandle<Result<()>>>,
    shard_senders: HashMap<ShardId, Sender<ShardMessage>>,
    shutdown_flags: Vec<Arc<AtomicBool>>,
    shard_stats: Vec<Arc<ShardStats>>,
    next_shard: AtomicUsize,
    total_shards: usize,
}

impl EnhancedSmpService {
    /// Initialize the enhanced SMP system
    pub fn new(config: SmpConfig) -> Result<Self> {
        let num_shards = if config.num_shards == 0 {
            num_cpus::get()
        } else {
            config.num_shards
        };
        
        tracing::info!("Initializing enhanced SMP system with {} shards", num_shards);
        
        let mut shard_workers = Vec::new();
        let mut shard_senders = HashMap::new();
        let mut shutdown_flags = Vec::new();
        let mut shard_stats = Vec::new();
        
        // Create workers for each shard
        for shard_id in 0..num_shards {
            let shutdown_flag = Arc::new(AtomicBool::new(false));
            shutdown_flags.push(shutdown_flag.clone());
            
            let (sender, receiver) = if config.channel_capacity > 0 {
                bounded(config.channel_capacity)
            } else {
                unbounded()
            };
            
            shard_senders.insert(shard_id, sender);
            
            // Create shard worker
            let mut worker = ShardWorker::new(
                shard_id,
                config.reactor_config.clone(),
                receiver,
                shutdown_flag.clone(),
            )?;
            
            shard_stats.push(worker.stats());
            
            // Spawn the worker thread
            let worker_handle = thread::Builder::new()
                .name(format!("seastar-shard-{}", shard_id))
                .spawn(move || {
                    // Create a tokio runtime for this shard
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    
                    rt.block_on(async move {
                        worker.run().await
                    })
                })?;
            
            shard_workers.push(worker_handle);
        }
        
        Ok(Self {
            config,
            shard_workers,
            shard_senders,
            shutdown_flags,
            shard_stats,
            next_shard: AtomicUsize::new(0),
            total_shards: num_shards,
        })
    }
    
    /// Submit a task to a specific shard
    pub fn submit_to_shard(&self, shard_id: ShardId, task: Box<dyn Task + Send>) -> Result<()> {
        if shard_id >= self.total_shards {
            return Err(Error::InvalidArgument(format!("Invalid shard ID: {}", shard_id)));
        }
        
        let sender = self.shard_senders.get(&shard_id)
            .ok_or_else(|| Error::Internal(format!("No sender for shard {}", shard_id)))?;
        
        sender.send(ShardMessage::SubmitTask(task))
            .map_err(|_| Error::Internal("Failed to send task to shard".to_string()))?;
        
        Ok(())
    }
    
    /// Submit a task to any shard (load balanced)
    pub fn submit(&self, task: Box<dyn Task + Send>) -> Result<ShardId> {
        let shard_id = if self.config.load_balancing {
            self.select_best_shard()
        } else {
            self.next_shard.fetch_add(1, Ordering::Relaxed) % self.total_shards
        };
        
        self.submit_to_shard(shard_id, task)?;
        Ok(shard_id)
    }
    
    /// Spawn a future on any shard
    pub fn spawn<F>(&self, future: F) -> Result<ShardId>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = Box::new(FutureTask::new(future));
        self.submit(task)
    }
    
    /// Spawn a future on a specific shard
    pub fn spawn_on_shard<F>(&self, shard_id: ShardId, future: F) -> Result<()>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = Box::new(FutureTask::new(future));
        self.submit_to_shard(shard_id, task)
    }
    
    /// Select the best shard for load balancing
    fn select_best_shard(&self) -> ShardId {
        // Simple round-robin for now
        self.next_shard.fetch_add(1, Ordering::Relaxed) % self.total_shards
    }
    
    /// Get the number of shards
    pub fn shard_count(&self) -> usize {
        self.total_shards
    }
    
    /// Get statistics for all shards
    pub fn all_stats(&self) -> Vec<Arc<ShardStats>> {
        self.shard_stats.clone()
    }
    
    /// Shutdown all shards gracefully
    pub fn shutdown(&mut self) -> Result<()> {
        tracing::info!("Shutting down enhanced SMP system with {} shards", self.total_shards);
        
        // Signal shutdown to all shards
        for (shard_id, sender) in &self.shard_senders {
            if let Err(e) = sender.send(ShardMessage::Shutdown) {
                tracing::warn!("Failed to send shutdown to shard {}: {}", shard_id, e);
            }
        }
        
        // Set shutdown flags
        for flag in &self.shutdown_flags {
            flag.store(true, Ordering::Relaxed);
        }
        
        // Wait for all worker threads to finish
        let workers = std::mem::take(&mut self.shard_workers);
        for worker in workers {
            if let Err(e) = worker.join() {
                tracing::error!("Shard worker thread panicked: {:?}", e);
            }
        }
        
        tracing::info!("Enhanced SMP system shutdown complete");
        Ok(())
    }
}

impl Drop for EnhancedSmpService {
    fn drop(&mut self) {
        if !self.shard_workers.is_empty() {
            let _ = self.shutdown();
        }
    }
}

/// Service that manages all shards in the system (legacy implementation)
pub struct SmpService {
    shards: Vec<Shard>,
    current_shard: AtomicUsize,
}

impl SmpService {
    /// Create a new SMP service with the specified number of shards
    pub fn new(num_shards: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        
        for id in 0..num_shards {
            shards.push(Shard::new(id));
        }
        
        Self {
            shards,
            current_shard: AtomicUsize::new(0),
        }
    }
    
    /// Create a new SMP service with one shard per CPU core
    pub fn new_with_cpu_count() -> Self {
        Self::new(shard_count())
    }
    
    /// Get the number of shards
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
    
    /// Get a reference to a specific shard
    pub fn shard(&self, id: ShardId) -> Option<&Shard> {
        self.shards.get(id)
    }
    
    /// Submit a task to a specific shard
    pub fn submit_to(&self, shard_id: ShardId, task: Box<dyn Task + Send>) -> Result<()> {
        if let Some(shard) = self.shard(shard_id) {
            shard.submit_task(task)
        } else {
            Err(Error::InvalidArgument(format!("Invalid shard ID: {}", shard_id)))
        }
    }
    
    /// Submit a task to the next shard in round-robin fashion
    pub fn submit_round_robin(&self, task: Box<dyn Task + Send>) -> Result<()> {
        let shard_id = self.current_shard.fetch_add(1, Ordering::SeqCst) % self.shards.len();
        self.submit_to(shard_id, task)
    }
    
    /// Submit a task to the current shard
    pub fn submit_to_current(&self, task: Box<dyn Task + Send>) -> Result<()> {
        let current_id = this_shard_id();
        self.submit_to(current_id, task)
    }
    
    /// Get statistics for all shards
    pub fn all_stats(&self) -> Vec<Arc<ShardStats>> {
        self.shards.iter().map(|shard| shard.stats()).collect()
    }
    
    /// Shutdown all shards
    pub fn shutdown_all(&self) -> Result<()> {
        for shard in &self.shards {
            shard.shutdown()?;
        }
        Ok(())
    }
}

/// Submit a task to a specific shard
pub fn submit_to<T>(_shard_id: ShardId, _task: T) -> Result<()>
where
    T: Task + Send + 'static,
{
    // In a real implementation, this would access a global SMP service
    Err(Error::Internal("SMP service not initialized".to_string()))
}

/// Submit a task to the current shard
pub fn submit_to_current<T>(task: T) -> Result<()>
where
    T: Task + Send + 'static,
{
    submit_to(this_shard_id(), task)
}

// Global enhanced SMP instance
static mut GLOBAL_ENHANCED_SMP: Option<EnhancedSmpService> = None;
static ENHANCED_SMP_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global enhanced SMP system
pub fn initialize_enhanced_smp(config: SmpConfig) -> Result<()> {
    ENHANCED_SMP_INIT.call_once(|| {
        match EnhancedSmpService::new(config) {
            Ok(smp) => {
                unsafe {
                    GLOBAL_ENHANCED_SMP = Some(smp);
                }
            }
            Err(e) => {
                tracing::error!("Failed to initialize enhanced SMP system: {}", e);
                panic!("Enhanced SMP initialization failed: {}", e);
            }
        }
    });
    Ok(())
}

/// Get a reference to the global enhanced SMP system
pub fn enhanced_smp() -> &'static EnhancedSmpService {
    unsafe {
        GLOBAL_ENHANCED_SMP.as_ref().expect("Enhanced SMP system not initialized")
    }
}

/// Submit a future to the enhanced SMP system
pub fn spawn_enhanced<F>(future: F) -> Result<ShardId>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    enhanced_smp().spawn(future)
}

/// Spawn a future on a specific shard
pub fn spawn_on_shard_enhanced<F>(shard_id: ShardId, future: F) -> Result<()>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    enhanced_smp().spawn_on_shard(shard_id, future)
}

/// Get the current number of shards (enhanced)
pub fn enhanced_shard_count() -> usize {
    enhanced_smp().shard_count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::make_closure_task;
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    
    #[test]
    fn test_shard_creation() {
        let shard = Shard::new(0);
        assert_eq!(shard.id(), 0);
    }
    
    #[test]
    fn test_smp_service() {
        let smp = SmpService::new(4);
        assert_eq!(smp.shard_count(), 4);
        
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();
        
        let task = make_closure_task(move || {
            executed_clone.store(true, Ordering::SeqCst);
        });
        
        smp.submit_to(0, task).unwrap();
        
        // In a real test, we'd process the messages on the shard
        // For now, just verify the task was submitted without error
    }
    
    #[test]
    fn test_enhanced_smp_config() {
        let config = SmpConfig::default();
        assert_eq!(config.num_shards, 0); // Auto-detect
        assert!(config.load_balancing);
        assert!(config.shard_affinity);
    }
}