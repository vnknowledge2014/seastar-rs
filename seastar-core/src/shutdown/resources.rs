//! Shutdown resource implementations for core Seastar-RS subsystems

use super::{ShutdownResource, ShutdownPhase};
use crate::Result;
use std::time::Duration;

/// Shutdown resource for SMP services
pub struct SmpShutdownResource {
    smp_service: Option<crate::smp::EnhancedSmpService>,
}

impl SmpShutdownResource {
    pub fn new(smp_service: crate::smp::EnhancedSmpService) -> Self {
        Self {
            smp_service: Some(smp_service),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for SmpShutdownResource {
    fn name(&self) -> &str {
        "smp_service"
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::StopReactors
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            if let Some(mut smp) = self.smp_service.take() {
                tracing::info!("Shutting down SMP service");
                smp.shutdown()?;
            }
            Ok(())
    }
}

/// Shutdown resource for metrics system
pub struct MetricsShutdownResource {
    monitoring_system: Option<crate::metrics::integration::MonitoringSystem>,
}

impl MetricsShutdownResource {
    pub fn new(monitoring_system: crate::metrics::integration::MonitoringSystem) -> Self {
        Self {
            monitoring_system: Some(monitoring_system),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for MetricsShutdownResource {
    fn name(&self) -> &str {
        "metrics_system"
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::StopMetrics
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            if let Some(mut monitoring) = self.monitoring_system.take() {
                tracing::info!("Shutting down metrics system");
                monitoring.shutdown().await;
            }
            Ok(())
    }
}

/// Shutdown resource for HTTP servers
pub struct HttpServerShutdownResource {
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HttpServerShutdownResource {
    pub fn new(
        server_handle: tokio::task::JoinHandle<()>,
        shutdown_signal: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            server_handle: Some(server_handle),
            shutdown_signal: Some(shutdown_signal),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for HttpServerShutdownResource {
    fn name(&self) -> &str {
        "http_server"
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::DrainConnections
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(45)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            tracing::info!("Shutting down HTTP server");
            
            // Send shutdown signal
            if let Some(shutdown_tx) = self.shutdown_signal.take() {
                let _ = shutdown_tx.send(());
            }
            
            // Wait for server to shut down
            if let Some(handle) = self.server_handle.take() {
                if let Err(e) = handle.await {
                    tracing::error!("HTTP server shutdown error: {}", e);
                }
            }
            
            Ok(())
    }
}

/// Shutdown resource for RPC servers
pub struct RpcServerShutdownResource {
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

impl RpcServerShutdownResource {
    pub fn new(
        server_handle: tokio::task::JoinHandle<()>,
        shutdown_signal: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            server_handle: Some(server_handle),
            shutdown_signal: Some(shutdown_signal),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for RpcServerShutdownResource {
    fn name(&self) -> &str {
        "rpc_server"
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::DrainConnections
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(45)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            tracing::info!("Shutting down RPC server");
            
            // Send shutdown signal
            if let Some(shutdown_tx) = self.shutdown_signal.take() {
                let _ = shutdown_tx.send(());
            }
            
            // Wait for server to shut down
            if let Some(handle) = self.server_handle.take() {
                if let Err(e) = handle.await {
                    tracing::error!("RPC server shutdown error: {}", e);
                }
            }
            
            Ok(())
    }
}

/// Shutdown resource for timer systems
pub struct TimerShutdownResource {
    timer_wheel: Option<crate::timer::TimerWheel>,
}

impl TimerShutdownResource {
    pub fn new(timer_wheel: crate::timer::TimerWheel) -> Self {
        Self {
            timer_wheel: Some(timer_wheel),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for TimerShutdownResource {
    fn name(&self) -> &str {
        "timer_system"
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::StopTimers
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(15)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            if let Some(mut timer_wheel) = self.timer_wheel.take() {
                tracing::info!("Shutting down timer system");
                timer_wheel.shutdown().await?;
            }
            Ok(())
    }
}

/// Shutdown resource for file I/O operations
pub struct FileIoShutdownResource {
    name: String,
    file_handles: Vec<std::fs::File>,
}

impl FileIoShutdownResource {
    pub fn new(name: String) -> Self {
        Self {
            name,
            file_handles: Vec::new(),
        }
    }
    
    pub fn add_file_handle(&mut self, file: std::fs::File) {
        self.file_handles.push(file);
    }
}

#[async_trait::async_trait]
impl ShutdownResource for FileIoShutdownResource {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::FinalCleanup
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            tracing::info!("Shutting down file I/O resources: {}", self.name);
            
            // Close all file handles
            let handles = std::mem::take(&mut self.file_handles);
            for (i, _handle) in handles.into_iter().enumerate() {
                // Handles will be closed automatically when dropped
                tracing::debug!("Closing file handle {}", i);
            }
            
            Ok(())
    }
}

/// Generic resource for shutting down any async task
pub struct TaskShutdownResource {
    name: String,
    phase: ShutdownPhase,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TaskShutdownResource {
    pub fn new(
        name: String,
        phase: ShutdownPhase,
        task_handle: tokio::task::JoinHandle<()>,
        shutdown_signal: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            name,
            phase,
            task_handle: Some(task_handle),
            shutdown_signal: Some(shutdown_signal),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for TaskShutdownResource {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        self.phase
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            tracing::info!("Shutting down task: {}", self.name);
            
            // Send shutdown signal
            if let Some(shutdown_tx) = self.shutdown_signal.take() {
                let _ = shutdown_tx.send(());
            }
            
            // Wait for task to complete
            if let Some(handle) = self.task_handle.take() {
                if let Err(e) = handle.await {
                    tracing::error!("Task {} shutdown error: {}", self.name, e);
                }
            }
            
            Ok(())
    }
}

/// Connection pool shutdown resource
pub struct ConnectionPoolShutdownResource {
    name: String,
    // In a real implementation, this would hold actual connection pool handles
    connection_count: std::sync::atomic::AtomicUsize,
}

impl ConnectionPoolShutdownResource {
    pub fn new(name: String) -> Self {
        Self {
            name,
            connection_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ShutdownResource for ConnectionPoolShutdownResource {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::StopAccepting
    }
    
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
    
    async fn shutdown(&mut self) -> Result<()> {
            tracing::info!("Shutting down connection pool: {}", self.name);
            
            // Stop accepting new connections
            let connection_count = self.connection_count.load(std::sync::atomic::Ordering::Relaxed);
            tracing::info!("Draining {} connections from pool: {}", connection_count, self.name);
            
            // In a real implementation, this would:
            // 1. Stop accepting new connections
            // 2. Wait for existing connections to drain
            // 3. Force close remaining connections after timeout
            
            // Simulate draining connections
            let mut remaining = connection_count;
            let start = std::time::Instant::now();
            
            while remaining > 0 && start.elapsed() < Duration::from_secs(45) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                remaining = remaining.saturating_sub(1);
                self.connection_count.store(remaining, std::sync::atomic::Ordering::Relaxed);
                
                if remaining % 10 == 0 {
                    tracing::debug!("Connection pool {} has {} remaining connections", self.name, remaining);
                }
            }
            
            if remaining > 0 {
                tracing::warn!("Force closing {} remaining connections in pool: {}", remaining, self.name);
            }
            
            Ok(())
    }
}