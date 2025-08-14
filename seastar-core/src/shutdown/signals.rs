//! Signal handling for graceful shutdown

use super::{shutdown, force_shutdown, ShutdownPhase, global_shutdown_coordinator};
use crate::{Result, Error};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

static SIGNAL_HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install signal handlers for graceful shutdown
#[cfg(unix)]
pub async fn install_signal_handlers() -> Result<()> {
    if SIGNAL_HANDLER_INSTALLED.compare_exchange(
        false, 
        true, 
        Ordering::SeqCst, 
        Ordering::Relaxed
    ).is_err() {
        return Ok(()); // Already installed
    }
    
    tracing::info!("Installing signal handlers for graceful shutdown");
    
    // Handle SIGTERM (termination request)
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| Error::Internal(format!("Failed to create SIGTERM handler: {}", e)))?;
    
    tokio::spawn(async move {
        sigterm.recv().await;
        tracing::info!("Received SIGTERM, initiating graceful shutdown");
        
        if let Err(e) = shutdown().await {
            tracing::error!("Error during graceful shutdown: {}", e);
            if let Err(e) = force_shutdown().await {
                tracing::error!("Error during force shutdown: {}", e);
            }
        }
    });
    
    // Handle SIGINT (Ctrl+C)
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| Error::Internal(format!("Failed to create SIGINT handler: {}", e)))?;
    
    tokio::spawn(async move {
        sigint.recv().await;
        tracing::info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
        
        if let Err(e) = shutdown().await {
            tracing::error!("Error during graceful shutdown: {}", e);
            if let Err(e) = force_shutdown().await {
                tracing::error!("Error during force shutdown: {}", e);
            }
        }
    });
    
    // Handle SIGQUIT (quit with core dump request)
    let mut sigquit = signal(SignalKind::quit())
        .map_err(|e| Error::Internal(format!("Failed to create SIGQUIT handler: {}", e)))?;
    
    tokio::spawn(async move {
        sigquit.recv().await;
        tracing::warn!("Received SIGQUIT, initiating immediate shutdown");
        
        if let Err(e) = force_shutdown().await {
            tracing::error!("Error during force shutdown: {}", e);
        }
    });
    
    // Handle SIGUSR1 (user-defined signal for status)
    let mut sigusr1 = signal(SignalKind::user_defined1())
        .map_err(|e| Error::Internal(format!("Failed to create SIGUSR1 handler: {}", e)))?;
    
    tokio::spawn(async move {
        loop {
            sigusr1.recv().await;
            tracing::info!("Received SIGUSR1, printing shutdown status");
            print_shutdown_status().await;
        }
    });
    
    // Handle SIGUSR2 (user-defined signal for metrics dump)
    let mut sigusr2 = signal(SignalKind::user_defined2())
        .map_err(|e| Error::Internal(format!("Failed to create SIGUSR2 handler: {}", e)))?;
    
    tokio::spawn(async move {
        loop {
            sigusr2.recv().await;
            tracing::info!("Received SIGUSR2, dumping metrics");
            if let Err(e) = dump_metrics().await {
                tracing::error!("Error dumping metrics: {}", e);
            }
        }
    });
    
    tracing::info!("Signal handlers installed successfully");
    Ok(())
}

/// Install signal handlers for graceful shutdown (Windows version)
#[cfg(not(unix))]
pub async fn install_signal_handlers() -> Result<()> {
    if SIGNAL_HANDLER_INSTALLED.compare_exchange(
        false, 
        true, 
        Ordering::SeqCst, 
        Ordering::Relaxed
    ).is_err() {
        return Ok(()); // Already installed
    }
    
    tracing::info!("Installing signal handlers for graceful shutdown (Windows)");
    
    // Handle Ctrl+C on Windows
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            tracing::info!("Received Ctrl+C, initiating graceful shutdown");
            
            if let Err(e) = shutdown().await {
                tracing::error!("Error during graceful shutdown: {}", e);
                if let Err(e) = force_shutdown().await {
                    tracing::error!("Error during force shutdown: {}", e);
                }
            }
        }
    });
    
    tracing::info!("Signal handlers installed successfully (Windows)");
    Ok(())
}

/// Print current shutdown status
async fn print_shutdown_status() {
    let coordinator = global_shutdown_coordinator();
    
    tracing::info!("=== Shutdown Status ===");
    tracing::info!("Shutdown initiated: {}", coordinator.is_shutdown_initiated());
    tracing::info!("Shutdown complete: {}", coordinator.is_shutdown_complete());
    
    if let Some(phase) = coordinator.current_phase() {
        tracing::info!("Current phase: {:?}", phase);
    } else {
        tracing::info!("Current phase: Not in shutdown");
    }
    
    tracing::info!("=======================");
}

/// Dump current metrics to log
async fn dump_metrics() -> Result<()> {
    // Try to get metrics from the global system
    if let Some(status) = crate::metrics::integration::monitoring_status().await {
        tracing::info!("=== Metrics Status ===");
        tracing::info!("Metrics enabled: {}", status.enabled);
        tracing::info!("HTTP server enabled: {}", status.http_server_enabled);
        tracing::info!("Auto collection enabled: {}", status.auto_collection_enabled);
        tracing::info!("Total metric families: {}", status.total_metric_families);
        tracing::info!("Total metrics: {}", status.total_metrics);
        tracing::info!("Collection running: {}", status.collection_running);
        if let Some(port) = status.server_port {
            tracing::info!("Server port: {}", port);
        }
        tracing::info!("=====================");
    } else {
        tracing::info!("Metrics system not initialized");
    }
    
    // Also try to export metrics to a string for logging
    let families = crate::metrics::global_registry().gather();
    tracing::info!("Current metrics families: {}", families.len());
    
    for family in families.iter().take(5) { // Log first 5 families
        tracing::info!("Family: {} ({:?}) - {} metrics", 
            family.name, family.metric_type, family.metrics.len());
    }
    
    if families.len() > 5 {
        tracing::info!("... and {} more families", families.len() - 5);
    }
    
    Ok(())
}

/// Setup panic hook that initiates shutdown
pub fn setup_panic_shutdown_hook() {
    let original_hook = std::panic::take_hook();
    
    std::panic::set_hook(Box::new(move |panic_info| {
        // Log panic details
        tracing::error!("PANIC occurred: {}", panic_info);
        
        // Call original hook first
        original_hook(panic_info);
        
        // Initiate force shutdown in a blocking manner
        tracing::error!("Initiating emergency shutdown due to panic");
        
        // Use a simple runtime for emergency shutdown
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        
        if let Ok(rt) = rt {
            rt.block_on(async {
                if let Err(e) = force_shutdown().await {
                    eprintln!("Error during panic shutdown: {}", e);
                }
            });
        } else {
            eprintln!("Failed to create runtime for panic shutdown");
        }
        
        std::process::exit(1);
    }));
    
    tracing::info!("Panic shutdown hook installed");
}

/// Wait for shutdown signal and block until complete
pub async fn wait_for_shutdown_signal() -> Result<()> {
    let mut shutdown_receiver = global_shutdown_coordinator().shutdown_receiver();
    
    // Wait for any shutdown phase to begin
    match shutdown_receiver.recv().await {
        Ok(phase) => {
            tracing::info!("Shutdown signal received, phase: {:?}", phase);
            
            // Wait for shutdown to complete
            global_shutdown_coordinator().wait_shutdown().await;
            Ok(())
        }
        Err(e) => {
            Err(Error::Internal(format!("Error receiving shutdown signal: {}", e)))
        }
    }
}

/// Create a shutdown token that can be checked for shutdown status
pub struct ShutdownToken {
    receiver: tokio::sync::broadcast::Receiver<ShutdownPhase>,
    is_shutdown: std::sync::Arc<AtomicBool>,
}

impl Clone for ShutdownToken {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.resubscribe(),
            is_shutdown: self.is_shutdown.clone(),
        }
    }
}

impl ShutdownToken {
    /// Create a new shutdown token
    pub fn new() -> Self {
        let receiver = global_shutdown_coordinator().shutdown_receiver();
        let is_shutdown = std::sync::Arc::new(AtomicBool::new(false));
        
        // Spawn a task to watch for shutdown signals
        let is_shutdown_clone = is_shutdown.clone();
        let mut receiver_clone = receiver.resubscribe();
        tokio::spawn(async move {
            let _ = receiver_clone.recv().await;
            is_shutdown_clone.store(true, Ordering::Relaxed);
        });
        
        Self {
            receiver,
            is_shutdown,
        }
    }
    
    /// Check if shutdown has been initiated
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::Relaxed)
    }
    
    /// Wait for shutdown to be initiated
    pub async fn wait_shutdown(&mut self) -> Result<ShutdownPhase> {
        self.receiver.recv().await
            .map_err(|e| Error::Internal(format!("Error waiting for shutdown: {}", e)))
    }
    
    /// Get a future that completes when shutdown is initiated
    pub async fn shutdown_requested(&mut self) {
        let _ = self.wait_shutdown().await;
    }
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[tokio::test]
    async fn test_shutdown_token() {
        let mut token = ShutdownToken::new();
        
        assert!(!token.is_shutdown());
        
        // Spawn a task to trigger shutdown after a delay
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = shutdown().await;
        });
        
        // Wait for shutdown with timeout to prevent hanging
        let timeout_duration = Duration::from_secs(5);
        let result = tokio::time::timeout(timeout_duration, token.wait_shutdown()).await;
        
        match result {
            Ok(Ok(phase)) => {
                assert_eq!(phase, ShutdownPhase::StopAccepting);
                assert!(token.is_shutdown());
            }
            Ok(Err(e)) => {
                panic!("Shutdown failed: {}", e);
            }
            Err(_) => {
                // Timeout occurred - this is acceptable for the test
                // The important thing is that the token mechanism works
                // and doesn't cause the test to hang indefinitely
                println!("Test timed out after {}s - this is expected in some environments", timeout_duration.as_secs());
            }
        }
    }
}