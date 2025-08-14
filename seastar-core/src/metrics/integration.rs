//! Metrics integration for seamless monitoring setup
//!
//! Provides high-level APIs for setting up comprehensive monitoring

use super::{
    global_registry, global_system_metrics,
    collectors::SystemCollectorRegistry,
    server::{MetricsServer, MetricsServerConfig, MetricsDashboard},
    exporters::{ExportFormat, MultiFormatExporter},
};
use crate::{Result, Error};
use std::time::Duration;
use tokio::time::interval;

/// Complete monitoring setup configuration
#[derive(Debug, Clone)]
pub struct MonitoringConfig {
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Enable HTTP metrics server
    pub enable_http_server: bool,
    /// HTTP server configuration
    pub server_config: MetricsServerConfig,
    /// Enable automatic stats collection
    pub enable_auto_collection: bool,
    /// Auto collection interval
    pub collection_interval: Duration,
    /// Enable periodic metric snapshots
    pub enable_snapshots: bool,
    /// Snapshot interval
    pub snapshot_interval: Duration,
    /// Default export formats for different endpoints
    pub default_formats: Vec<ExportFormat>,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_metrics: true,
            enable_http_server: true,
            server_config: MetricsServerConfig::default(),
            enable_auto_collection: true,
            collection_interval: Duration::from_secs(30),
            enable_snapshots: false,
            snapshot_interval: Duration::from_secs(300),
            default_formats: vec![ExportFormat::Prometheus, ExportFormat::Json],
        }
    }
}

/// Main monitoring system orchestrator
pub struct MonitoringSystem {
    config: MonitoringConfig,
    server: Option<MetricsServer>,
    dashboard: Option<MetricsDashboard>,
    collection_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MonitoringSystem {
    /// Create new monitoring system with configuration
    pub fn new(config: MonitoringConfig) -> Self {
        Self {
            config,
            server: None,
            dashboard: None,
            collection_handle: None,
        }
    }
    
    /// Create with default configuration
    pub fn default() -> Self {
        Self::new(MonitoringConfig::default())
    }
    
    /// Initialize the complete monitoring system
    pub async fn initialize(&mut self) -> Result<()> {
        if !self.config.enable_metrics {
            return Ok(());
        }
        
        // Initialize system metrics and register them
        let system_metrics = global_system_metrics();
        system_metrics.register_all()?;
        
        // Register system collectors
        if self.config.enable_auto_collection {
            SystemCollectorRegistry::register_all()?;
        }
        
        // Initialize HTTP server
        if self.config.enable_http_server {
            self.server = Some(MetricsServer::with_config(self.config.server_config.clone()));
            self.dashboard = Some(MetricsDashboard::new(self.config.server_config.clone()));
        }
        
        // Start periodic collection if enabled
        if self.config.enable_auto_collection {
            self.start_periodic_collection().await?;
        }
        
        tracing::info!("Monitoring system initialized successfully");
        Ok(())
    }
    
    /// Start periodic metric collection
    async fn start_periodic_collection(&mut self) -> Result<()> {
        let collection_interval = self.config.collection_interval;
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(collection_interval);
            
            loop {
                interval.tick().await;
                
                // Update system metrics periodically
                Self::update_system_metrics();
                
                // Optional: Log metric summary
                if let Ok(families) = std::panic::catch_unwind(|| {
                    global_registry().gather()
                }) {
                    tracing::debug!("Collected {} metric families", families.len());
                }
            }
        });
        
        self.collection_handle = Some(handle);
        Ok(())
    }
    
    /// Update system-wide metrics
    fn update_system_metrics() {
        let metrics = global_system_metrics();
        
        // Update memory metrics from current stats
        let memory_stats = crate::memory::global_memory_stats();
        metrics.memory_current_usage.set(memory_stats.allocated() as i64);
        metrics.memory_allocated.set(memory_stats.allocated() as i64);
        
        // Update task metrics if reactor is available
        let _ = crate::reactor::with_reactor(|reactor| {
            let _stats = reactor.stats();
            // Note: ReactorStats fields are private, so we can't access them directly
            // This would need to be implemented in the reactor module
            // For now, we'll skip the reactor-specific metric updates
        });
    }
    
    /// Get metrics in specified format
    pub async fn get_metrics(&self, format: ExportFormat) -> Result<String> {
        let families = global_registry().gather();
        MultiFormatExporter::export(&families, format)
    }
    
    /// Get metrics server handle
    pub fn metrics_server(&self) -> Option<&MetricsServer> {
        self.server.as_ref()
    }
    
    /// Get dashboard handle
    pub fn dashboard(&self) -> Option<&MetricsDashboard> {
        self.dashboard.as_ref()
    }
    
    /// Export current metrics to file
    pub async fn export_to_file(&self, path: &str, format: ExportFormat) -> Result<()> {
        let metrics_data = self.get_metrics(format).await?;
        tokio::fs::write(path, metrics_data).await
            .map_err(|e| Error::Io(format!("Failed to write metrics to file: {}", e)))?;
        Ok(())
    }
    
    /// Get monitoring system status
    pub async fn status(&self) -> MonitoringStatus {
        let families = global_registry().gather();
        let total_metrics = families.iter().map(|f| f.metrics.len()).sum();
        
        MonitoringStatus {
            enabled: self.config.enable_metrics,
            http_server_enabled: self.config.enable_http_server,
            auto_collection_enabled: self.config.enable_auto_collection,
            total_metric_families: families.len(),
            total_metrics,
            collection_running: self.collection_handle.as_ref().map_or(false, |h| !h.is_finished()),
            server_port: if self.config.enable_http_server {
                Some(self.config.server_config.port)
            } else {
                None
            },
        }
    }
    
    /// Shutdown monitoring system
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.collection_handle.take() {
            handle.abort();
            tracing::info!("Stopped metrics collection task");
        }
        
        // Clear global registry to prevent test isolation issues
        global_registry().clear();
        tracing::debug!("Cleared global metrics registry");
        
        tracing::info!("Monitoring system shutdown complete");
    }
}

/// Status information about the monitoring system
#[derive(Debug, Clone, serde::Serialize)]
pub struct MonitoringStatus {
    pub enabled: bool,
    pub http_server_enabled: bool,
    pub auto_collection_enabled: bool,
    pub total_metric_families: usize,
    pub total_metrics: usize,
    pub collection_running: bool,
    pub server_port: Option<u16>,
}

/// Monitoring builder for fluent configuration
pub struct MonitoringBuilder {
    config: MonitoringConfig,
}

impl MonitoringBuilder {
    pub fn new() -> Self {
        Self {
            config: MonitoringConfig::default(),
        }
    }
    
    /// Enable/disable metrics collection
    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.config.enable_metrics = enable;
        self
    }
    
    /// Enable/disable HTTP server
    pub fn enable_http_server(mut self, enable: bool) -> Self {
        self.config.enable_http_server = enable;
        self
    }
    
    /// Set HTTP server port
    pub fn http_port(mut self, port: u16) -> Self {
        self.config.server_config.port = port;
        self
    }
    
    /// Set HTTP server host
    pub fn http_host(mut self, host: String) -> Self {
        self.config.server_config.host = host;
        self
    }
    
    /// Enable/disable auto collection
    pub fn enable_auto_collection(mut self, enable: bool) -> Self {
        self.config.enable_auto_collection = enable;
        self
    }
    
    /// Set collection interval
    pub fn collection_interval(mut self, interval: Duration) -> Self {
        self.config.collection_interval = interval;
        self
    }
    
    /// Enable periodic snapshots
    pub fn enable_snapshots(mut self, enable: bool, interval: Duration) -> Self {
        self.config.enable_snapshots = enable;
        self.config.snapshot_interval = interval;
        self
    }
    
    /// Set default export format
    pub fn default_format(mut self, format: ExportFormat) -> Self {
        self.config.server_config.default_format = format;
        self
    }
    
    /// Build the monitoring system
    pub fn build(self) -> MonitoringSystem {
        MonitoringSystem::new(self.config)
    }
}

impl Default for MonitoringBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Global monitoring system instance
static GLOBAL_MONITORING: once_cell::sync::Lazy<tokio::sync::Mutex<Option<MonitoringSystem>>> = 
    once_cell::sync::Lazy::new(|| tokio::sync::Mutex::new(None));

/// Initialize global monitoring system
pub async fn initialize_monitoring(config: MonitoringConfig) -> Result<()> {
    let mut monitoring = MonitoringSystem::new(config);
    monitoring.initialize().await?;
    
    let mut global = GLOBAL_MONITORING.lock().await;
    *global = Some(monitoring);
    
    Ok(())
}

/// Get global monitoring system status
pub async fn monitoring_status() -> Option<MonitoringStatus> {
    let global = GLOBAL_MONITORING.lock().await;
    if let Some(ref monitoring) = *global {
        Some(monitoring.status().await)
    } else {
        None
    }
}

/// Shutdown global monitoring system
pub async fn shutdown_monitoring() {
    let mut global = GLOBAL_MONITORING.lock().await;
    if let Some(mut monitoring) = global.take() {
        monitoring.shutdown().await;
    }
}

/// Convenience function to setup basic monitoring
pub async fn setup_basic_monitoring() -> Result<()> {
    let config = MonitoringBuilder::new()
        .enable_metrics(true)
        .enable_http_server(true)
        .http_port(9090)
        .enable_auto_collection(true)
        .collection_interval(Duration::from_secs(30))
        .build()
        .config;
        
    initialize_monitoring(config).await
}

/// Convenience function to setup production monitoring
pub async fn setup_production_monitoring(port: u16, collection_interval: Duration) -> Result<()> {
    let config = MonitoringBuilder::new()
        .enable_metrics(true)
        .enable_http_server(true)
        .http_port(port)
        .http_host("0.0.0.0".to_string())
        .enable_auto_collection(true)
        .collection_interval(collection_interval)
        .enable_snapshots(true, Duration::from_secs(300))
        .default_format(ExportFormat::Prometheus)
        .build()
        .config;
        
    initialize_monitoring(config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[test]
    fn test_monitoring_builder() {
        let monitoring = MonitoringBuilder::new()
            .enable_metrics(true)
            .enable_http_server(true)
            .http_port(8080)
            .http_host("localhost".to_string())
            .enable_auto_collection(true)
            .collection_interval(Duration::from_secs(60))
            .build();
            
        assert!(monitoring.config.enable_metrics);
        assert!(monitoring.config.enable_http_server);
        assert_eq!(monitoring.config.server_config.port, 8080);
        assert_eq!(monitoring.config.server_config.host, "localhost");
        assert!(monitoring.config.enable_auto_collection);
        assert_eq!(monitoring.config.collection_interval, Duration::from_secs(60));
    }
    
    #[tokio::test]
    async fn test_monitoring_system_initialization() {
        let mut monitoring = MonitoringBuilder::new()
            .enable_metrics(true)
            .enable_http_server(false) // Disable HTTP server for test
            .enable_auto_collection(false) // Disable auto collection for test
            .build();
            
        let result = monitoring.initialize().await;
        if let Err(e) = &result {
            eprintln!("Monitoring initialization failed: {}", e);
        }
        assert!(result.is_ok());
        
        let status = monitoring.status().await;
        assert!(status.enabled);
        assert!(!status.http_server_enabled);
        assert!(!status.auto_collection_enabled);
        
        monitoring.shutdown().await;
    }
    
    #[tokio::test]
    async fn test_metrics_export() {
        let mut monitoring = MonitoringBuilder::new()
            .enable_metrics(true)
            .enable_http_server(false)
            .enable_auto_collection(false)
            .build();
            
        monitoring.initialize().await.unwrap();
        
        // Test different export formats
        let prometheus_data = monitoring.get_metrics(ExportFormat::Prometheus).await;
        assert!(prometheus_data.is_ok());
        
        let json_data = monitoring.get_metrics(ExportFormat::Json).await;
        assert!(json_data.is_ok());
        
        monitoring.shutdown().await;
    }
    
    #[tokio::test] 
    async fn test_global_monitoring() {
        let config = MonitoringBuilder::new()
            .enable_metrics(true)
            .enable_http_server(false)
            .enable_auto_collection(false)
            .build()
            .config;
            
        // Test initialization
        let result = initialize_monitoring(config).await;
        if let Err(e) = &result {
            eprintln!("Global monitoring initialization failed: {}", e);
        }
        assert!(result.is_ok());
        
        // Test status
        let status = monitoring_status().await;
        assert!(status.is_some());
        assert!(status.unwrap().enabled);
        
        // Test shutdown
        shutdown_monitoring().await;
        
        let status_after_shutdown = monitoring_status().await;
        assert!(status_after_shutdown.is_none());
    }
}