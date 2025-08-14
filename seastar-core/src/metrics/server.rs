//! Metrics monitoring server
//!
//! Provides an HTTP server for exposing metrics in various formats

use super::global_registry;
use super::exporters::{MultiFormatExporter, ExportFormat};
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

/// Configuration for metrics server
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    /// Port to bind the server to
    pub port: u16,
    /// Host to bind the server to  
    pub host: String,
    /// Enable CORS headers
    pub enable_cors: bool,
    /// Default export format
    pub default_format: ExportFormat,
    /// Enable metric filtering
    pub enable_filtering: bool,
    /// Cache metrics for this duration
    pub cache_duration: Duration,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            host: "127.0.0.1".to_string(),
            enable_cors: true,
            default_format: ExportFormat::Prometheus,
            enable_filtering: true,
            cache_duration: Duration::from_secs(5),
        }
    }
}

/// Response structure for metric queries
#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub metrics: String,
    pub format: String,
    pub timestamp: u64,
    pub count: usize,
}

/// Metrics server for HTTP exposure
pub struct MetricsServer {
    config: MetricsServerConfig,
    registry: Arc<dyn MetricRegistryProvider + Send + Sync>,
    cache: std::sync::Mutex<Option<(SystemTime, String, ExportFormat)>>,
}

/// Trait for metric registry providers
pub trait MetricRegistryProvider {
    fn gather_metrics(&self) -> Vec<crate::metrics::MetricFamily>;
}

/// Default implementation using global registry
pub struct GlobalRegistryProvider;

impl MetricRegistryProvider for GlobalRegistryProvider {
    fn gather_metrics(&self) -> Vec<crate::metrics::MetricFamily> {
        global_registry().gather()
    }
}

impl MetricsServer {
    /// Create a new metrics server with default configuration
    pub fn new() -> Self {
        Self::with_config(MetricsServerConfig::default())
    }
    
    /// Create a new metrics server with custom configuration
    pub fn with_config(config: MetricsServerConfig) -> Self {
        Self {
            config,
            registry: Arc::new(GlobalRegistryProvider),
            cache: std::sync::Mutex::new(None),
        }
    }
    
    /// Create with custom registry provider
    pub fn with_registry<T: MetricRegistryProvider + Send + Sync + 'static>(
        config: MetricsServerConfig,
        registry: T
    ) -> Self {
        Self {
            config,
            registry: Arc::new(registry),
            cache: std::sync::Mutex::new(None),
        }
    }
    
    /// Handle metrics request
    pub async fn handle_metrics(&self, format: Option<ExportFormat>, filter: Option<String>) -> Result<MetricsResponse> {
        let export_format = format.unwrap_or(self.config.default_format);
        
        // Check cache first
        if let Ok(cache) = self.cache.lock() {
            if let Some((cached_time, cached_data, cached_format)) = cache.as_ref() {
                if cached_time.elapsed().unwrap_or(Duration::MAX) < self.config.cache_duration 
                   && *cached_format == export_format {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    return Ok(MetricsResponse {
                        metrics: cached_data.clone(),
                        format: format_name(export_format),
                        timestamp,
                        count: cached_data.lines().count(),
                    });
                }
            }
        }
        
        // Gather fresh metrics
        let families = self.registry.gather_metrics();
        
        // Apply filtering if requested
        let filtered_families = if let Some(filter_pattern) = filter {
            if self.config.enable_filtering {
                families.into_iter()
                    .filter(|family| family.name.contains(&filter_pattern))
                    .collect()
            } else {
                families
            }
        } else {
            families
        };
        
        // Export metrics
        let metrics_data = MultiFormatExporter::export(&filtered_families, export_format)?;
        
        // Update cache
        if let Ok(mut cache) = self.cache.lock() {
            *cache = Some((SystemTime::now(), metrics_data.clone(), export_format));
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Ok(MetricsResponse {
            metrics: metrics_data,
            format: format_name(export_format),
            timestamp,
            count: filtered_families.len(),
        })
    }
    
    /// Get health check status
    pub async fn handle_health(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "status": "healthy",
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "version": env!("CARGO_PKG_VERSION"),
            "registry_size": self.registry.gather_metrics().len()
        }))
    }
    
    /// Get metrics metadata
    pub async fn handle_metadata(&self) -> Result<serde_json::Value> {
        let families = self.registry.gather_metrics();
        let metadata: Vec<serde_json::Value> = families.iter().map(|family| {
            serde_json::json!({
                "name": family.name,
                "help": family.help,
                "type": match family.metric_type {
                    crate::metrics::MetricType::Counter => "counter",
                    crate::metrics::MetricType::Gauge => "gauge", 
                    crate::metrics::MetricType::Histogram => "histogram",
                    crate::metrics::MetricType::Summary => "summary",
                },
                "metrics_count": family.metrics.len()
            })
        }).collect();
        
        Ok(serde_json::json!({
            "families": metadata,
            "total_families": families.len(),
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }))
    }
    
    /// Clear metrics cache
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            *cache = None;
        }
    }
    
    /// Get server configuration
    pub fn config(&self) -> &MetricsServerConfig {
        &self.config
    }
}

impl Default for MetricsServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Get format name as string
fn format_name(format: ExportFormat) -> String {
    match format {
        ExportFormat::Prometheus => "prometheus".to_string(),
        ExportFormat::Json => "json".to_string(),
        ExportFormat::Csv => "csv".to_string(),
        ExportFormat::OpenMetrics => "openmetrics".to_string(),
        ExportFormat::InfluxDB => "influxdb".to_string(),
    }
}

/// Parse format from string
pub fn parse_format(format_str: &str) -> Option<ExportFormat> {
    match format_str.to_lowercase().as_str() {
        "prometheus" | "prom" => Some(ExportFormat::Prometheus),
        "json" => Some(ExportFormat::Json),
        "csv" => Some(ExportFormat::Csv),
        "openmetrics" | "om" => Some(ExportFormat::OpenMetrics),
        "influxdb" | "influx" => Some(ExportFormat::InfluxDB),
        _ => None,
    }
}

/// Metrics dashboard data provider
pub struct MetricsDashboard {
    server: MetricsServer,
}

impl MetricsDashboard {
    /// Create new dashboard
    pub fn new(config: MetricsServerConfig) -> Self {
        Self {
            server: MetricsServer::with_config(config),
        }
    }
    
    /// Get dashboard data
    pub async fn get_dashboard_data(&self) -> Result<serde_json::Value> {
        let families = self.server.registry.gather_metrics();
        
        let mut dashboard_data = serde_json::Map::new();
        
        // Summary stats
        dashboard_data.insert("total_metrics".to_string(), serde_json::Value::Number(families.len().into()));
        dashboard_data.insert("timestamp".to_string(), serde_json::Value::Number(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs().into()
        ));
        
        // Metric type breakdown
        let mut type_counts = HashMap::new();
        for family in &families {
            *type_counts.entry(family.metric_type).or_insert(0) += 1;
        }
        
        let type_breakdown: serde_json::Map<String, serde_json::Value> = type_counts.iter().map(|(k, v)| {
            let type_name = match k {
                crate::metrics::MetricType::Counter => "counters",
                crate::metrics::MetricType::Gauge => "gauges",
                crate::metrics::MetricType::Histogram => "histograms", 
                crate::metrics::MetricType::Summary => "summaries",
            };
            (type_name.to_string(), serde_json::Value::Number((*v).into()))
        }).collect();
        
        dashboard_data.insert("metric_types".to_string(), serde_json::Value::Object(type_breakdown));
        
        // Recent metrics (last 10)
        let recent_metrics: Vec<serde_json::Value> = families.iter()
            .take(10)
            .map(|family| serde_json::json!({
                "name": family.name,
                "type": match family.metric_type {
                    crate::metrics::MetricType::Counter => "counter",
                    crate::metrics::MetricType::Gauge => "gauge",
                    crate::metrics::MetricType::Histogram => "histogram",
                    crate::metrics::MetricType::Summary => "summary",
                },
                "help": family.help,
                "metrics_count": family.metrics.len()
            }))
            .collect();
        
        dashboard_data.insert("recent_metrics".to_string(), serde_json::Value::Array(recent_metrics));
        
        Ok(serde_json::Value::Object(dashboard_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Counter, MetricRegistry};
    
    struct TestRegistryProvider {
        registry: MetricRegistry,
    }
    
    impl TestRegistryProvider {
        fn new() -> Self {
            Self {
                registry: MetricRegistry::new(),
            }
        }
        
        fn add_counter(&self, name: String, help: String, value: u64) {
            let counter = Counter::new(name, help);
            counter.add(value);
            self.registry.register(counter).unwrap();
        }
    }
    
    impl MetricRegistryProvider for TestRegistryProvider {
        fn gather_metrics(&self) -> Vec<crate::metrics::MetricFamily> {
            self.registry.gather()
        }
    }
    
    #[tokio::test]
    async fn test_metrics_server() {
        let provider = TestRegistryProvider::new();
        provider.add_counter("test_counter".to_string(), "Test counter".to_string(), 42);
        
        let config = MetricsServerConfig::default();
        let server = MetricsServer::with_registry(config, provider);
        
        let response = server.handle_metrics(Some(ExportFormat::Prometheus), None).await.unwrap();
        
        assert!(response.metrics.contains("test_counter"));
        assert!(response.metrics.contains("42"));
        assert_eq!(response.format, "prometheus");
    }
    
    #[tokio::test]
    async fn test_health_endpoint() {
        let server = MetricsServer::new();
        let health = server.handle_health().await.unwrap();
        
        assert_eq!(health["status"], "healthy");
        assert!(health["timestamp"].is_number());
    }
    
    #[tokio::test]
    async fn test_format_parsing() {
        assert_eq!(parse_format("prometheus"), Some(ExportFormat::Prometheus));
        assert_eq!(parse_format("json"), Some(ExportFormat::Json));
        assert_eq!(parse_format("csv"), Some(ExportFormat::Csv));
        assert_eq!(parse_format("invalid"), None);
    }
    
    #[tokio::test]
    async fn test_dashboard_data() {
        let provider = TestRegistryProvider::new();
        provider.add_counter("dashboard_test".to_string(), "Dashboard test counter".to_string(), 100);
        
        let config = MetricsServerConfig::default();
        let server = MetricsServer::with_registry(config.clone(), provider);
        let dashboard = MetricsDashboard { server };
        
        let data = dashboard.get_dashboard_data().await.unwrap();
        
        assert!(data["total_metrics"].is_number());
        assert!(data["metric_types"].is_object());
        assert!(data["recent_metrics"].is_array());
    }
}