//! Core configuration structures

use serde::{Serialize, Deserialize};
use std::time::Duration;
use std::path::PathBuf;

/// Main Seastar configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeastarConfig {
    /// Application metadata
    pub app: AppConfig,
    
    /// Reactor configuration  
    pub reactor: ReactorConfig,
    
    /// SMP configuration
    pub smp: SmpConfig,
    
    /// Database configuration (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseConfig>,
    
    /// Metrics configuration
    pub metrics: MetricsConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// TLS configuration (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
    
    /// WebSocket configuration (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket: Option<WebSocketConfig>,
    
    /// Testing configuration (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub testing: Option<TestingConfig>,
}

/// Application metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Application name
    pub name: String,
    
    /// Application version
    pub version: String,
    
    /// Environment (dev, staging, production)
    #[serde(default = "default_environment")]
    pub environment: String,
    
    /// Application instance ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

/// Reactor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactorConfig {
    /// Task queue size
    #[serde(default = "default_task_queue_size")]
    pub task_queue_size: usize,
    
    /// IO polling timeout in milliseconds
    #[serde(default = "default_io_polling_timeout_ms")]
    pub io_polling_timeout_ms: u64,
    
    /// Enable io_uring on Linux (if available)
    #[serde(default = "default_true")]
    pub enable_io_uring: bool,
    
    /// Maximum concurrent IO operations
    #[serde(default = "default_max_io_ops")]
    pub max_io_operations: usize,
}

/// SMP configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmpConfig {
    /// Number of shards (0 = auto-detect CPU count)
    #[serde(default)]
    pub num_shards: usize,
    
    /// Pin threads to CPU cores
    #[serde(default = "default_true")]
    pub pin_threads: bool,
    
    /// Inter-shard message queue size
    #[serde(default = "default_message_queue_size")]
    pub message_queue_size: usize,
    
    /// Enable work stealing between shards
    #[serde(default = "default_false")]
    pub enable_work_stealing: bool,
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database URL
    pub url: String,
    
    /// Maximum connections in pool
    #[serde(default = "default_db_max_connections")]
    pub max_connections: u32,
    
    /// Minimum connections in pool
    #[serde(default = "default_db_min_connections")]
    pub min_connections: u32,
    
    /// Connection timeout in seconds
    #[serde(default = "default_db_timeout_secs")]
    pub connect_timeout_secs: u64,
    
    /// Query timeout in seconds
    #[serde(default = "default_db_query_timeout_secs")]
    pub query_timeout_secs: u64,
    
    /// Connection max lifetime in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_lifetime_secs: Option<u64>,
    
    /// Connection idle timeout in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    
    /// Enable SQL logging
    #[serde(default = "default_false")]
    pub enable_logging: bool,
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics collection
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Metrics export interval in seconds
    #[serde(default = "default_metrics_interval_secs")]
    pub export_interval_secs: u64,
    
    /// HTTP server for metrics endpoint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_server: Option<MetricsHttpConfig>,
    
    /// Prometheus configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prometheus: Option<PrometheusConfig>,
    
    /// File export configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_export: Option<FileExportConfig>,
}

/// Metrics HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsHttpConfig {
    /// Bind address
    #[serde(default = "default_metrics_bind")]
    pub bind: String,
    
    /// Port
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    
    /// Enable dashboard
    #[serde(default = "default_true")]
    pub enable_dashboard: bool,
}

/// Prometheus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Registry name
    #[serde(default = "default_prometheus_registry")]
    pub registry: String,
    
    /// Enable push gateway
    #[serde(default = "default_false")]
    pub enable_push_gateway: bool,
    
    /// Push gateway URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_gateway_url: Option<String>,
}

/// File export configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileExportConfig {
    /// Output directory
    pub directory: PathBuf,
    
    /// File format (json, csv, parquet)
    #[serde(default = "default_file_format")]
    pub format: String,
    
    /// Rotation interval in seconds
    #[serde(default = "default_file_rotation_secs")]
    pub rotation_interval_secs: u64,
    
    /// Maximum number of files to keep
    #[serde(default = "default_file_retention")]
    pub max_files: usize,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,
    
    /// Log format (json, text)
    #[serde(default = "default_log_format")]
    pub format: String,
    
    /// Enable colored output
    #[serde(default = "default_true")]
    pub colored: bool,
    
    /// Log to file configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<LogFileConfig>,
    
    /// Enable structured logging
    #[serde(default = "default_true")]
    pub structured: bool,
}

/// Log file configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFileConfig {
    /// Log file path
    pub path: PathBuf,
    
    /// Maximum file size in MB
    #[serde(default = "default_log_file_size_mb")]
    pub max_size_mb: u64,
    
    /// Maximum number of log files
    #[serde(default = "default_log_file_count")]
    pub max_files: usize,
    
    /// Enable compression for rotated files
    #[serde(default = "default_true")]
    pub compress: bool,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Certificate file path
    pub cert_file: PathBuf,
    
    /// Private key file path
    pub key_file: PathBuf,
    
    /// CA bundle file path (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    
    /// Require client certificates
    #[serde(default = "default_false")]
    pub require_client_cert: bool,
    
    /// TLS version (1.2, 1.3)
    #[serde(default = "default_tls_version")]
    pub min_version: String,
    
    /// Cipher suites
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cipher_suites: Option<Vec<String>>,
}

/// WebSocket configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// Maximum message size in bytes
    #[serde(default = "default_ws_max_message_size")]
    pub max_message_size: usize,
    
    /// Maximum frame size in bytes
    #[serde(default = "default_ws_max_frame_size")]
    pub max_frame_size: usize,
    
    /// Ping interval in seconds
    #[serde(default = "default_ws_ping_interval_secs")]
    pub ping_interval_secs: u64,
    
    /// Connection timeout in seconds
    #[serde(default = "default_ws_timeout_secs")]
    pub connection_timeout_secs: u64,
    
    /// Maximum concurrent connections
    #[serde(default = "default_ws_max_connections")]
    pub max_connections: usize,
}

/// Testing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestingConfig {
    /// Enable test mode
    #[serde(default = "default_false")]
    pub enabled: bool,
    
    /// Test data directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    
    /// Enable chaos testing
    #[serde(default = "default_false")]
    pub enable_chaos: bool,
    
    /// Chaos testing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chaos: Option<ChaosConfig>,
    
    /// Performance testing configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance: Option<PerformanceConfig>,
}

/// Chaos testing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    /// Failure injection rate (0.0 - 1.0)
    #[serde(default = "default_chaos_failure_rate")]
    pub failure_rate: f64,
    
    /// Latency injection in milliseconds
    #[serde(default)]
    pub latency_injection_ms: u64,
    
    /// Memory pressure simulation
    #[serde(default = "default_false")]
    pub enable_memory_pressure: bool,
    
    /// CPU pressure simulation
    #[serde(default = "default_false")]
    pub enable_cpu_pressure: bool,
}

/// Performance testing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Enable performance monitoring
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Target throughput (ops/sec)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_throughput: Option<u64>,
    
    /// Maximum acceptable latency in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_latency_ms: Option<u64>,
    
    /// Performance test duration in seconds
    #[serde(default = "default_perf_duration_secs")]
    pub duration_secs: u64,
}

impl Default for SeastarConfig {
    fn default() -> Self {
        Self {
            app: AppConfig::default(),
            reactor: ReactorConfig::default(),
            smp: SmpConfig::default(),
            database: None,
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            tls: None,
            websocket: None,
            testing: None,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: "seastar-app".to_string(),
            version: "0.1.0".to_string(),
            environment: default_environment(),
            instance_id: None,
        }
    }
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            task_queue_size: default_task_queue_size(),
            io_polling_timeout_ms: default_io_polling_timeout_ms(),
            enable_io_uring: default_true(),
            max_io_operations: default_max_io_ops(),
        }
    }
}

impl Default for SmpConfig {
    fn default() -> Self {
        Self {
            num_shards: 0, // Auto-detect
            pin_threads: default_true(),
            message_queue_size: default_message_queue_size(),
            enable_work_stealing: default_false(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            export_interval_secs: default_metrics_interval_secs(),
            http_server: None,
            prometheus: None,
            file_export: None,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            colored: default_true(),
            file: None,
            structured: default_true(),
        }
    }
}

// Default value functions
fn default_environment() -> String { "development".to_string() }
fn default_task_queue_size() -> usize { 1024 }
fn default_io_polling_timeout_ms() -> u64 { 100 }
fn default_max_io_ops() -> usize { 1024 }
fn default_message_queue_size() -> usize { 1024 }
fn default_db_max_connections() -> u32 { 10 }
fn default_db_min_connections() -> u32 { 1 }
fn default_db_timeout_secs() -> u64 { 30 }
fn default_db_query_timeout_secs() -> u64 { 30 }
fn default_metrics_interval_secs() -> u64 { 60 }
fn default_metrics_bind() -> String { "0.0.0.0".to_string() }
fn default_metrics_port() -> u16 { 8080 }
fn default_prometheus_registry() -> String { "seastar".to_string() }
fn default_file_format() -> String { "json".to_string() }
fn default_file_rotation_secs() -> u64 { 3600 }
fn default_file_retention() -> usize { 24 }
fn default_log_level() -> String { "info".to_string() }
fn default_log_format() -> String { "text".to_string() }
fn default_log_file_size_mb() -> u64 { 100 }
fn default_log_file_count() -> usize { 10 }
fn default_tls_version() -> String { "1.2".to_string() }
fn default_ws_max_message_size() -> usize { 1024 * 1024 } // 1MB
fn default_ws_max_frame_size() -> usize { 64 * 1024 } // 64KB
fn default_ws_ping_interval_secs() -> u64 { 30 }
fn default_ws_timeout_secs() -> u64 { 60 }
fn default_ws_max_connections() -> usize { 1000 }
fn default_chaos_failure_rate() -> f64 { 0.01 } // 1%
fn default_perf_duration_secs() -> u64 { 60 }
fn default_true() -> bool { true }
fn default_false() -> bool { false }

impl SeastarConfig {
    /// Get connection timeout as Duration
    pub fn database_connect_timeout(&self) -> Option<Duration> {
        self.database.as_ref().map(|db| Duration::from_secs(db.connect_timeout_secs))
    }
    
    /// Get query timeout as Duration
    pub fn database_query_timeout(&self) -> Option<Duration> {
        self.database.as_ref().map(|db| Duration::from_secs(db.query_timeout_secs))
    }
    
    /// Get metrics export interval as Duration
    pub fn metrics_export_interval(&self) -> Duration {
        Duration::from_secs(self.metrics.export_interval_secs)
    }
    
    /// Check if running in development mode
    pub fn is_development(&self) -> bool {
        self.app.environment == "development" || self.app.environment == "dev"
    }
    
    /// Check if running in production mode
    pub fn is_production(&self) -> bool {
        self.app.environment == "production" || self.app.environment == "prod"
    }
}