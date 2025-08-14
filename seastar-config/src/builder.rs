//! Configuration builder for programmatic config creation

use crate::config::*;
use std::path::PathBuf;

/// Fluent configuration builder
#[derive(Default)]
pub struct ConfigBuilder {
    config: SeastarConfig,
}

impl ConfigBuilder {
    /// Create a new config builder
    pub fn new() -> Self {
        Self {
            config: SeastarConfig::default(),
        }
    }
    
    /// Set application configuration
    pub fn app(mut self, name: &str, version: &str) -> Self {
        self.config.app.name = name.to_string();
        self.config.app.version = version.to_string();
        self
    }
    
    /// Set environment
    pub fn environment(mut self, env: &str) -> Self {
        self.config.app.environment = env.to_string();
        self
    }
    
    /// Set instance ID
    pub fn instance_id(mut self, id: &str) -> Self {
        self.config.app.instance_id = Some(id.to_string());
        self
    }
    
    /// Configure reactor
    pub fn reactor(mut self, f: impl FnOnce(ReactorBuilder) -> ReactorBuilder) -> Self {
        let builder = ReactorBuilder::new(self.config.reactor);
        self.config.reactor = f(builder).build();
        self
    }
    
    /// Configure SMP
    pub fn smp(mut self, f: impl FnOnce(SmpBuilder) -> SmpBuilder) -> Self {
        let builder = SmpBuilder::new(self.config.smp);
        self.config.smp = f(builder).build();
        self
    }
    
    /// Configure database (enables database features)
    pub fn database(mut self, f: impl FnOnce(DatabaseBuilder) -> DatabaseBuilder) -> Self {
        let builder = DatabaseBuilder::new();
        self.config.database = Some(f(builder).build());
        self
    }
    
    /// Configure metrics
    pub fn metrics(mut self, f: impl FnOnce(MetricsBuilder) -> MetricsBuilder) -> Self {
        let builder = MetricsBuilder::new(self.config.metrics);
        self.config.metrics = f(builder).build();
        self
    }
    
    /// Configure logging
    pub fn logging(mut self, f: impl FnOnce(LoggingBuilder) -> LoggingBuilder) -> Self {
        let builder = LoggingBuilder::new(self.config.logging);
        self.config.logging = f(builder).build();
        self
    }
    
    /// Configure TLS (enables TLS features)
    pub fn tls(mut self, f: impl FnOnce(TlsBuilder) -> TlsBuilder) -> Self {
        let builder = TlsBuilder::new();
        self.config.tls = Some(f(builder).build());
        self
    }
    
    /// Configure WebSocket (enables WebSocket features)
    pub fn websocket(mut self, f: impl FnOnce(WebSocketBuilder) -> WebSocketBuilder) -> Self {
        let builder = WebSocketBuilder::new();
        self.config.websocket = Some(f(builder).build());
        self
    }
    
    /// Configure testing (enables testing features)
    pub fn testing(mut self, f: impl FnOnce(TestingBuilder) -> TestingBuilder) -> Self {
        let builder = TestingBuilder::new();
        self.config.testing = Some(f(builder).build());
        self
    }
    
    /// Build the final configuration
    pub fn build(self) -> SeastarConfig {
        self.config
    }
}

/// Reactor configuration builder
pub struct ReactorBuilder {
    config: ReactorConfig,
}

impl ReactorBuilder {
    fn new(config: ReactorConfig) -> Self {
        Self { config }
    }
    
    pub fn task_queue_size(mut self, size: usize) -> Self {
        self.config.task_queue_size = size;
        self
    }
    
    pub fn io_polling_timeout_ms(mut self, timeout: u64) -> Self {
        self.config.io_polling_timeout_ms = timeout;
        self
    }
    
    pub fn enable_io_uring(mut self, enable: bool) -> Self {
        self.config.enable_io_uring = enable;
        self
    }
    
    pub fn max_io_operations(mut self, max: usize) -> Self {
        self.config.max_io_operations = max;
        self
    }
    
    fn build(self) -> ReactorConfig {
        self.config
    }
}

/// SMP configuration builder
pub struct SmpBuilder {
    config: SmpConfig,
}

impl SmpBuilder {
    fn new(config: SmpConfig) -> Self {
        Self { config }
    }
    
    pub fn num_shards(mut self, num: usize) -> Self {
        self.config.num_shards = num;
        self
    }
    
    pub fn pin_threads(mut self, pin: bool) -> Self {
        self.config.pin_threads = pin;
        self
    }
    
    pub fn message_queue_size(mut self, size: usize) -> Self {
        self.config.message_queue_size = size;
        self
    }
    
    pub fn enable_work_stealing(mut self, enable: bool) -> Self {
        self.config.enable_work_stealing = enable;
        self
    }
    
    fn build(self) -> SmpConfig {
        self.config
    }
}

/// Database configuration builder
pub struct DatabaseBuilder {
    config: DatabaseConfig,
}

impl DatabaseBuilder {
    fn new() -> Self {
        Self {
            config: DatabaseConfig {
                url: "".to_string(),
                max_connections: 10,
                min_connections: 1,
                connect_timeout_secs: 30,
                query_timeout_secs: 30,
                max_lifetime_secs: None,
                idle_timeout_secs: None,
                enable_logging: false,
            }
        }
    }
    
    pub fn url(mut self, url: &str) -> Self {
        self.config.url = url.to_string();
        self
    }
    
    pub fn max_connections(mut self, max: u32) -> Self {
        self.config.max_connections = max;
        self
    }
    
    pub fn min_connections(mut self, min: u32) -> Self {
        self.config.min_connections = min;
        self
    }
    
    pub fn connect_timeout_secs(mut self, timeout: u64) -> Self {
        self.config.connect_timeout_secs = timeout;
        self
    }
    
    pub fn query_timeout_secs(mut self, timeout: u64) -> Self {
        self.config.query_timeout_secs = timeout;
        self
    }
    
    pub fn max_lifetime_secs(mut self, lifetime: u64) -> Self {
        self.config.max_lifetime_secs = Some(lifetime);
        self
    }
    
    pub fn idle_timeout_secs(mut self, timeout: u64) -> Self {
        self.config.idle_timeout_secs = Some(timeout);
        self
    }
    
    pub fn enable_logging(mut self, enable: bool) -> Self {
        self.config.enable_logging = enable;
        self
    }
    
    fn build(self) -> DatabaseConfig {
        self.config
    }
}

/// Metrics configuration builder
pub struct MetricsBuilder {
    config: MetricsConfig,
}

impl MetricsBuilder {
    fn new(config: MetricsConfig) -> Self {
        Self { config }
    }
    
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }
    
    pub fn export_interval_secs(mut self, interval: u64) -> Self {
        self.config.export_interval_secs = interval;
        self
    }
    
    pub fn http_server(mut self, bind: &str, port: u16) -> Self {
        self.config.http_server = Some(MetricsHttpConfig {
            bind: bind.to_string(),
            port,
            enable_dashboard: true,
        });
        self
    }
    
    pub fn prometheus(mut self, registry: &str) -> Self {
        self.config.prometheus = Some(PrometheusConfig {
            registry: registry.to_string(),
            enable_push_gateway: false,
            push_gateway_url: None,
        });
        self
    }
    
    pub fn file_export(mut self, directory: PathBuf, format: &str) -> Self {
        self.config.file_export = Some(FileExportConfig {
            directory,
            format: format.to_string(),
            rotation_interval_secs: 3600,
            max_files: 24,
        });
        self
    }
    
    fn build(self) -> MetricsConfig {
        self.config
    }
}

/// Logging configuration builder
pub struct LoggingBuilder {
    config: LoggingConfig,
}

impl LoggingBuilder {
    fn new(config: LoggingConfig) -> Self {
        Self { config }
    }
    
    pub fn level(mut self, level: &str) -> Self {
        self.config.level = level.to_string();
        self
    }
    
    pub fn format(mut self, format: &str) -> Self {
        self.config.format = format.to_string();
        self
    }
    
    pub fn colored(mut self, colored: bool) -> Self {
        self.config.colored = colored;
        self
    }
    
    pub fn structured(mut self, structured: bool) -> Self {
        self.config.structured = structured;
        self
    }
    
    pub fn file(mut self, path: PathBuf, max_size_mb: u64, max_files: usize) -> Self {
        self.config.file = Some(LogFileConfig {
            path,
            max_size_mb,
            max_files,
            compress: true,
        });
        self
    }
    
    fn build(self) -> LoggingConfig {
        self.config
    }
}

/// TLS configuration builder
pub struct TlsBuilder {
    config: TlsConfig,
}

impl TlsBuilder {
    fn new() -> Self {
        Self {
            config: TlsConfig {
                cert_file: PathBuf::from("cert.pem"),
                key_file: PathBuf::from("key.pem"),
                ca_file: None,
                require_client_cert: false,
                min_version: "1.2".to_string(),
                cipher_suites: None,
            }
        }
    }
    
    pub fn cert_file(mut self, path: PathBuf) -> Self {
        self.config.cert_file = path;
        self
    }
    
    pub fn key_file(mut self, path: PathBuf) -> Self {
        self.config.key_file = path;
        self
    }
    
    pub fn ca_file(mut self, path: PathBuf) -> Self {
        self.config.ca_file = Some(path);
        self
    }
    
    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.config.require_client_cert = require;
        self
    }
    
    pub fn min_version(mut self, version: &str) -> Self {
        self.config.min_version = version.to_string();
        self
    }
    
    fn build(self) -> TlsConfig {
        self.config
    }
}

/// WebSocket configuration builder
pub struct WebSocketBuilder {
    config: WebSocketConfig,
}

impl WebSocketBuilder {
    fn new() -> Self {
        Self {
            config: WebSocketConfig {
                max_message_size: 1024 * 1024,
                max_frame_size: 64 * 1024,
                ping_interval_secs: 30,
                connection_timeout_secs: 60,
                max_connections: 1000,
            }
        }
    }
    
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }
    
    pub fn max_frame_size(mut self, size: usize) -> Self {
        self.config.max_frame_size = size;
        self
    }
    
    pub fn ping_interval_secs(mut self, interval: u64) -> Self {
        self.config.ping_interval_secs = interval;
        self
    }
    
    pub fn connection_timeout_secs(mut self, timeout: u64) -> Self {
        self.config.connection_timeout_secs = timeout;
        self
    }
    
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }
    
    fn build(self) -> WebSocketConfig {
        self.config
    }
}

/// Testing configuration builder
pub struct TestingBuilder {
    config: TestingConfig,
}

impl TestingBuilder {
    fn new() -> Self {
        Self {
            config: TestingConfig {
                enabled: false,
                data_dir: None,
                enable_chaos: false,
                chaos: None,
                performance: None,
            }
        }
    }
    
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }
    
    pub fn data_dir(mut self, dir: PathBuf) -> Self {
        self.config.data_dir = Some(dir);
        self
    }
    
    pub fn enable_chaos(mut self, enable: bool) -> Self {
        self.config.enable_chaos = enable;
        if enable && self.config.chaos.is_none() {
            self.config.chaos = Some(ChaosConfig {
                failure_rate: 0.01,
                latency_injection_ms: 0,
                enable_memory_pressure: false,
                enable_cpu_pressure: false,
            });
        }
        self
    }
    
    pub fn chaos_failure_rate(mut self, rate: f64) -> Self {
        if let Some(ref mut chaos) = self.config.chaos {
            chaos.failure_rate = rate;
        }
        self
    }
    
    pub fn performance_testing(mut self, target_throughput: u64, max_latency_ms: u64) -> Self {
        self.config.performance = Some(PerformanceConfig {
            enabled: true,
            target_throughput: Some(target_throughput),
            max_latency_ms: Some(max_latency_ms),
            duration_secs: 60,
        });
        self
    }
    
    fn build(self) -> TestingConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .app("test-app", "1.0.0")
            .environment("production")
            .reactor(|r| r.task_queue_size(2048).io_polling_timeout_ms(50))
            .smp(|s| s.num_shards(8).pin_threads(true))
            .database(|d| d.url("postgres://localhost/test").max_connections(20))
            .metrics(|m| m.enabled(true).http_server("0.0.0.0", 9090))
            .logging(|l| l.level("debug").format("json"))
            .build();
        
        assert_eq!(config.app.name, "test-app");
        assert_eq!(config.app.environment, "production");
        assert_eq!(config.reactor.task_queue_size, 2048);
        assert_eq!(config.smp.num_shards, 8);
        assert!(config.database.is_some());
        assert_eq!(config.database.unwrap().max_connections, 20);
        assert!(config.metrics.http_server.is_some());
        assert_eq!(config.logging.level, "debug");
    }
}