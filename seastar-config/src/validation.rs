//! Configuration validation utilities

use crate::{config::*, Result, ConfigError};

/// Validate complete configuration
pub fn validate_config(config: &SeastarConfig) -> Result<()> {
    validate_app_config(&config.app)?;
    validate_reactor_config(&config.reactor)?;
    validate_smp_config(&config.smp)?;
    
    if let Some(ref db) = config.database {
        validate_database_config(db)?;
    }
    
    validate_metrics_config(&config.metrics)?;
    validate_logging_config(&config.logging)?;
    
    if let Some(ref tls) = config.tls {
        validate_tls_config(tls)?;
    }
    
    if let Some(ref ws) = config.websocket {
        validate_websocket_config(ws)?;
    }
    
    if let Some(ref testing) = config.testing {
        validate_testing_config(testing)?;
    }
    
    Ok(())
}

/// Validate application configuration
pub fn validate_app_config(config: &AppConfig) -> Result<()> {
    if config.name.is_empty() {
        return Err(ConfigError::Validation("App name cannot be empty".to_string()));
    }
    
    if config.version.is_empty() {
        return Err(ConfigError::Validation("App version cannot be empty".to_string()));
    }
    
    let valid_environments = ["development", "dev", "staging", "stage", "production", "prod", "test"];
    if !valid_environments.contains(&config.environment.as_str()) {
        return Err(ConfigError::Validation(
            format!("Invalid environment '{}'. Valid values: {:?}", config.environment, valid_environments)
        ));
    }
    
    Ok(())
}

/// Validate reactor configuration
pub fn validate_reactor_config(config: &ReactorConfig) -> Result<()> {
    if config.task_queue_size == 0 {
        return Err(ConfigError::Validation("Task queue size must be greater than 0".to_string()));
    }
    
    if config.task_queue_size > 1_000_000 {
        return Err(ConfigError::Validation("Task queue size should not exceed 1,000,000".to_string()));
    }
    
    if config.io_polling_timeout_ms == 0 {
        return Err(ConfigError::Validation("IO polling timeout must be greater than 0".to_string()));
    }
    
    if config.io_polling_timeout_ms > 60_000 {
        return Err(ConfigError::Validation("IO polling timeout should not exceed 60 seconds".to_string()));
    }
    
    if config.max_io_operations == 0 {
        return Err(ConfigError::Validation("Max IO operations must be greater than 0".to_string()));
    }
    
    if config.max_io_operations > 1_000_000 {
        return Err(ConfigError::Validation("Max IO operations should not exceed 1,000,000".to_string()));
    }
    
    Ok(())
}

/// Validate SMP configuration
pub fn validate_smp_config(config: &SmpConfig) -> Result<()> {
    if config.num_shards > 1024 {
        return Err(ConfigError::Validation("Number of shards should not exceed 1024".to_string()));
    }
    
    if config.message_queue_size == 0 {
        return Err(ConfigError::Validation("Message queue size must be greater than 0".to_string()));
    }
    
    if config.message_queue_size > 10_000_000 {
        return Err(ConfigError::Validation("Message queue size should not exceed 10,000,000".to_string()));
    }
    
    Ok(())
}

/// Validate database configuration
pub fn validate_database_config(config: &DatabaseConfig) -> Result<()> {
    if config.url.is_empty() {
        return Err(ConfigError::Validation("Database URL cannot be empty".to_string()));
    }
    
    // Basic URL validation - should start with supported schemes
    let valid_schemes = ["postgres://", "postgresql://", "mysql://", "sqlite://"];
    let has_valid_scheme = valid_schemes.iter().any(|&scheme| config.url.starts_with(scheme));
    if !has_valid_scheme {
        return Err(ConfigError::Validation(
            format!("Database URL must start with one of: {:?}", valid_schemes)
        ));
    }
    
    if config.max_connections == 0 {
        return Err(ConfigError::Validation("Max connections must be greater than 0".to_string()));
    }
    
    if config.max_connections > 10000 {
        return Err(ConfigError::Validation("Max connections should not exceed 10,000".to_string()));
    }
    
    if config.min_connections == 0 {
        return Err(ConfigError::Validation("Min connections must be greater than 0".to_string()));
    }
    
    if config.min_connections > config.max_connections {
        return Err(ConfigError::Validation("Min connections cannot exceed max connections".to_string()));
    }
    
    if config.connect_timeout_secs == 0 {
        return Err(ConfigError::Validation("Connect timeout must be greater than 0".to_string()));
    }
    
    if config.connect_timeout_secs > 3600 {
        return Err(ConfigError::Validation("Connect timeout should not exceed 1 hour".to_string()));
    }
    
    if config.query_timeout_secs == 0 {
        return Err(ConfigError::Validation("Query timeout must be greater than 0".to_string()));
    }
    
    if config.query_timeout_secs > 3600 {
        return Err(ConfigError::Validation("Query timeout should not exceed 1 hour".to_string()));
    }
    
    Ok(())
}

/// Validate metrics configuration
pub fn validate_metrics_config(config: &MetricsConfig) -> Result<()> {
    if config.export_interval_secs == 0 {
        return Err(ConfigError::Validation("Metrics export interval must be greater than 0".to_string()));
    }
    
    if config.export_interval_secs > 86400 {
        return Err(ConfigError::Validation("Metrics export interval should not exceed 24 hours".to_string()));
    }
    
    if let Some(ref http) = config.http_server {
        if http.port == 0 {
            return Err(ConfigError::Validation("HTTP server port must be greater than 0".to_string()));
        }
        
        if http.bind.is_empty() {
            return Err(ConfigError::Validation("HTTP server bind address cannot be empty".to_string()));
        }
    }
    
    if let Some(ref prometheus) = config.prometheus {
        if prometheus.registry.is_empty() {
            return Err(ConfigError::Validation("Prometheus registry name cannot be empty".to_string()));
        }
    }
    
    Ok(())
}

/// Validate logging configuration
pub fn validate_logging_config(config: &LoggingConfig) -> Result<()> {
    let valid_levels = ["trace", "debug", "info", "warn", "error"];
    if !valid_levels.contains(&config.level.as_str()) {
        return Err(ConfigError::Validation(
            format!("Invalid log level '{}'. Valid values: {:?}", config.level, valid_levels)
        ));
    }
    
    let valid_formats = ["text", "json"];
    if !valid_formats.contains(&config.format.as_str()) {
        return Err(ConfigError::Validation(
            format!("Invalid log format '{}'. Valid values: {:?}", config.format, valid_formats)
        ));
    }
    
    if let Some(ref file) = config.file {
        if file.max_size_mb == 0 {
            return Err(ConfigError::Validation("Log file max size must be greater than 0".to_string()));
        }
        
        if file.max_size_mb > 10240 { // 10GB
            return Err(ConfigError::Validation("Log file max size should not exceed 10GB".to_string()));
        }
        
        if file.max_files == 0 {
            return Err(ConfigError::Validation("Log file max files must be greater than 0".to_string()));
        }
        
        if file.max_files > 10000 {
            return Err(ConfigError::Validation("Log file max files should not exceed 10,000".to_string()));
        }
    }
    
    Ok(())
}

/// Validate TLS configuration
pub fn validate_tls_config(config: &TlsConfig) -> Result<()> {
    if !config.cert_file.exists() {
        return Err(ConfigError::Validation(
            format!("TLS certificate file does not exist: {}", config.cert_file.display())
        ));
    }
    
    if !config.key_file.exists() {
        return Err(ConfigError::Validation(
            format!("TLS key file does not exist: {}", config.key_file.display())
        ));
    }
    
    if let Some(ref ca_file) = config.ca_file {
        if !ca_file.exists() {
            return Err(ConfigError::Validation(
                format!("TLS CA file does not exist: {}", ca_file.display())
            ));
        }
    }
    
    let valid_versions = ["1.2", "1.3"];
    if !valid_versions.contains(&config.min_version.as_str()) {
        return Err(ConfigError::Validation(
            format!("Invalid TLS version '{}'. Valid values: {:?}", config.min_version, valid_versions)
        ));
    }
    
    Ok(())
}

/// Validate WebSocket configuration
pub fn validate_websocket_config(config: &WebSocketConfig) -> Result<()> {
    if config.max_message_size == 0 {
        return Err(ConfigError::Validation("WebSocket max message size must be greater than 0".to_string()));
    }
    
    if config.max_message_size > 100 * 1024 * 1024 { // 100MB
        return Err(ConfigError::Validation("WebSocket max message size should not exceed 100MB".to_string()));
    }
    
    if config.max_frame_size == 0 {
        return Err(ConfigError::Validation("WebSocket max frame size must be greater than 0".to_string()));
    }
    
    if config.max_frame_size > config.max_message_size {
        return Err(ConfigError::Validation("WebSocket max frame size cannot exceed max message size".to_string()));
    }
    
    if config.ping_interval_secs == 0 {
        return Err(ConfigError::Validation("WebSocket ping interval must be greater than 0".to_string()));
    }
    
    if config.connection_timeout_secs == 0 {
        return Err(ConfigError::Validation("WebSocket connection timeout must be greater than 0".to_string()));
    }
    
    if config.max_connections == 0 {
        return Err(ConfigError::Validation("WebSocket max connections must be greater than 0".to_string()));
    }
    
    if config.max_connections > 1_000_000 {
        return Err(ConfigError::Validation("WebSocket max connections should not exceed 1,000,000".to_string()));
    }
    
    Ok(())
}

/// Validate testing configuration
pub fn validate_testing_config(config: &TestingConfig) -> Result<()> {
    if let Some(ref data_dir) = config.data_dir {
        if !data_dir.exists() {
            return Err(ConfigError::Validation(
                format!("Test data directory does not exist: {}", data_dir.display())
            ));
        }
        
        if !data_dir.is_dir() {
            return Err(ConfigError::Validation(
                format!("Test data path is not a directory: {}", data_dir.display())
            ));
        }
    }
    
    if let Some(ref chaos) = config.chaos {
        if chaos.failure_rate < 0.0 || chaos.failure_rate > 1.0 {
            return Err(ConfigError::Validation(
                "Chaos failure rate must be between 0.0 and 1.0".to_string()
            ));
        }
        
        if chaos.latency_injection_ms > 60000 {
            return Err(ConfigError::Validation(
                "Chaos latency injection should not exceed 60 seconds".to_string()
            ));
        }
    }
    
    if let Some(ref perf) = config.performance {
        if let Some(throughput) = perf.target_throughput {
            if throughput == 0 {
                return Err(ConfigError::Validation("Target throughput must be greater than 0".to_string()));
            }
        }
        
        if let Some(latency) = perf.max_latency_ms {
            if latency == 0 {
                return Err(ConfigError::Validation("Max latency must be greater than 0".to_string()));
            }
        }
        
        if perf.duration_secs == 0 {
            return Err(ConfigError::Validation("Performance test duration must be greater than 0".to_string()));
        }
        
        if perf.duration_secs > 86400 {
            return Err(ConfigError::Validation("Performance test duration should not exceed 24 hours".to_string()));
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_validate_app_config() {
        let mut config = AppConfig {
            name: "test-app".to_string(),
            version: "1.0.0".to_string(),
            environment: "development".to_string(),
            instance_id: None,
        };
        
        assert!(validate_app_config(&config).is_ok());
        
        config.name = "".to_string();
        assert!(validate_app_config(&config).is_err());
        
        config.name = "test-app".to_string();
        config.version = "".to_string();
        assert!(validate_app_config(&config).is_err());
        
        config.version = "1.0.0".to_string();
        config.environment = "invalid".to_string();
        assert!(validate_app_config(&config).is_err());
    }

    #[test]
    fn test_validate_database_config() {
        let mut config = DatabaseConfig {
            url: "postgres://localhost/test".to_string(),
            max_connections: 10,
            min_connections: 1,
            connect_timeout_secs: 30,
            query_timeout_secs: 30,
            max_lifetime_secs: None,
            idle_timeout_secs: None,
            enable_logging: false,
        };
        
        assert!(validate_database_config(&config).is_ok());
        
        config.url = "".to_string();
        assert!(validate_database_config(&config).is_err());
        
        config.url = "invalid://localhost/test".to_string();
        assert!(validate_database_config(&config).is_err());
        
        config.url = "postgres://localhost/test".to_string();
        config.min_connections = 20;
        config.max_connections = 10;
        assert!(validate_database_config(&config).is_err());
    }

    #[test]
    fn test_validate_reactor_config() {
        let mut config = ReactorConfig {
            task_queue_size: 1024,
            io_polling_timeout_ms: 100,
            enable_io_uring: true,
            max_io_operations: 1024,
        };
        
        assert!(validate_reactor_config(&config).is_ok());
        
        config.task_queue_size = 0;
        assert!(validate_reactor_config(&config).is_err());
        
        config.task_queue_size = 2_000_000;
        assert!(validate_reactor_config(&config).is_err());
    }

    #[test]
    fn test_validate_websocket_config() {
        let mut config = WebSocketConfig {
            max_message_size: 1024 * 1024,
            max_frame_size: 64 * 1024,
            ping_interval_secs: 30,
            connection_timeout_secs: 60,
            max_connections: 1000,
        };
        
        assert!(validate_websocket_config(&config).is_ok());
        
        config.max_frame_size = config.max_message_size + 1;
        assert!(validate_websocket_config(&config).is_err());
        
        config.max_frame_size = 64 * 1024;
        config.max_connections = 0;
        assert!(validate_websocket_config(&config).is_err());
    }
}