//! Environment variable override support

use crate::{config::*, Result, ConfigError};
use std::env;
use std::path::PathBuf;

/// Environment variable override utility
pub struct EnvOverride;

impl EnvOverride {
    /// Apply environment variable overrides to configuration
    pub fn apply(mut config: SeastarConfig) -> Result<SeastarConfig> {
        // App configuration
        if let Ok(name) = env::var("SEASTAR_APP_NAME") {
            config.app.name = name;
        }
        if let Ok(version) = env::var("SEASTAR_APP_VERSION") {
            config.app.version = version;
        }
        if let Ok(env_var) = env::var("SEASTAR_APP_ENVIRONMENT") {
            config.app.environment = env_var;
        }
        if let Ok(instance_id) = env::var("SEASTAR_APP_INSTANCE_ID") {
            config.app.instance_id = Some(instance_id);
        }

        // Reactor configuration
        if let Ok(queue_size) = env::var("SEASTAR_REACTOR_TASK_QUEUE_SIZE") {
            config.reactor.task_queue_size = queue_size.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_REACTOR_TASK_QUEUE_SIZE".to_string()))?;
        }
        if let Ok(timeout) = env::var("SEASTAR_REACTOR_IO_POLLING_TIMEOUT_MS") {
            config.reactor.io_polling_timeout_ms = timeout.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_REACTOR_IO_POLLING_TIMEOUT_MS".to_string()))?;
        }
        if let Ok(enable_io_uring) = env::var("SEASTAR_REACTOR_ENABLE_IO_URING") {
            config.reactor.enable_io_uring = parse_bool(&enable_io_uring)?;
        }
        if let Ok(max_io_ops) = env::var("SEASTAR_REACTOR_MAX_IO_OPERATIONS") {
            config.reactor.max_io_operations = max_io_ops.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_REACTOR_MAX_IO_OPERATIONS".to_string()))?;
        }

        // SMP configuration
        if let Ok(num_shards) = env::var("SEASTAR_SMP_NUM_SHARDS") {
            config.smp.num_shards = num_shards.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_SMP_NUM_SHARDS".to_string()))?;
        }
        if let Ok(pin_threads) = env::var("SEASTAR_SMP_PIN_THREADS") {
            config.smp.pin_threads = parse_bool(&pin_threads)?;
        }
        if let Ok(queue_size) = env::var("SEASTAR_SMP_MESSAGE_QUEUE_SIZE") {
            config.smp.message_queue_size = queue_size.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_SMP_MESSAGE_QUEUE_SIZE".to_string()))?;
        }
        if let Ok(work_stealing) = env::var("SEASTAR_SMP_ENABLE_WORK_STEALING") {
            config.smp.enable_work_stealing = parse_bool(&work_stealing)?;
        }

        // Database configuration
        if let Ok(db_url) = env::var("SEASTAR_DATABASE_URL") {
            let db_config = config.database.get_or_insert(DatabaseConfig {
                url: db_url,
                max_connections: 10,
                min_connections: 1,
                connect_timeout_secs: 30,
                query_timeout_secs: 30,
                max_lifetime_secs: None,
                idle_timeout_secs: None,
                enable_logging: false,
            });
            db_config.url = env::var("SEASTAR_DATABASE_URL").unwrap();
        }
        if let Some(ref mut db) = config.database {
            if let Ok(max_conn) = env::var("SEASTAR_DATABASE_MAX_CONNECTIONS") {
                db.max_connections = max_conn.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_DATABASE_MAX_CONNECTIONS".to_string()))?;
            }
            if let Ok(min_conn) = env::var("SEASTAR_DATABASE_MIN_CONNECTIONS") {
                db.min_connections = min_conn.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_DATABASE_MIN_CONNECTIONS".to_string()))?;
            }
            if let Ok(timeout) = env::var("SEASTAR_DATABASE_CONNECT_TIMEOUT_SECS") {
                db.connect_timeout_secs = timeout.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_DATABASE_CONNECT_TIMEOUT_SECS".to_string()))?;
            }
            if let Ok(timeout) = env::var("SEASTAR_DATABASE_QUERY_TIMEOUT_SECS") {
                db.query_timeout_secs = timeout.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_DATABASE_QUERY_TIMEOUT_SECS".to_string()))?;
            }
            if let Ok(enable_logging) = env::var("SEASTAR_DATABASE_ENABLE_LOGGING") {
                db.enable_logging = parse_bool(&enable_logging)?;
            }
        }

        // Metrics configuration
        if let Ok(enabled) = env::var("SEASTAR_METRICS_ENABLED") {
            config.metrics.enabled = parse_bool(&enabled)?;
        }
        if let Ok(interval) = env::var("SEASTAR_METRICS_EXPORT_INTERVAL_SECS") {
            config.metrics.export_interval_secs = interval.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_METRICS_EXPORT_INTERVAL_SECS".to_string()))?;
        }

        // Logging configuration
        if let Ok(level) = env::var("SEASTAR_LOG_LEVEL") {
            config.logging.level = level;
        }
        if let Ok(format) = env::var("SEASTAR_LOG_FORMAT") {
            config.logging.format = format;
        }
        if let Ok(colored) = env::var("SEASTAR_LOG_COLORED") {
            config.logging.colored = parse_bool(&colored)?;
        }
        if let Ok(structured) = env::var("SEASTAR_LOG_STRUCTURED") {
            config.logging.structured = parse_bool(&structured)?;
        }

        // TLS configuration
        if let Ok(cert_file) = env::var("SEASTAR_TLS_CERT_FILE") {
            let tls_config = config.tls.get_or_insert(TlsConfig {
                cert_file: PathBuf::from(&cert_file),
                key_file: PathBuf::from("key.pem"),
                ca_file: None,
                require_client_cert: false,
                min_version: "1.2".to_string(),
                cipher_suites: None,
            });
            tls_config.cert_file = PathBuf::from(cert_file);
        }
        if let Some(ref mut tls) = config.tls {
            if let Ok(key_file) = env::var("SEASTAR_TLS_KEY_FILE") {
                tls.key_file = PathBuf::from(key_file);
            }
            if let Ok(ca_file) = env::var("SEASTAR_TLS_CA_FILE") {
                tls.ca_file = Some(PathBuf::from(ca_file));
            }
            if let Ok(require_client_cert) = env::var("SEASTAR_TLS_REQUIRE_CLIENT_CERT") {
                tls.require_client_cert = parse_bool(&require_client_cert)?;
            }
            if let Ok(min_version) = env::var("SEASTAR_TLS_MIN_VERSION") {
                tls.min_version = min_version;
            }
        }

        // WebSocket configuration
        if let Ok(max_message_size) = env::var("SEASTAR_WEBSOCKET_MAX_MESSAGE_SIZE") {
            let ws_config = config.websocket.get_or_insert(WebSocketConfig {
                max_message_size: max_message_size.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_MAX_MESSAGE_SIZE".to_string()))?,
                max_frame_size: 64 * 1024,
                ping_interval_secs: 30,
                connection_timeout_secs: 60,
                max_connections: 1000,
            });
            ws_config.max_message_size = max_message_size.parse()
                .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_MAX_MESSAGE_SIZE".to_string()))?;
        }
        if let Some(ref mut ws) = config.websocket {
            if let Ok(max_frame_size) = env::var("SEASTAR_WEBSOCKET_MAX_FRAME_SIZE") {
                ws.max_frame_size = max_frame_size.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_MAX_FRAME_SIZE".to_string()))?;
            }
            if let Ok(ping_interval) = env::var("SEASTAR_WEBSOCKET_PING_INTERVAL_SECS") {
                ws.ping_interval_secs = ping_interval.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_PING_INTERVAL_SECS".to_string()))?;
            }
            if let Ok(timeout) = env::var("SEASTAR_WEBSOCKET_CONNECTION_TIMEOUT_SECS") {
                ws.connection_timeout_secs = timeout.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_CONNECTION_TIMEOUT_SECS".to_string()))?;
            }
            if let Ok(max_connections) = env::var("SEASTAR_WEBSOCKET_MAX_CONNECTIONS") {
                ws.max_connections = max_connections.parse()
                    .map_err(|_| ConfigError::Environment("Invalid SEASTAR_WEBSOCKET_MAX_CONNECTIONS".to_string()))?;
            }
        }

        Ok(config)
    }
}

/// Apply environment variable overrides (convenience function)
pub fn apply_env_overrides(config: SeastarConfig) -> Result<SeastarConfig> {
    EnvOverride::apply(config)
}

/// Parse boolean from string
fn parse_bool(s: &str) -> Result<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Environment(format!("Invalid boolean value: {}", s))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_env_overrides() {
        // Set some environment variables
        env::set_var("SEASTAR_APP_NAME", "test-from-env");
        env::set_var("SEASTAR_SMP_NUM_SHARDS", "16");
        env::set_var("SEASTAR_METRICS_ENABLED", "false");
        env::set_var("SEASTAR_LOG_LEVEL", "trace");
        
        let config = SeastarConfig::default();
        let config = EnvOverride::apply(config).unwrap();
        
        assert_eq!(config.app.name, "test-from-env");
        assert_eq!(config.smp.num_shards, 16);
        assert_eq!(config.metrics.enabled, false);
        assert_eq!(config.logging.level, "trace");
        
        // Cleanup
        env::remove_var("SEASTAR_APP_NAME");
        env::remove_var("SEASTAR_SMP_NUM_SHARDS");
        env::remove_var("SEASTAR_METRICS_ENABLED");
        env::remove_var("SEASTAR_LOG_LEVEL");
    }

    #[test]
    fn test_database_env_creation() {
        env::set_var("SEASTAR_DATABASE_URL", "postgres://localhost/test_env");
        
        let mut config = SeastarConfig::default();
        assert!(config.database.is_none());
        
        config = EnvOverride::apply(config).unwrap();
        assert!(config.database.is_some());
        assert_eq!(config.database.unwrap().url, "postgres://localhost/test_env");
        
        // Cleanup
        env::remove_var("SEASTAR_DATABASE_URL");
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_bool("true").unwrap(), true);
        assert_eq!(parse_bool("TRUE").unwrap(), true);
        assert_eq!(parse_bool("1").unwrap(), true);
        assert_eq!(parse_bool("yes").unwrap(), true);
        assert_eq!(parse_bool("on").unwrap(), true);
        
        assert_eq!(parse_bool("false").unwrap(), false);
        assert_eq!(parse_bool("FALSE").unwrap(), false);
        assert_eq!(parse_bool("0").unwrap(), false);
        assert_eq!(parse_bool("no").unwrap(), false);
        assert_eq!(parse_bool("off").unwrap(), false);
        
        assert!(parse_bool("invalid").is_err());
    }
}