//! # Seastar-RS Configuration System
//! 
//! Comprehensive configuration management for Seastar-RS applications.
//! Supports YAML, TOML, JSON formats with environment variable overrides.

pub mod config;
pub mod loader;
pub mod builder;
pub mod env;
pub mod validation;

pub use config::*;
pub use loader::ConfigLoader;
pub use builder::ConfigBuilder;
pub use env::EnvOverride;

use thiserror::Error;

/// Configuration errors
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("YAML parsing error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    
    #[error("TOML parsing error: {0}")]
    Toml(#[from] toml::de::Error),
    
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Validation error: {0}")]
    Validation(String),
    
    #[error("Environment variable error: {0}")]
    Environment(String),
    
    #[error("Missing required configuration: {0}")]
    Missing(String),
}

pub type Result<T> = std::result::Result<T, ConfigError>;