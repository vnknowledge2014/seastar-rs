//! Configuration loading utilities

use crate::{config::SeastarConfig, Result, ConfigError};
use std::path::Path;
use std::fs;

/// Configuration file formats
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigFormat {
    Yaml,
    Toml,
    Json,
}

impl ConfigFormat {
    /// Detect format from file extension
    pub fn from_extension(path: &Path) -> Option<Self> {
        path.extension()?.to_str().map(|ext| {
            match ext.to_lowercase().as_str() {
                "yaml" | "yml" => ConfigFormat::Yaml,
                "toml" => ConfigFormat::Toml,
                "json" => ConfigFormat::Json,
                _ => ConfigFormat::Yaml, // Default
            }
        })
    }
}

/// Configuration loader
pub struct ConfigLoader {
    search_paths: Vec<String>,
    file_names: Vec<String>,
}

impl Default for ConfigLoader {
    fn default() -> Self {
        Self {
            search_paths: vec![
                ".".to_string(),
                "config".to_string(),
                "/etc/seastar".to_string(),
            ],
            file_names: vec![
                "seastar".to_string(),
                "config".to_string(),
                "app".to_string(),
            ],
        }
    }
}

impl ConfigLoader {
    /// Create a new config loader
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Add a search path
    pub fn add_search_path<P: Into<String>>(mut self, path: P) -> Self {
        self.search_paths.push(path.into());
        self
    }
    
    /// Add a file name to search for
    pub fn add_file_name<S: Into<String>>(mut self, name: S) -> Self {
        self.file_names.push(name.into());
        self
    }
    
    /// Load configuration from a specific file
    pub fn load_file<P: AsRef<Path>>(&self, path: P) -> Result<SeastarConfig> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(e))?;
        
        let format = ConfigFormat::from_extension(path)
            .unwrap_or(ConfigFormat::Yaml);
        
        self.parse_content(&content, format)
    }
    
    /// Load configuration by searching for config files
    pub fn load(&self) -> Result<SeastarConfig> {
        // Try to find config file
        if let Some(path) = self.find_config_file()? {
            tracing::info!("Loading configuration from: {}", path.display());
            return self.load_file(&path);
        }
        
        // No config file found, use defaults
        tracing::info!("No configuration file found, using defaults");
        Ok(SeastarConfig::default())
    }
    
    /// Find the first available config file
    fn find_config_file(&self) -> Result<Option<std::path::PathBuf>> {
        let extensions = ["yaml", "yml", "toml", "json"];
        
        for search_path in &self.search_paths {
            for file_name in &self.file_names {
                for ext in &extensions {
                    let path = Path::new(search_path)
                        .join(format!("{}.{}", file_name, ext));
                    
                    if path.exists() {
                        return Ok(Some(path));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    /// Parse configuration content based on format
    fn parse_content(&self, content: &str, format: ConfigFormat) -> Result<SeastarConfig> {
        match format {
            ConfigFormat::Yaml => {
                serde_yaml::from_str(content).map_err(ConfigError::Yaml)
            },
            ConfigFormat::Toml => {
                toml::from_str(content).map_err(ConfigError::Toml)
            },
            ConfigFormat::Json => {
                serde_json::from_str(content).map_err(ConfigError::Json)
            },
        }
    }
    
    /// Load configuration with environment variable overrides
    pub fn load_with_env(&self) -> Result<SeastarConfig> {
        let mut config = self.load()?;
        config = crate::env::apply_env_overrides(config)?;
        Ok(config)
    }
    
    /// Validate configuration file without loading
    pub fn validate_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let _config = self.load_file(path)?;
        crate::validation::validate_config(&_config)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;
    
    #[test]
    fn test_load_yaml_config() {
        let yaml_content = r#"
app:
  name: "test-app"
  version: "1.0.0"
  environment: "test"

reactor:
  task_queue_size: 2048
  io_polling_timeout_ms: 50

smp:
  num_shards: 4
  pin_threads: true

metrics:
  enabled: true
  export_interval_secs: 30

logging:
  level: "debug"
  format: "json"
"#;
        
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();
        
        let loader = ConfigLoader::new();
        let config = loader.load_file(file.path()).unwrap();
        
        assert_eq!(config.app.name, "test-app");
        assert_eq!(config.reactor.task_queue_size, 2048);
        assert_eq!(config.smp.num_shards, 4);
        assert_eq!(config.logging.level, "debug");
    }
    
    #[test]
    fn test_load_toml_config() {
        let toml_content = r#"
[app]
name = "test-app"
version = "1.0.0"
environment = "test"

[reactor]
task_queue_size = 2048
io_polling_timeout_ms = 50

[smp]
num_shards = 4
pin_threads = true

[metrics]
enabled = true
export_interval_secs = 30

[logging]
level = "debug"
format = "json"
"#;
        
        let mut file = NamedTempFile::with_suffix(".toml").unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        
        let loader = ConfigLoader::new();
        let config = loader.load_file(file.path()).unwrap();
        
        assert_eq!(config.app.name, "test-app");
        assert_eq!(config.reactor.task_queue_size, 2048);
        assert_eq!(config.smp.num_shards, 4);
        assert_eq!(config.logging.level, "debug");
    }
    
    #[test]
    fn test_format_detection() {
        assert_eq!(ConfigFormat::from_extension(Path::new("config.yaml")), Some(ConfigFormat::Yaml));
        assert_eq!(ConfigFormat::from_extension(Path::new("config.yml")), Some(ConfigFormat::Yaml));
        assert_eq!(ConfigFormat::from_extension(Path::new("config.toml")), Some(ConfigFormat::Toml));
        assert_eq!(ConfigFormat::from_extension(Path::new("config.json")), Some(ConfigFormat::Json));
    }
    
    #[test]
    fn test_default_config() {
        let loader = ConfigLoader::new();
        // This should not fail even without config file
        let _config = loader.load().unwrap();
    }
}