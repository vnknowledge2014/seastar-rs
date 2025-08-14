//! Integration testing utilities

use async_trait::async_trait;
use futures::Future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum IntegrationError {
    #[error("Environment setup failed: {0}")]
    EnvironmentSetup(String),
    #[error("Service startup failed: {0}")]
    ServiceStartup(String),
    #[error("Test execution failed: {0}")]
    TestExecution(String),
    #[error("Service communication failed: {0}")]
    Communication(String),
    #[error("Timeout error: {0}")]
    Timeout(String),
    #[error("Cleanup failed: {0}")]
    Cleanup(String),
}

pub type IntegrationResult<T> = Result<T, IntegrationError>;

/// Test environment configuration
#[derive(Debug, Clone)]
pub struct TestEnvironmentConfig {
    pub base_port: u16,
    pub working_directory: PathBuf,
    pub cleanup_on_failure: bool,
    pub service_startup_timeout: Duration,
    pub test_timeout: Duration,
    pub log_level: String,
    pub environment_variables: HashMap<String, String>,
}

impl Default for TestEnvironmentConfig {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("RUST_LOG".to_string(), "debug".to_string());
        env_vars.insert("SEASTAR_TEST_MODE".to_string(), "true".to_string());

        Self {
            base_port: 8000,
            working_directory: PathBuf::from("/tmp/seastar-integration-tests"),
            cleanup_on_failure: true,
            service_startup_timeout: Duration::from_secs(30),
            test_timeout: Duration::from_secs(300),
            log_level: "debug".to_string(),
            environment_variables: env_vars,
        }
    }
}

/// Service configuration for integration tests
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub name: String,
    pub binary_path: PathBuf,
    pub args: Vec<String>,
    pub environment: HashMap<String, String>,
    pub health_check_url: Option<String>,
    pub health_check_timeout: Duration,
    pub dependencies: Vec<String>,
    pub ports: Vec<u16>,
}

impl ServiceConfig {
    pub fn new<S: Into<String>, P: Into<PathBuf>>(name: S, binary_path: P) -> Self {
        Self {
            name: name.into(),
            binary_path: binary_path.into(),
            args: Vec::new(),
            environment: HashMap::new(),
            health_check_url: None,
            health_check_timeout: Duration::from_secs(10),
            dependencies: Vec::new(),
            ports: Vec::new(),
        }
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn with_env(mut self, key: String, value: String) -> Self {
        self.environment.insert(key, value);
        self
    }

    pub fn with_health_check(mut self, url: String) -> Self {
        self.health_check_url = Some(url);
        self
    }

    pub fn with_dependency(mut self, service: String) -> Self {
        self.dependencies.push(service);
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.ports.push(port);
        self
    }
}

/// Running service instance
pub struct ServiceInstance {
    pub config: ServiceConfig,
    pub process: Option<Child>,
    pub pid: Option<u32>,
    pub ports: Vec<u16>,
    pub start_time: Instant,
}

impl ServiceInstance {
    fn new(config: ServiceConfig, process: Child, ports: Vec<u16>) -> Self {
        let pid = process.id();
        Self {
            config,
            process: Some(process),
            pid,
            ports,
            start_time: Instant::now(),
        }
    }

    pub async fn stop(&mut self) -> IntegrationResult<()> {
        if let Some(mut process) = self.process.take() {
            debug!("Stopping service: {}", self.config.name);
            
            // Try graceful shutdown first
            if let Some(pid) = self.pid {
                #[cfg(unix)]
                {
                    unsafe {
                        libc::kill(pid as i32, libc::SIGTERM);
                    }
                }
            }

            // Wait for graceful shutdown
            match timeout(Duration::from_secs(10), process.wait()).await {
                Ok(Ok(status)) => {
                    info!("Service {} stopped gracefully: {:?}", self.config.name, status);
                }
                Ok(Err(e)) => {
                    warn!("Error waiting for service {}: {}", self.config.name, e);
                }
                Err(_) => {
                    // Force kill if graceful shutdown timed out
                    warn!("Force killing service: {}", self.config.name);
                    if let Err(e) = process.kill().await {
                        warn!("Failed to kill service {}: {}", self.config.name, e);
                    }
                }
            }
        }
        
        Ok(())
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub async fn health_check(&self) -> bool {
        if let Some(ref url) = self.config.health_check_url {
            match timeout(self.config.health_check_timeout, self.check_health_url(url)).await {
                Ok(Ok(healthy)) => healthy,
                Ok(Err(_)) | Err(_) => false,
            }
        } else {
            // Default health check - just check if process is running
            self.process.is_some()
        }
    }

    async fn check_health_url(&self, url: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let response = reqwest::get(url).await?;
        Ok(response.status().is_success())
    }
}

/// Integration test environment
pub struct TestEnvironment {
    config: TestEnvironmentConfig,
    services: RwLock<HashMap<String, Arc<Mutex<ServiceInstance>>>>,
    port_allocator: Mutex<PortAllocator>,
    working_dir: PathBuf,
}

impl TestEnvironment {
    pub async fn new(config: TestEnvironmentConfig) -> IntegrationResult<Self> {
        let working_dir = config.working_directory.clone();
        
        // Create working directory
        tokio::fs::create_dir_all(&working_dir).await
            .map_err(|e| IntegrationError::EnvironmentSetup(e.to_string()))?;

        let port_allocator = PortAllocator::new(config.base_port);

        Ok(Self {
            config,
            services: RwLock::new(HashMap::new()),
            port_allocator: Mutex::new(port_allocator),
            working_dir,
        })
    }

    /// Start a service in the test environment
    pub async fn start_service(&self, mut service_config: ServiceConfig) -> IntegrationResult<Arc<Mutex<ServiceInstance>>> {
        // Wait for dependencies
        for dependency in &service_config.dependencies {
            self.wait_for_service_health(dependency, Duration::from_secs(60)).await?;
        }

        // Allocate ports
        let ports = if service_config.ports.is_empty() {
            vec![self.port_allocator.lock().await.allocate()]
        } else {
            service_config.ports.clone()
        };

        // Update environment variables with allocated ports
        for (i, &port) in ports.iter().enumerate() {
            service_config.environment.insert(
                format!("PORT_{}", i),
                port.to_string(),
            );
        }

        // Merge with global environment variables
        for (key, value) in &self.config.environment_variables {
            service_config.environment.entry(key.clone()).or_insert_with(|| value.clone());
        }

        info!("Starting service: {} on ports: {:?}", service_config.name, ports);

        // Build command
        let mut command = Command::new(&service_config.binary_path);
        command.args(&service_config.args);
        command.envs(&service_config.environment);
        command.current_dir(&self.working_dir);
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        // Start the process
        let process = command.spawn()
            .map_err(|e| IntegrationError::ServiceStartup(format!("Failed to start {}: {}", service_config.name, e)))?;

        let service_instance = Arc::new(Mutex::new(ServiceInstance::new(service_config.clone(), process, ports)));

        // Wait for service to be healthy
        let health_check_start = Instant::now();
        loop {
            if service_instance.lock().await.health_check().await {
                info!("Service {} is healthy", service_config.name);
                break;
            }

            if health_check_start.elapsed() > self.config.service_startup_timeout {
                return Err(IntegrationError::ServiceStartup(
                    format!("Service {} failed health check after {:?}", service_config.name, self.config.service_startup_timeout)
                ));
            }

            sleep(Duration::from_millis(100)).await;
        }

        // Store service instance
        self.services.write().await.insert(service_config.name.clone(), service_instance.clone());

        Ok(service_instance)
    }

    /// Stop a service
    pub async fn stop_service(&self, service_name: &str) -> IntegrationResult<()> {
        let services = self.services.read().await;
        if let Some(service) = services.get(service_name) {
            service.lock().await.stop().await?;
            drop(services);
            self.services.write().await.remove(service_name);
        }
        Ok(())
    }

    /// Stop all services
    pub async fn stop_all_services(&self) -> IntegrationResult<()> {
        let service_names: Vec<String> = self.services.read().await.keys().cloned().collect();
        
        // Stop services in reverse order (dependencies last)
        for service_name in service_names.iter().rev() {
            if let Err(e) = self.stop_service(service_name).await {
                warn!("Failed to stop service {}: {}", service_name, e);
            }
        }
        
        Ok(())
    }

    /// Get service instance
    pub async fn get_service(&self, service_name: &str) -> Option<Arc<Mutex<ServiceInstance>>> {
        self.services.read().await.get(service_name).cloned()
    }

    /// Wait for a service to be healthy
    pub async fn wait_for_service_health(&self, service_name: &str, timeout_duration: Duration) -> IntegrationResult<()> {
        let start_time = Instant::now();
        
        loop {
            if let Some(service) = self.get_service(service_name).await {
                if service.lock().await.health_check().await {
                    return Ok(());
                }
            }

            if start_time.elapsed() > timeout_duration {
                return Err(IntegrationError::Timeout(
                    format!("Service {} not healthy after {:?}", service_name, timeout_duration)
                ));
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Get allocated port for a service
    pub async fn get_service_port(&self, service_name: &str, port_index: usize) -> Option<u16> {
        self.services.read().await
            .get(service_name)?
            .lock().await
            .ports
            .get(port_index)
            .copied()
    }

    /// Get service base URL
    pub async fn get_service_url(&self, service_name: &str) -> Option<String> {
        let port = self.get_service_port(service_name, 0).await?;
        Some(format!("http://localhost:{}", port))
    }

    /// Cleanup test environment
    pub async fn cleanup(&self) -> IntegrationResult<()> {
        info!("Cleaning up test environment");
        
        // Stop all services
        self.stop_all_services().await?;
        
        // Remove working directory
        if self.working_dir.exists() {
            tokio::fs::remove_dir_all(&self.working_dir).await
                .map_err(|e| IntegrationError::Cleanup(e.to_string()))?;
        }
        
        Ok(())
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        // Best-effort cleanup on drop
        let working_dir = self.working_dir.clone();
        tokio::spawn(async move {
            if working_dir.exists() {
                let _ = tokio::fs::remove_dir_all(&working_dir).await;
            }
        });
    }
}

/// Port allocator for services
struct PortAllocator {
    next_port: u16,
}

impl PortAllocator {
    fn new(base_port: u16) -> Self {
        Self {
            next_port: base_port,
        }
    }

    fn allocate(&mut self) -> u16 {
        let port = self.next_port;
        self.next_port += 1;
        port
    }
}

/// Integration test trait
#[async_trait]
pub trait IntegrationTest: Send + Sync {
    type Setup: Send + Sync;
    type Result: Send + Sync;

    /// Setup the test environment and services
    async fn setup(&self, env: &TestEnvironment) -> IntegrationResult<Self::Setup>;

    /// Execute the integration test
    async fn execute(&self, env: &TestEnvironment, setup: Self::Setup) -> IntegrationResult<(Self::Result, Self::Setup)>;

    /// Cleanup after the test
    async fn cleanup(&self, env: &TestEnvironment, setup: Self::Setup) -> IntegrationResult<()>;

    /// Name of the test
    fn name(&self) -> &str;

    /// Run the complete integration test
    async fn run(&self, config: TestEnvironmentConfig) -> IntegrationResult<Self::Result> {
        let env = TestEnvironment::new(config).await?;
        
        let setup = match self.setup(&env).await {
            Ok(setup) => setup,
            Err(e) => {
                env.cleanup().await.unwrap_or_default();
                return Err(e);
            }
        };

        let result = match timeout(env.config.test_timeout, self.execute(&env, setup)).await {
            Ok(Ok((result, returned_setup))) => {
                self.cleanup(&env, returned_setup).await?;
                env.cleanup().await?;
                Ok(result)
            }
            Ok(Err(e)) => {
                env.cleanup().await.unwrap_or_default();
                Err(e)
            }
            Err(_) => {
                env.cleanup().await.unwrap_or_default();
                Err(IntegrationError::Timeout("Test execution timed out".to_string()))
            }
        };

        result
    }
}

/// Test result information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationTestResult {
    pub test_name: String,
    pub success: bool,
    pub duration_ms: u64,
    pub error_message: Option<String>,
    pub services_started: Vec<String>,
    pub resource_usage: HashMap<String, serde_json::Value>,
}

/// Built-in integration test scenarios
pub struct IntegrationScenarios;

impl IntegrationScenarios {
    /// HTTP service test
    pub fn http_service_test(
        service_binary: PathBuf,
        test_endpoints: Vec<String>,
    ) -> impl IntegrationTest<Setup = Arc<Mutex<ServiceInstance>>, Result = Vec<reqwest::Response>> {
        HttpServiceTest {
            service_binary,
            test_endpoints,
        }
    }

    /// Multi-service communication test
    pub fn multi_service_test(
        services: Vec<ServiceConfig>,
    ) -> impl IntegrationTest<Setup = Vec<Arc<Mutex<ServiceInstance>>>, Result = Vec<String>> {
        MultiServiceTest { services }
    }

    /// Database integration test
    pub fn database_test(
        app_binary: PathBuf,
        database_url: String,
    ) -> impl IntegrationTest<Setup = (Arc<Mutex<ServiceInstance>>, String), Result = Vec<serde_json::Value>> {
        DatabaseIntegrationTest {
            app_binary,
            database_url,
        }
    }
}

struct HttpServiceTest {
    service_binary: PathBuf,
    test_endpoints: Vec<String>,
}

#[async_trait]
impl IntegrationTest for HttpServiceTest {
    type Setup = Arc<Mutex<ServiceInstance>>;
    type Result = Vec<reqwest::Response>;

    async fn setup(&self, env: &TestEnvironment) -> IntegrationResult<Self::Setup> {
        let service_config = ServiceConfig::new("http_service", &self.service_binary)
            .with_health_check("http://localhost:8000/health".to_string())
            .with_port(8000);

        env.start_service(service_config).await
    }

    async fn execute(&self, env: &TestEnvironment, setup: Self::Setup) -> IntegrationResult<(Self::Result, Self::Setup)> {
        let base_url = env.get_service_url("http_service").await
            .ok_or_else(|| IntegrationError::TestExecution("Service URL not found".to_string()))?;

        let mut responses = Vec::new();
        let client = reqwest::Client::new();

        for endpoint in &self.test_endpoints {
            let url = format!("{}{}", base_url, endpoint);
            let response = client.get(&url).send().await
                .map_err(|e| IntegrationError::Communication(e.to_string()))?;
            responses.push(response);
        }

        Ok((responses, setup))
    }

    async fn cleanup(&self, _env: &TestEnvironment, _setup: Self::Setup) -> IntegrationResult<()> {
        // Service cleanup is handled by environment
        Ok(())
    }

    fn name(&self) -> &str {
        "http_service_test"
    }
}

struct MultiServiceTest {
    services: Vec<ServiceConfig>,
}

#[async_trait]
impl IntegrationTest for MultiServiceTest {
    type Setup = Vec<Arc<Mutex<ServiceInstance>>>;
    type Result = Vec<String>;

    async fn setup(&self, env: &TestEnvironment) -> IntegrationResult<Self::Setup> {
        let mut instances = Vec::new();
        
        for service_config in &self.services {
            let instance = env.start_service(service_config.clone()).await?;
            instances.push(instance);
        }
        
        Ok(instances)
    }

    async fn execute(&self, env: &TestEnvironment, setup: Self::Setup) -> IntegrationResult<(Self::Result, Self::Setup)> {
        let mut results = Vec::new();
        
        // Test communication between services
        for service_config in &self.services {
            if let Some(url) = env.get_service_url(&service_config.name).await {
                let client = reqwest::Client::new();
                match client.get(&format!("{}/status", url)).send().await {
                    Ok(response) => {
                        let status = response.status().to_string();
                        results.push(format!("Service {}: {}", service_config.name, status));
                    }
                    Err(e) => {
                        results.push(format!("Service {} error: {}", service_config.name, e));
                    }
                }
            }
        }
        
        Ok((results, setup))
    }

    async fn cleanup(&self, _env: &TestEnvironment, _setup: Self::Setup) -> IntegrationResult<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "multi_service_test"
    }
}

struct DatabaseIntegrationTest {
    app_binary: PathBuf,
    database_url: String,
}

#[async_trait]
impl IntegrationTest for DatabaseIntegrationTest {
    type Setup = (Arc<Mutex<ServiceInstance>>, String);
    type Result = Vec<serde_json::Value>;

    async fn setup(&self, env: &TestEnvironment) -> IntegrationResult<Self::Setup> {
        let service_config = ServiceConfig::new("database_app", &self.app_binary)
            .with_env("DATABASE_URL".to_string(), self.database_url.clone())
            .with_health_check("http://localhost:8000/health".to_string())
            .with_port(8000);

        let instance = env.start_service(service_config).await?;
        Ok((instance, self.database_url.clone()))
    }

    async fn execute(&self, env: &TestEnvironment, setup: Self::Setup) -> IntegrationResult<(Self::Result, Self::Setup)> {
        let (_, database_url) = setup;
        let base_url = env.get_service_url("database_app").await
            .ok_or_else(|| IntegrationError::TestExecution("Service URL not found".to_string()))?;

        let client = reqwest::Client::new();
        let mut results = Vec::new();

        // Test database operations through the service
        let endpoints = vec![
            "/api/users",
            "/api/users/1",
            "/api/health/db",
        ];

        for endpoint in endpoints {
            let url = format!("{}{}", base_url, endpoint);
            match client.get(&url).send().await {
                Ok(response) => {
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => results.push(json),
                        Err(_) => results.push(serde_json::json!({
                            "error": "Failed to parse response",
                            "endpoint": endpoint
                        })),
                    }
                }
                Err(e) => {
                    results.push(serde_json::json!({
                        "error": e.to_string(),
                        "endpoint": endpoint
                    }));
                }
            }
        }

        Ok((results, (setup.0, database_url)))
    }

    async fn cleanup(&self, _env: &TestEnvironment, _setup: Self::Setup) -> IntegrationResult<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "database_integration_test"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_test_environment_creation() {
        let config = TestEnvironmentConfig::default();
        let env = TestEnvironment::new(config).await;
        assert!(env.is_ok());
        
        let env = env.unwrap();
        env.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_port_allocator() {
        let mut allocator = PortAllocator::new(9000);
        
        let port1 = allocator.allocate();
        let port2 = allocator.allocate();
        
        assert_eq!(port1, 9000);
        assert_eq!(port2, 9001);
        assert_ne!(port1, port2);
    }

    #[tokio::test]
    async fn test_service_config() {
        let config = ServiceConfig::new("test-service", "/usr/bin/test")
            .with_args(vec!["--port".to_string(), "8080".to_string()])
            .with_env("LOG_LEVEL".to_string(), "debug".to_string())
            .with_health_check("http://localhost:8080/health".to_string())
            .with_dependency("database".to_string())
            .with_port(8080);

        assert_eq!(config.name, "test-service");
        assert_eq!(config.args, vec!["--port", "8080"]);
        assert_eq!(config.environment.get("LOG_LEVEL"), Some(&"debug".to_string()));
        assert_eq!(config.health_check_url, Some("http://localhost:8080/health".to_string()));
        assert_eq!(config.dependencies, vec!["database"]);
        assert_eq!(config.ports, vec![8080]);
    }
}