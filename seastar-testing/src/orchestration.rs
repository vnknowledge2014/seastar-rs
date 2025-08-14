//! Test orchestration and execution planning

use crate::chaos::{ChaosEngine, FaultInjection};
use crate::integration::{IntegrationTest, TestEnvironmentConfig};
use crate::performance::{PerformanceTest, PerformanceTestConfig};
use crate::property::{PropertyTest, PropertyTestResult};
use crate::reporting::{TestReporter, TestResults};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Error, Debug)]
pub enum OrchestrationError {
    #[error("Test plan validation failed: {0}")]
    PlanValidation(String),
    #[error("Test execution failed: {0}")]
    TestExecution(String),
    #[error("Resource allocation failed: {0}")]
    ResourceAllocation(String),
    #[error("Dependency resolution failed: {0}")]
    DependencyResolution(String),
    #[error("Timeout error: {0}")]
    Timeout(String),
}

pub type OrchestrationResult<T> = Result<T, OrchestrationError>;

/// Test execution mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Run tests sequentially
    Sequential,
    /// Run tests in parallel with specified concurrency
    Parallel { max_concurrent: usize },
    /// Run tests in groups (parallel within groups, sequential between groups)
    Grouped { group_size: usize, max_concurrent_per_group: usize },
}

/// Test plan configuration
#[derive(Debug, Clone)]
pub struct TestPlanConfig {
    pub execution_mode: ExecutionMode,
    pub retry_failed_tests: bool,
    pub max_retries: usize,
    pub fail_fast: bool,
    pub timeout: Duration,
    pub cleanup_on_failure: bool,
    pub collect_metrics: bool,
    pub enable_chaos: bool,
}

impl Default for TestPlanConfig {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::Sequential,
            retry_failed_tests: true,
            max_retries: 2,
            fail_fast: false,
            timeout: Duration::from_secs(3600), // 1 hour
            cleanup_on_failure: true,
            collect_metrics: true,
            enable_chaos: false,
        }
    }
}

/// Test case definition
#[derive(Debug, Clone)]
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub description: String,
    pub test_type: TestType,
    pub dependencies: Vec<String>,
    pub tags: Vec<String>,
    pub timeout: Option<Duration>,
    pub retry_count: usize,
    pub skip_condition: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TestType {
    Unit,
    Integration { config: TestEnvironmentConfig },
    Performance { config: PerformanceTestConfig },
    Property { config: crate::property::PropertyConfig },
    Chaos { faults: Vec<FaultInjection> },
    EndToEnd,
}

/// Test execution plan
pub struct TestPlan {
    pub config: TestPlanConfig,
    pub test_cases: Vec<TestCase>,
    pub test_groups: HashMap<String, Vec<String>>, // group_name -> test_ids
    pub global_setup: Option<Box<dyn Fn() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync>>,
    pub global_teardown: Option<Box<dyn Fn() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync>>,
}

impl TestPlan {
    pub fn new(config: TestPlanConfig) -> Self {
        Self {
            config,
            test_cases: Vec::new(),
            test_groups: HashMap::new(),
            global_setup: None,
            global_teardown: None,
        }
    }

    pub fn add_test_case(mut self, test_case: TestCase) -> Self {
        self.test_cases.push(test_case);
        self
    }

    pub fn add_test_group(mut self, group_name: String, test_ids: Vec<String>) -> Self {
        self.test_groups.insert(group_name, test_ids);
        self
    }

    pub fn with_global_setup<F>(mut self, setup: F) -> Self
    where
        F: Fn() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        self.global_setup = Some(Box::new(setup));
        self
    }

    pub fn with_global_teardown<F>(mut self, teardown: F) -> Self
    where
        F: Fn() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        self.global_teardown = Some(Box::new(teardown));
        self
    }

    /// Validate the test plan
    pub fn validate(&self) -> OrchestrationResult<()> {
        // Check for duplicate test IDs
        let mut seen_ids = std::collections::HashSet::new();
        for test_case in &self.test_cases {
            if !seen_ids.insert(&test_case.id) {
                return Err(OrchestrationError::PlanValidation(
                    format!("Duplicate test ID: {}", test_case.id)
                ));
            }
        }

        // Check dependencies exist
        let test_ids: std::collections::HashSet<_> = self.test_cases.iter().map(|t| &t.id).collect();
        for test_case in &self.test_cases {
            for dep in &test_case.dependencies {
                if !test_ids.contains(dep) {
                    return Err(OrchestrationError::DependencyResolution(
                        format!("Test {} depends on non-existent test {}", test_case.id, dep)
                    ));
                }
            }
        }

        // Check for circular dependencies
        if self.has_circular_dependencies() {
            return Err(OrchestrationError::DependencyResolution(
                "Circular dependencies detected".to_string()
            ));
        }

        Ok(())
    }

    fn has_circular_dependencies(&self) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();
        
        for test_case in &self.test_cases {
            if self.has_cycle(&test_case.id, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        
        false
    }

    fn has_cycle(
        &self,
        test_id: &str,
        visited: &mut std::collections::HashSet<String>,
        rec_stack: &mut std::collections::HashSet<String>,
    ) -> bool {
        visited.insert(test_id.to_string());
        rec_stack.insert(test_id.to_string());

        if let Some(test_case) = self.test_cases.iter().find(|t| t.id == test_id) {
            for dep in &test_case.dependencies {
                if !visited.contains(dep) && self.has_cycle(dep, visited, rec_stack) {
                    return true;
                }
                if rec_stack.contains(dep) {
                    return true;
                }
            }
        }

        rec_stack.remove(test_id);
        false
    }

    /// Get tests in execution order (topological sort)
    pub fn get_execution_order(&self) -> OrchestrationResult<Vec<String>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        // Initialize
        for test_case in &self.test_cases {
            in_degree.insert(test_case.id.clone(), test_case.dependencies.len());
            adjacency.insert(test_case.id.clone(), Vec::new());
        }

        // Build adjacency list (reverse edges for topological sort)
        for test_case in &self.test_cases {
            for dep in &test_case.dependencies {
                adjacency.get_mut(dep).unwrap().push(test_case.id.clone());
            }
        }

        // Kahn's algorithm for topological sorting
        let mut queue = std::collections::VecDeque::new();
        let mut result = Vec::new();

        // Find all nodes with no incoming edge
        for (test_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(test_id.clone());
            }
        }

        while let Some(test_id) = queue.pop_front() {
            result.push(test_id.clone());

            // For each neighbor of the current test
            if let Some(neighbors) = adjacency.get(&test_id) {
                for neighbor in neighbors {
                    if let Some(degree) = in_degree.get_mut(neighbor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
            }
        }

        if result.len() != self.test_cases.len() {
            return Err(OrchestrationError::DependencyResolution(
                "Circular dependency detected during topological sort".to_string()
            ));
        }

        Ok(result)
    }
}

impl std::fmt::Debug for TestPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestPlan")
            .field("config", &self.config)
            .field("test_cases", &self.test_cases)
            .field("test_groups", &self.test_groups)
            .field("global_setup", &self.global_setup.is_some())
            .field("global_teardown", &self.global_teardown.is_some())
            .finish()
    }
}

/// Test execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestExecutionResult {
    pub test_id: String,
    pub test_name: String,
    pub success: bool,
    pub duration_ms: u64,
    pub retry_count: usize,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, serde_json::Value>,
    pub chaos_faults_injected: usize,
}

/// Test orchestrator
pub struct TestOrchestrator {
    plan: TestPlan,
    chaos_engine: Option<Arc<ChaosEngine>>,
    reporter: Arc<dyn TestReporter>,
    semaphore: Option<Arc<Semaphore>>,
}

impl TestOrchestrator {
    pub fn new(plan: TestPlan, reporter: Arc<dyn TestReporter>) -> OrchestrationResult<Self> {
        plan.validate()?;

        let semaphore = match &plan.config.execution_mode {
            ExecutionMode::Parallel { max_concurrent } => Some(Arc::new(Semaphore::new(*max_concurrent))),
            ExecutionMode::Grouped { max_concurrent_per_group, .. } => Some(Arc::new(Semaphore::new(*max_concurrent_per_group))),
            ExecutionMode::Sequential => None,
        };

        let chaos_engine = if plan.config.enable_chaos {
            Some(Arc::new(ChaosEngine::new(seastar_config::ChaosConfig {
                failure_rate: 0.01,
                latency_injection_ms: 100,
                enable_memory_pressure: true,
                enable_cpu_pressure: true,
            })))
        } else {
            None
        };

        Ok(Self {
            plan,
            chaos_engine,
            reporter,
            semaphore,
        })
    }

    /// Execute the test plan
    pub async fn execute(&self) -> OrchestrationResult<TestResults> {
        info!("Starting test execution with {} test cases", self.plan.test_cases.len());

        let start_time = Instant::now();
        
        // Global setup
        if let Some(ref setup) = self.plan.global_setup {
            info!("Running global setup");
            setup().map_err(|e| OrchestrationError::TestExecution(e.to_string()))?;
        }

        // Execute tests based on execution mode
        let results = match &self.plan.config.execution_mode {
            ExecutionMode::Sequential => {
                self.execute_sequential().await?
            }
            ExecutionMode::Parallel { .. } => {
                self.execute_parallel().await?
            }
            ExecutionMode::Grouped { group_size, .. } => {
                self.execute_grouped(*group_size).await?
            }
        };

        // Global teardown
        if let Some(ref teardown) = self.plan.global_teardown {
            info!("Running global teardown");
            teardown().map_err(|e| OrchestrationError::TestExecution(e.to_string()))?;
        }

        let total_duration = start_time.elapsed();
        let successful_tests = results.iter().filter(|r| r.success).count();
        let failed_tests = results.len() - successful_tests;
        let total_tests = results.len();

        let test_results = TestResults {
            total_tests,
            successful_tests,
            failed_tests,
            skipped_tests: 0,
            total_duration_ms: total_duration.as_millis() as u64,
            test_results: results,
            summary: format!(
                "Executed {} tests in {:.2}s - {} passed, {} failed",
                total_tests,
                total_duration.as_secs_f64(),
                successful_tests,
                failed_tests
            ),
        };

        // Report results
        self.reporter.generate_report(&test_results).await
            .map_err(|e| OrchestrationError::TestExecution(e.to_string()))?;

        Ok(test_results)
    }

    async fn execute_sequential(&self) -> OrchestrationResult<Vec<TestExecutionResult>> {
        let execution_order = self.plan.get_execution_order()?;
        let mut results = Vec::new();

        for test_id in execution_order {
            let result = self.execute_single_test(&test_id).await?;
            
            if !result.success && self.plan.config.fail_fast {
                error!("Test {} failed, stopping execution due to fail_fast", test_id);
                results.push(result);
                break;
            }
            
            results.push(result);
        }

        Ok(results)
    }

    async fn execute_parallel(&self) -> OrchestrationResult<Vec<TestExecutionResult>> {
        let execution_order = self.plan.get_execution_order()?;
        let semaphore = self.semaphore.as_ref().unwrap();
        
        let mut handles = Vec::new();
        
        for test_id in execution_order {
            let permit = semaphore.clone().acquire_owned().await
                .map_err(|e| OrchestrationError::ResourceAllocation(e.to_string()))?;
            
            let test_id_clone = test_id.clone();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                // TODO: Execute test (requires refactoring to avoid self borrowing issues)
                TestExecutionResult {
                    test_id: test_id_clone,
                    test_name: "placeholder".to_string(),
                    success: true,
                    duration_ms: 0,
                    retry_count: 0,
                    error_message: None,
                    metrics: HashMap::new(),
                    chaos_faults_injected: 0,
                }
            });
            
            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| OrchestrationError::TestExecution(e.to_string()))?;

        Ok(results)
    }

    async fn execute_grouped(&self, group_size: usize) -> OrchestrationResult<Vec<TestExecutionResult>> {
        let execution_order = self.plan.get_execution_order()?;
        let mut results = Vec::new();

        // Execute tests in groups
        for chunk in execution_order.chunks(group_size) {
            let group_results = self.execute_test_group(chunk).await?;
            
            let group_failed = group_results.iter().any(|r| !r.success);
            if group_failed && self.plan.config.fail_fast {
                results.extend(group_results);
                break;
            }
            
            results.extend(group_results);
        }

        Ok(results)
    }

    async fn execute_test_group(&self, test_ids: &[String]) -> OrchestrationResult<Vec<TestExecutionResult>> {
        let semaphore = self.semaphore.as_ref().unwrap();
        let mut handles = Vec::new();
        
        for test_id in test_ids {
            let permit = semaphore.clone().acquire_owned().await
                .map_err(|e| OrchestrationError::ResourceAllocation(e.to_string()))?;
            
            let test_id_clone = test_id.clone();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                // TODO: Execute test
                TestExecutionResult {
                    test_id: test_id_clone,
                    test_name: "placeholder".to_string(),
                    success: true,
                    duration_ms: 0,
                    retry_count: 0,
                    error_message: None,
                    metrics: HashMap::new(),
                    chaos_faults_injected: 0,
                }
            });
            
            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| OrchestrationError::TestExecution(e.to_string()))?;

        Ok(results)
    }

    async fn execute_single_test(&self, test_id: &str) -> OrchestrationResult<TestExecutionResult> {
        let test_case = self.plan.test_cases.iter()
            .find(|t| t.id == test_id)
            .ok_or_else(|| OrchestrationError::TestExecution(format!("Test not found: {}", test_id)))?;

        info!("Executing test: {} - {}", test_case.id, test_case.name);

        let start_time = Instant::now();
        let mut retry_count = 0;
        let mut last_error = None;
        let max_retries = if self.plan.config.retry_failed_tests { 
            self.plan.config.max_retries 
        } else { 
            0 
        };

        loop {
            let test_timeout = test_case.timeout.unwrap_or(self.plan.config.timeout);
            
            match tokio::time::timeout(test_timeout, self.run_test_case(test_case)).await {
                Ok(Ok(_)) => {
                    let duration = start_time.elapsed();
                    return Ok(TestExecutionResult {
                        test_id: test_case.id.clone(),
                        test_name: test_case.name.clone(),
                        success: true,
                        duration_ms: duration.as_millis() as u64,
                        retry_count,
                        error_message: None,
                        metrics: HashMap::new(), // TODO: Collect actual metrics
                        chaos_faults_injected: 0, // TODO: Track chaos faults
                    });
                }
                Ok(Err(e)) => {
                    last_error = Some(e.to_string());
                }
                Err(_) => {
                    last_error = Some("Test timed out".to_string());
                }
            }

            retry_count += 1;
            if retry_count > max_retries {
                break;
            }

            warn!("Test {} failed, retrying ({}/{})", test_id, retry_count, max_retries);
            tokio::time::sleep(Duration::from_secs(1)).await; // Brief delay between retries
        }

        let duration = start_time.elapsed();
        Ok(TestExecutionResult {
            test_id: test_case.id.clone(),
            test_name: test_case.name.clone(),
            success: false,
            duration_ms: duration.as_millis() as u64,
            retry_count,
            error_message: last_error,
            metrics: HashMap::new(),
            chaos_faults_injected: 0,
        })
    }

    async fn run_test_case(&self, _test_case: &TestCase) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Implement actual test execution based on test type
        // This is a placeholder implementation
        Ok(())
    }
}

/// Test plan builder
pub struct TestPlanBuilder {
    config: TestPlanConfig,
    test_cases: Vec<TestCase>,
    test_groups: HashMap<String, Vec<String>>,
}

impl TestPlanBuilder {
    pub fn new() -> Self {
        Self {
            config: TestPlanConfig::default(),
            test_cases: Vec::new(),
            test_groups: HashMap::new(),
        }
    }

    pub fn with_config(mut self, config: TestPlanConfig) -> Self {
        self.config = config;
        self
    }

    pub fn execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.config.execution_mode = mode;
        self
    }

    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.config.fail_fast = fail_fast;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn enable_chaos(mut self, enable: bool) -> Self {
        self.config.enable_chaos = enable;
        self
    }

    pub fn add_unit_test(mut self, id: String, name: String) -> Self {
        self.test_cases.push(TestCase {
            id,
            name,
            description: String::new(),
            test_type: TestType::Unit,
            dependencies: Vec::new(),
            tags: Vec::new(),
            timeout: None,
            retry_count: 0,
            skip_condition: None,
        });
        self
    }

    pub fn add_integration_test(mut self, id: String, name: String, config: TestEnvironmentConfig) -> Self {
        self.test_cases.push(TestCase {
            id,
            name,
            description: String::new(),
            test_type: TestType::Integration { config },
            dependencies: Vec::new(),
            tags: Vec::new(),
            timeout: None,
            retry_count: 0,
            skip_condition: None,
        });
        self
    }

    pub fn add_performance_test(mut self, id: String, name: String, config: PerformanceTestConfig) -> Self {
        self.test_cases.push(TestCase {
            id,
            name,
            description: String::new(),
            test_type: TestType::Performance { config },
            dependencies: Vec::new(),
            tags: Vec::new(),
            timeout: None,
            retry_count: 0,
            skip_condition: None,
        });
        self
    }

    pub fn add_dependency(mut self, test_id: &str, dependency: String) -> Self {
        if let Some(test_case) = self.test_cases.iter_mut().find(|t| t.id == test_id) {
            test_case.dependencies.push(dependency);
        }
        self
    }

    pub fn add_group(mut self, group_name: String, test_ids: Vec<String>) -> Self {
        self.test_groups.insert(group_name, test_ids);
        self
    }

    pub fn build(self) -> TestPlan {
        TestPlan {
            config: self.config,
            test_cases: self.test_cases,
            test_groups: self.test_groups,
            global_setup: None,
            global_teardown: None,
        }
    }
}

impl Default for TestPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporting::JsonReporter;

    #[test]
    fn test_test_plan_validation() {
        let plan = TestPlanBuilder::new()
            .add_unit_test("test1".to_string(), "Test 1".to_string())
            .add_unit_test("test2".to_string(), "Test 2".to_string())
            .add_dependency("test2", "test1".to_string())
            .build();

        assert!(plan.validate().is_ok());
    }

    #[test]
    fn test_circular_dependency_detection() {
        let plan = TestPlanBuilder::new()
            .add_unit_test("test1".to_string(), "Test 1".to_string())
            .add_unit_test("test2".to_string(), "Test 2".to_string())
            .add_dependency("test1", "test2".to_string())
            .add_dependency("test2", "test1".to_string())
            .build();

        assert!(plan.validate().is_err());
    }

    #[test]
    fn test_execution_order() {
        let plan = TestPlanBuilder::new()
            .add_unit_test("test1".to_string(), "Test 1".to_string())
            .add_unit_test("test2".to_string(), "Test 2".to_string())
            .add_unit_test("test3".to_string(), "Test 3".to_string())
            .add_dependency("test2", "test1".to_string())
            .add_dependency("test3", "test2".to_string())
            .build();

        let order = plan.get_execution_order().unwrap();
        
        // test1 should come before test2, test2 before test3
        let pos1 = order.iter().position(|t| t == "test1").unwrap();
        let pos2 = order.iter().position(|t| t == "test2").unwrap();
        let pos3 = order.iter().position(|t| t == "test3").unwrap();
        
        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
    }

    #[tokio::test]
    async fn test_orchestrator_creation() {
        let plan = TestPlanBuilder::new()
            .add_unit_test("test1".to_string(), "Test 1".to_string())
            .build();

        let reporter = Arc::new(JsonReporter::new("/tmp/test-results.json".into()));
        let orchestrator = TestOrchestrator::new(plan, reporter);

        assert!(orchestrator.is_ok());
    }
}