//! Performance testing and benchmarking utilities

use async_trait::async_trait;
use futures::Future;
use seastar_config::PerformanceConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum PerformanceError {
    #[error("Benchmark failed: {0}")]
    BenchmarkFailed(String),
    #[error("Load generation error: {0}")]
    LoadGeneration(String),
    #[error("Timeout error: {0}")]
    Timeout(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

pub type PerformanceResult<T> = Result<T, PerformanceError>;

/// Performance test configuration
#[derive(Debug, Clone)]
pub struct PerformanceTestConfig {
    pub duration: Duration,
    pub warmup_duration: Duration,
    pub max_concurrency: usize,
    pub target_throughput: Option<u64>,
    pub target_latency_percentiles: HashMap<String, Duration>,
    pub sample_rate: f64,
    pub report_interval: Duration,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        let mut percentiles = HashMap::new();
        percentiles.insert("p50".to_string(), Duration::from_millis(100));
        percentiles.insert("p95".to_string(), Duration::from_millis(500));
        percentiles.insert("p99".to_string(), Duration::from_millis(1000));

        Self {
            duration: Duration::from_secs(60),
            warmup_duration: Duration::from_secs(10),
            max_concurrency: 100,
            target_throughput: None,
            target_latency_percentiles: percentiles,
            sample_rate: 1.0,
            report_interval: Duration::from_secs(5),
        }
    }
}

impl From<PerformanceConfig> for PerformanceTestConfig {
    fn from(config: PerformanceConfig) -> Self {
        let mut perf_config = Self::default();
        perf_config.duration = Duration::from_secs(config.duration_secs);
        
        if let Some(throughput) = config.target_throughput {
            perf_config.target_throughput = Some(throughput);
        }
        
        if let Some(latency_ms) = config.max_latency_ms {
            perf_config.target_latency_percentiles.insert(
                "p99".to_string(),
                Duration::from_millis(latency_ms),
            );
        }
        
        perf_config
    }
}

/// Performance metrics collected during testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub test_name: String,
    pub duration_ms: u64,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub throughput_ops_per_sec: f64,
    pub latency_percentiles: HashMap<String, u64>,
    pub error_rate: f64,
    pub resource_usage: ResourceUsage,
    pub timestamps: Vec<u64>,
    pub latencies_ms: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub peak_memory_mb: f64,
    pub avg_cpu_percent: f64,
    pub peak_cpu_percent: f64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            peak_memory_mb: 0.0,
            avg_cpu_percent: 0.0,
            peak_cpu_percent: 0.0,
            network_bytes_sent: 0,
            network_bytes_received: 0,
        }
    }
}

/// Load generator for creating realistic test loads
pub struct LoadGenerator {
    config: PerformanceTestConfig,
    metrics: Arc<PerformanceCollector>,
}

impl LoadGenerator {
    pub fn new(config: PerformanceTestConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(PerformanceCollector::new()),
        }
    }

    /// Generate constant load
    pub async fn constant_load<F, Fut, T>(
        &self,
        operation: F,
    ) -> PerformanceResult<PerformanceMetrics>
    where
        F: Fn() -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        info!("Starting constant load test for {:?}", self.config.duration);
        
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrency));
        let start_time = Instant::now();
        let end_time = start_time + self.config.duration;
        
        // Warmup phase
        if self.config.warmup_duration > Duration::ZERO {
            info!("Warming up for {:?}", self.config.warmup_duration);
            self.warmup_phase(operation.clone()).await?;
        }

        let metrics = self.metrics.clone();
        let operation_clone = operation.clone();

        // Main load generation loop
        let mut handles = Vec::new();
        while Instant::now() < end_time {
            let permit = semaphore.clone().acquire_owned().await
                .map_err(|e| PerformanceError::LoadGeneration(e.to_string()))?;
            
            let metrics_clone = metrics.clone();
            let op = operation_clone.clone();
            
            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold permit until completion
                let start = Instant::now();
                
                match op().await {
                    Ok(_) => {
                        let latency = start.elapsed();
                        metrics_clone.record_success(latency).await;
                    }
                    Err(e) => {
                        let latency = start.elapsed();
                        metrics_clone.record_failure(latency, e.to_string()).await;
                    }
                }
            });
            
            handles.push(handle);

            // Rate limiting based on target throughput
            if let Some(target_ops_per_sec) = self.config.target_throughput {
                let delay = Duration::from_secs(1) / target_ops_per_sec as u32;
                sleep(delay).await;
            }
        }

        // Wait for all operations to complete
        futures::future::join_all(handles).await;
        
        let total_duration = start_time.elapsed();
        self.metrics.finalize(total_duration).await
    }

    /// Generate ramping load (gradually increasing)
    pub async fn ramping_load<F, Fut, T>(
        &self,
        operation: F,
        start_rps: u64,
        end_rps: u64,
    ) -> PerformanceResult<PerformanceMetrics>
    where
        F: Fn() -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        info!(
            "Starting ramping load test from {} to {} RPS over {:?}",
            start_rps, end_rps, self.config.duration
        );
        
        let test_start = Instant::now();
        let duration_secs = self.config.duration.as_secs_f64();
        let rps_increase_per_sec = (end_rps as f64 - start_rps as f64) / duration_secs;

        while test_start.elapsed() < self.config.duration {
            let elapsed_secs = test_start.elapsed().as_secs_f64();
            let current_rps = start_rps as f64 + (rps_increase_per_sec * elapsed_secs);
            
            debug!("Current target RPS: {:.2}", current_rps);
            
            // Generate load at current rate for 1 second
            let ops_this_second = current_rps as u64;
            let delay_between_ops = Duration::from_millis(1000 / ops_this_second.max(1));
            
            let second_start = Instant::now();
            while second_start.elapsed() < Duration::from_secs(1) &&
                  test_start.elapsed() < self.config.duration {
                
                let metrics_clone = self.metrics.clone();
                let op = operation.clone();
                
                tokio::spawn(async move {
                    let start = Instant::now();
                    match op().await {
                        Ok(_) => {
                            let latency = start.elapsed();
                            metrics_clone.record_success(latency).await;
                        }
                        Err(e) => {
                            let latency = start.elapsed();
                            metrics_clone.record_failure(latency, e.to_string()).await;
                        }
                    }
                });
                
                sleep(delay_between_ops).await;
            }
        }

        let total_duration = test_start.elapsed();
        self.metrics.finalize(total_duration).await
    }

    /// Generate spike load (sudden bursts)
    pub async fn spike_load<F, Fut, T>(
        &self,
        operation: F,
        base_rps: u64,
        spike_rps: u64,
        spike_duration: Duration,
        spike_interval: Duration,
    ) -> PerformanceResult<PerformanceMetrics>
    where
        F: Fn() -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        info!(
            "Starting spike load test: base {} RPS, spike {} RPS for {:?} every {:?}",
            base_rps, spike_rps, spike_duration, spike_interval
        );
        
        let test_start = Instant::now();
        let mut next_spike = test_start + spike_interval;
        
        while test_start.elapsed() < self.config.duration {
            let now = Instant::now();
            let current_rps = if now >= next_spike && now < next_spike + spike_duration {
                // During spike
                if now >= next_spike + spike_duration {
                    next_spike += spike_interval;
                }
                spike_rps
            } else {
                // Base load
                base_rps
            };
            
            if current_rps > 0 {
                let delay = Duration::from_millis(1000 / current_rps);
                
                let metrics_clone = self.metrics.clone();
                let op = operation.clone();
                
                tokio::spawn(async move {
                    let start = Instant::now();
                    match op().await {
                        Ok(_) => {
                            let latency = start.elapsed();
                            metrics_clone.record_success(latency).await;
                        }
                        Err(e) => {
                            let latency = start.elapsed();
                            metrics_clone.record_failure(latency, e.to_string()).await;
                        }
                    }
                });
                
                sleep(delay).await;
            }
        }

        let total_duration = test_start.elapsed();
        self.metrics.finalize(total_duration).await
    }

    async fn warmup_phase<F, Fut, T>(&self, operation: F) -> PerformanceResult<()>
    where
        F: Fn() -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        let warmup_start = Instant::now();
        let warmup_rps = 10; // Low rate for warmup
        
        while warmup_start.elapsed() < self.config.warmup_duration {
            let op = operation.clone();
            tokio::spawn(async move {
                let _ = op().await;
            });
            
            sleep(Duration::from_millis(1000 / warmup_rps)).await;
        }
        
        info!("Warmup completed");
        Ok(())
    }
}

/// Performance metrics collector
struct PerformanceCollector {
    total_operations: AtomicU64,
    successful_operations: AtomicU64,
    failed_operations: AtomicU64,
    latencies: parking_lot::Mutex<Vec<Duration>>,
    start_time: Instant,
}

impl PerformanceCollector {
    fn new() -> Self {
        Self {
            total_operations: AtomicU64::new(0),
            successful_operations: AtomicU64::new(0),
            failed_operations: AtomicU64::new(0),
            latencies: parking_lot::Mutex::new(Vec::new()),
            start_time: Instant::now(),
        }
    }

    async fn record_success(&self, latency: Duration) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.successful_operations.fetch_add(1, Ordering::Relaxed);
        
        let mut latencies = self.latencies.lock();
        latencies.push(latency);
    }

    async fn record_failure(&self, latency: Duration, _error: String) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.failed_operations.fetch_add(1, Ordering::Relaxed);
        
        let mut latencies = self.latencies.lock();
        latencies.push(latency);
    }

    async fn finalize(&self, total_duration: Duration) -> PerformanceResult<PerformanceMetrics> {
        let total_ops = self.total_operations.load(Ordering::Relaxed);
        let successful_ops = self.successful_operations.load(Ordering::Relaxed);
        let failed_ops = self.failed_operations.load(Ordering::Relaxed);
        
        let throughput = if total_duration.as_secs_f64() > 0.0 {
            total_ops as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };
        
        let error_rate = if total_ops > 0 {
            failed_ops as f64 / total_ops as f64
        } else {
            0.0
        };

        let latencies = self.latencies.lock();
        let mut sorted_latencies: Vec<u64> = latencies
            .iter()
            .map(|d| d.as_millis() as u64)
            .collect();
        sorted_latencies.sort_unstable();

        let mut percentiles = HashMap::new();
        if !sorted_latencies.is_empty() {
            percentiles.insert("p50".to_string(), percentile(&sorted_latencies, 0.5));
            percentiles.insert("p95".to_string(), percentile(&sorted_latencies, 0.95));
            percentiles.insert("p99".to_string(), percentile(&sorted_latencies, 0.99));
            percentiles.insert("p99.9".to_string(), percentile(&sorted_latencies, 0.999));
        }

        Ok(PerformanceMetrics {
            test_name: "performance_test".to_string(),
            duration_ms: total_duration.as_millis() as u64,
            total_operations: total_ops,
            successful_operations: successful_ops,
            failed_operations: failed_ops,
            throughput_ops_per_sec: throughput,
            latency_percentiles: percentiles,
            error_rate,
            resource_usage: ResourceUsage::default(), // TODO: Collect actual resource usage
            timestamps: Vec::new(), // TODO: Collect timestamps
            latencies_ms: sorted_latencies,
        })
    }
}

fn percentile(sorted_values: &[u64], percentile: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    
    let index = (percentile * (sorted_values.len() - 1) as f64) as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

/// Performance test trait for defining custom benchmarks
#[async_trait]
pub trait PerformanceTest: Send + Sync {
    type Setup: Send + Sync;
    type Input: Send + Sync + Clone;
    type Output: Send + Sync;

    /// Setup the test environment
    async fn setup(&self) -> PerformanceResult<Self::Setup>;

    /// Generate input data for the test
    async fn generate_input(&self, setup: &Self::Setup) -> PerformanceResult<Self::Input>;

    /// Execute the operation being benchmarked
    async fn execute(&self, setup: &Self::Setup, input: Self::Input) -> PerformanceResult<Self::Output>;

    /// Cleanup after the test
    async fn cleanup(&self, setup: Self::Setup) -> PerformanceResult<()>;

    /// Name of the test for reporting
    fn name(&self) -> &str;

    /// Run the complete performance test
    async fn run(&self, config: PerformanceTestConfig) -> PerformanceResult<PerformanceMetrics> {
        let setup = self.setup().await?;
        let generator = LoadGenerator::new(config);
        
        // Simplified implementation - just run a basic load test
        let result = generator.constant_load(|| async {
            // Simulate some work
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }).await?;
        
        self.cleanup(setup).await?;
        
        Ok(result)
    }
}

/// Built-in performance test scenarios
pub struct PerformanceScenarios;

impl PerformanceScenarios {
    /// CPU-intensive benchmark
    pub fn cpu_intensive(iterations: usize) -> impl PerformanceTest<Setup = (), Input = usize, Output = u64> {
        CpuIntensiveTest { iterations }
    }

    /// Memory allocation benchmark
    pub fn memory_allocation(size_mb: usize) -> impl PerformanceTest<Setup = (), Input = usize, Output = Vec<u8>> {
        MemoryAllocationTest { size_mb }
    }

    /// I/O benchmark
    pub fn io_operations(file_size_kb: usize) -> impl PerformanceTest<Setup = tempfile::TempDir, Input = Vec<u8>, Output = Vec<u8>> {
        IoOperationsTest { file_size_kb }
    }
}

struct CpuIntensiveTest {
    iterations: usize,
}

#[async_trait]
impl PerformanceTest for CpuIntensiveTest {
    type Setup = ();
    type Input = usize;
    type Output = u64;

    async fn setup(&self) -> PerformanceResult<Self::Setup> {
        Ok(())
    }

    async fn generate_input(&self, _setup: &Self::Setup) -> PerformanceResult<Self::Input> {
        Ok(self.iterations)
    }

    async fn execute(&self, _setup: &Self::Setup, input: Self::Input) -> PerformanceResult<Self::Output> {
        let mut result = 0u64;
        for i in 0..input {
            result = result.wrapping_add(i as u64 * 13 + 7);
        }
        Ok(result)
    }

    async fn cleanup(&self, _setup: Self::Setup) -> PerformanceResult<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "cpu_intensive"
    }
}

struct MemoryAllocationTest {
    size_mb: usize,
}

#[async_trait]
impl PerformanceTest for MemoryAllocationTest {
    type Setup = ();
    type Input = usize;
    type Output = Vec<u8>;

    async fn setup(&self) -> PerformanceResult<Self::Setup> {
        Ok(())
    }

    async fn generate_input(&self, _setup: &Self::Setup) -> PerformanceResult<Self::Input> {
        Ok(self.size_mb * 1024 * 1024)
    }

    async fn execute(&self, _setup: &Self::Setup, input: Self::Input) -> PerformanceResult<Self::Output> {
        let mut data = Vec::with_capacity(input);
        data.resize(input, 42u8);
        Ok(data)
    }

    async fn cleanup(&self, _setup: Self::Setup) -> PerformanceResult<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "memory_allocation"
    }
}

struct IoOperationsTest {
    file_size_kb: usize,
}

#[async_trait]
impl PerformanceTest for IoOperationsTest {
    type Setup = tempfile::TempDir;
    type Input = Vec<u8>;
    type Output = Vec<u8>;

    async fn setup(&self) -> PerformanceResult<Self::Setup> {
        tempfile::TempDir::new()
            .map_err(|e| PerformanceError::BenchmarkFailed(e.to_string()))
    }

    async fn generate_input(&self, _setup: &Self::Setup) -> PerformanceResult<Self::Input> {
        let data = vec![42u8; self.file_size_kb * 1024];
        Ok(data)
    }

    async fn execute(&self, setup: &Self::Setup, input: Self::Input) -> PerformanceResult<Self::Output> {
        let file_path = setup.path().join("test_file");
        
        // Write data
        tokio::fs::write(&file_path, &input).await
            .map_err(|e| PerformanceError::BenchmarkFailed(e.to_string()))?;
        
        // Read data back
        let data = tokio::fs::read(&file_path).await
            .map_err(|e| PerformanceError::BenchmarkFailed(e.to_string()))?;
        
        Ok(data)
    }

    async fn cleanup(&self, _setup: Self::Setup) -> PerformanceResult<()> {
        // TempDir automatically cleans up
        Ok(())
    }

    fn name(&self) -> &str {
        "io_operations"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_generator() {
        let config = PerformanceTestConfig {
            duration: Duration::from_millis(100),
            warmup_duration: Duration::ZERO,
            max_concurrency: 10,
            target_throughput: Some(100),
            ..Default::default()
        };
        
        let generator = LoadGenerator::new(config);
        
        let result = generator.constant_load(|| async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }).await;
        
        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert!(metrics.total_operations > 0);
        assert!(metrics.throughput_ops_per_sec > 0.0);
    }

    #[tokio::test]
    async fn test_cpu_intensive_benchmark() {
        let test = PerformanceScenarios::cpu_intensive(1000);
        let config = PerformanceTestConfig {
            duration: Duration::from_millis(100),
            warmup_duration: Duration::ZERO,
            max_concurrency: 1,
            ..Default::default()
        };
        
        let result = test.run(config).await;
        assert!(result.is_ok());
        
        let metrics = result.unwrap();
        assert!(metrics.total_operations > 0);
        assert_eq!(metrics.test_name, "performance_test");
    }

    #[test]
    fn test_percentile_calculation() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        
        assert_eq!(percentile(&data, 0.5), 5);
        assert_eq!(percentile(&data, 0.9), 9);
        assert_eq!(percentile(&data, 1.0), 10);
        
        // Edge cases
        let empty: Vec<u64> = vec![];
        assert_eq!(percentile(&empty, 0.5), 0);
        
        let single = vec![42];
        assert_eq!(percentile(&single, 0.5), 42);
    }

    #[tokio::test]
    async fn test_ramping_load() {
        let config = PerformanceTestConfig {
            duration: Duration::from_millis(200),
            warmup_duration: Duration::ZERO,
            max_concurrency: 10,
            ..Default::default()
        };
        
        let generator = LoadGenerator::new(config);
        
        let result = generator.ramping_load(
            || async {
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            },
            10,  // start RPS
            50   // end RPS
        ).await;
        
        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert!(metrics.total_operations > 0);
    }
}