//! Chaos engineering and fault injection utilities

use async_trait::async_trait;
use futures::Future;
use rand::{thread_rng, Rng};
use seastar_config::ChaosConfig;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum ChaosError {
    #[error("Fault injection error: {0}")]
    Injection(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Timeout error: {0}")]
    Timeout(String),
}

pub type ChaosResult<T> = Result<T, ChaosError>;

/// Fault injection types
#[derive(Debug, Clone)]
pub enum FaultType {
    /// Introduce latency delays
    Latency(Duration),
    /// Cause operation failures
    Failure { rate: f64, error_msg: String },
    /// Throttle throughput
    Throttle { max_ops_per_sec: u64 },
    /// Memory pressure simulation
    MemoryPressure { target_mb: usize },
    /// CPU pressure simulation  
    CpuPressure { target_percent: f64 },
    /// Network partition simulation
    NetworkPartition { duration: Duration },
    /// Disk I/O errors
    DiskErrors { rate: f64 },
}

/// Fault injection configuration
#[derive(Debug, Clone)]
pub struct FaultInjection {
    pub fault_type: FaultType,
    pub enabled: bool,
    pub target_operations: Vec<String>,
    pub start_time: Option<Instant>,
    pub duration: Option<Duration>,
}

impl FaultInjection {
    pub fn new(fault_type: FaultType) -> Self {
        Self {
            fault_type,
            enabled: true,
            target_operations: vec![],
            start_time: None,
            duration: None,
        }
    }

    pub fn with_targets(mut self, operations: Vec<String>) -> Self {
        self.target_operations = operations;
        self
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = Some(duration);
        self
    }

    pub fn enable(&mut self) {
        self.enabled = true;
        self.start_time = Some(Instant::now());
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

    pub fn is_active(&self) -> bool {
        if !self.enabled {
            return false;
        }

        if let (Some(start), Some(duration)) = (self.start_time, self.duration) {
            start.elapsed() < duration
        } else {
            true
        }
    }
}

/// Chaos engine for orchestrating fault injection
pub struct ChaosEngine {
    config: ChaosConfig,
    faults: Vec<FaultInjection>,
    stats: ChaosStats,
}

#[derive(Debug, Default)]
pub struct ChaosStats {
    pub total_faults_injected: AtomicU64,
    pub latency_faults: AtomicU64,
    pub failure_faults: AtomicU64,
    pub throttle_faults: AtomicU64,
    pub memory_pressure_events: AtomicU64,
    pub cpu_pressure_events: AtomicU64,
}

impl ChaosEngine {
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            config,
            faults: Vec::new(),
            stats: ChaosStats::default(),
        }
    }

    pub fn add_fault(&mut self, fault: FaultInjection) {
        info!(
            "Adding chaos fault: {:?} for operations: {:?}",
            fault.fault_type, fault.target_operations
        );
        self.faults.push(fault);
    }

    pub fn remove_fault(&mut self, index: usize) -> Option<FaultInjection> {
        if index < self.faults.len() {
            Some(self.faults.remove(index))
        } else {
            None
        }
    }

    pub fn enable_all_faults(&mut self) {
        for fault in &mut self.faults {
            fault.enable();
        }
        info!("Enabled all chaos faults");
    }

    pub fn disable_all_faults(&mut self) {
        for fault in &mut self.faults {
            fault.disable();
        }
        info!("Disabled all chaos faults");
    }

    pub fn stats(&self) -> &ChaosStats {
        &self.stats
    }

    /// Apply chaos to an operation
    pub async fn apply_chaos<T, Fut>(&self, operation_name: &str, future: Fut) -> ChaosResult<T>
    where
        Fut: Future<Output = T> + Send,
        T: Send,
    {
        // Check if any faults apply to this operation
        let applicable_faults: Vec<_> = self
            .faults
            .iter()
            .filter(|f| {
                f.is_active() && 
                (f.target_operations.is_empty() || f.target_operations.contains(&operation_name.to_string()))
            })
            .collect();

        if applicable_faults.is_empty() {
            return Ok(future.await);
        }

        // Apply faults sequentially 
        for fault in &applicable_faults {
            self.apply_fault_effects(fault).await?;
        }

        // Execute the original operation
        Ok(future.await)
    }

    async fn apply_fault_effects(&self, fault: &FaultInjection) -> ChaosResult<()> {
        match &fault.fault_type {
            FaultType::Latency(duration) => {
                self.stats.latency_faults.fetch_add(1, Ordering::Relaxed);
                debug!("Injecting latency: {:?}", duration);
                sleep(*duration).await;
                Ok(())
            }
            FaultType::Failure { rate, error_msg } => {
                let should_fail = thread_rng().gen::<f64>() < *rate;
                if should_fail {
                    self.stats.failure_faults.fetch_add(1, Ordering::Relaxed);
                    warn!("Injecting failure: {}", error_msg);
                    return Err(ChaosError::Injection(error_msg.clone()));
                }
                Ok(())
            }
            FaultType::Throttle { max_ops_per_sec } => {
                self.stats.throttle_faults.fetch_add(1, Ordering::Relaxed);
                let delay = Duration::from_millis(1000 / max_ops_per_sec);
                debug!("Injecting throttle delay: {:?}", delay);
                sleep(delay).await;
                Ok(())
            }
            FaultType::MemoryPressure { target_mb } => {
                self.stats.memory_pressure_events.fetch_add(1, Ordering::Relaxed);
                debug!("Simulating memory pressure: {} MB", target_mb);
                // Allocate memory to simulate pressure
                let _memory_ballast: Vec<u8> = vec![0u8; target_mb * 1024 * 1024];
                // Memory is released when _memory_ballast goes out of scope
                Ok(())
            }
            FaultType::CpuPressure { target_percent } => {
                self.stats.cpu_pressure_events.fetch_add(1, Ordering::Relaxed);
                debug!("Simulating CPU pressure: {}%", target_percent);
                let cpu_work_duration = Duration::from_millis((*target_percent * 10.0) as u64);
                
                // Simulate CPU work
                let start = Instant::now();
                while start.elapsed() < cpu_work_duration {
                    // Busy work
                    for _ in 0..1000 {
                        std::hint::black_box(thread_rng().gen::<u64>());
                    }
                }
                Ok(())
            }
            FaultType::NetworkPartition { duration: _ } => {
                debug!("Simulating network partition");
                // Network partitions are handled by caller timeout
                Ok(())
            }
            FaultType::DiskErrors { rate } => {
                let should_fail = thread_rng().gen::<f64>() < *rate;
                if should_fail {
                    warn!("Injecting disk I/O error");
                    return Err(ChaosError::Injection("Simulated disk I/O error".to_string()));
                }
                Ok(())
            }
        }
    }
}

/// Trait for chaos-aware operations
#[async_trait]
pub trait ChaosAware {
    type Output;
    type Error;

    async fn execute_with_chaos(
        &self,
        chaos_engine: &ChaosEngine,
        operation_name: &str,
    ) -> Result<Self::Output, Self::Error>;
}

/// Macro for creating chaos-aware operations
#[macro_export]
macro_rules! chaos_aware {
    ($operation_name:expr, $chaos_engine:expr, $future:expr) => {{
        $chaos_engine
            .apply_chaos($operation_name, $future)
            .await
            .map_err(|e| format!("Chaos error in {}: {}", $operation_name, e))
    }};
}

/// Predefined chaos scenarios
pub struct ChaosScenarios;

impl ChaosScenarios {
    /// Network instability scenario
    pub fn network_instability() -> Vec<FaultInjection> {
        vec![
            FaultInjection::new(FaultType::Latency(Duration::from_millis(100)))
                .with_targets(vec!["http_request".to_string(), "database_query".to_string()])
                .with_duration(Duration::from_secs(30)),
            FaultInjection::new(FaultType::Failure {
                rate: 0.05,
                error_msg: "Network timeout".to_string(),
            })
            .with_targets(vec!["http_request".to_string()])
            .with_duration(Duration::from_secs(60)),
        ]
    }

    /// Database instability scenario
    pub fn database_instability() -> Vec<FaultInjection> {
        vec![
            FaultInjection::new(FaultType::Latency(Duration::from_millis(500)))
                .with_targets(vec!["database_query".to_string(), "database_transaction".to_string()])
                .with_duration(Duration::from_secs(45)),
            FaultInjection::new(FaultType::Failure {
                rate: 0.02,
                error_msg: "Database connection lost".to_string(),
            })
            .with_targets(vec!["database_query".to_string()])
            .with_duration(Duration::from_secs(30)),
        ]
    }

    /// Resource pressure scenario
    pub fn resource_pressure() -> Vec<FaultInjection> {
        vec![
            FaultInjection::new(FaultType::MemoryPressure { target_mb: 100 })
                .with_duration(Duration::from_secs(120)),
            FaultInjection::new(FaultType::CpuPressure { target_percent: 80.0 })
                .with_duration(Duration::from_secs(60)),
            FaultInjection::new(FaultType::Throttle { max_ops_per_sec: 10 })
                .with_targets(vec!["disk_write".to_string(), "disk_read".to_string()])
                .with_duration(Duration::from_secs(90)),
        ]
    }

    /// Complete system chaos scenario
    pub fn complete_chaos() -> Vec<FaultInjection> {
        let mut faults = Vec::new();
        faults.extend(Self::network_instability());
        faults.extend(Self::database_instability());
        faults.extend(Self::resource_pressure());
        
        // Add disk errors
        faults.push(
            FaultInjection::new(FaultType::DiskErrors { rate: 0.01 })
                .with_targets(vec!["file_read".to_string(), "file_write".to_string()])
                .with_duration(Duration::from_secs(180))
        );
        
        faults
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seastar_config::ChaosConfig;

    fn test_config() -> ChaosConfig {
        ChaosConfig {
            failure_rate: 0.1,
            latency_injection_ms: 100,
            enable_memory_pressure: true,
            enable_cpu_pressure: true,
        }
    }

    #[tokio::test]
    async fn test_latency_injection() {
        let config = test_config();
        let mut engine = ChaosEngine::new(config);
        
        let fault = FaultInjection::new(FaultType::Latency(Duration::from_millis(50)))
            .with_targets(vec!["test_op".to_string()]);
        
        engine.add_fault(fault);

        let start = Instant::now();
        let result = engine.apply_chaos("test_op", async { "success" }).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert!(elapsed >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_failure_injection() {
        let config = test_config();
        let mut engine = ChaosEngine::new(config);
        
        let fault = FaultInjection::new(FaultType::Failure {
            rate: 1.0, // Always fail
            error_msg: "Test failure".to_string(),
        })
        .with_targets(vec!["test_op".to_string()]);
        
        engine.add_fault(fault);

        let result = engine.apply_chaos("test_op", async { "success" }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_chaos_scenarios() {
        let faults = ChaosScenarios::network_instability();
        assert!(!faults.is_empty());
        
        let faults = ChaosScenarios::complete_chaos();
        assert!(faults.len() > 5); // Should have multiple types of faults
    }

    #[tokio::test]
    async fn test_fault_duration() {
        let config = test_config();
        let mut engine = ChaosEngine::new(config);
        
        let mut fault = FaultInjection::new(FaultType::Latency(Duration::from_millis(10)))
            .with_duration(Duration::from_millis(50));
        
        fault.enable();
        assert!(fault.is_active());
        
        // Wait for fault to expire
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!fault.is_active());
    }
}