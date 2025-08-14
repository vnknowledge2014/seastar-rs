//! # Seastar-RS Advanced Testing Framework
//! 
//! Comprehensive testing utilities for Seastar-RS applications including:
//! - Chaos engineering and fault injection
//! - Property-based testing
//! - Performance testing and benchmarking  
//! - Integration test utilities
//! - Test orchestration and reporting

pub mod test_runner;
pub mod benchmarks;
pub mod fixtures;
pub mod chaos;
pub mod property;
pub mod performance;
pub mod integration;
pub mod orchestration;
pub mod reporting;

pub use test_runner::TestRunner;
pub use benchmarks::BenchmarkSuite;
pub use fixtures::TestFixture;
pub use chaos::{ChaosEngine, FaultInjection};
pub use property::{PropertyTest, PropertyGenerator};
pub use performance::{PerformanceTest, LoadGenerator};
pub use integration::{IntegrationTest, TestEnvironment};
pub use orchestration::{TestOrchestrator, TestPlan};
pub use reporting::{TestReporter, TestResults};