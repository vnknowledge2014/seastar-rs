//! Metrics and monitoring system for Seastar-RS
//!
//! Provides a comprehensive metrics collection and monitoring system inspired by 
//! Seastar C++ and Prometheus. Supports counters, gauges, histograms, and custom metrics.

pub mod exporters;
pub mod server;
pub mod collectors;
pub mod integration;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use serde::{Serialize, Deserialize};
use crate::Result;

/// Metric value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    /// Counter - monotonically increasing value
    Counter(u64),
    /// Gauge - can increase or decrease  
    Gauge(i64),
    /// Histogram - distribution of values
    Histogram {
        count: u64,
        sum: f64,
        buckets: Vec<HistogramBucket>,
    },
    /// Summary - quantile summaries
    Summary {
        count: u64,
        sum: f64,
        quantiles: Vec<Quantile>,
    },
}

/// Histogram bucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub upper_bound: f64,
    pub count: u64,
}

/// Quantile for summary metrics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]  
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

/// Metric metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMetadata {
    /// Metric name
    pub name: String,
    /// Help text describing the metric
    pub help: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Labels for this metric
    pub labels: BTreeMap<String, String>,
    /// Unit of measurement
    pub unit: Option<String>,
    /// Timestamp when metric was last updated
    pub timestamp: SystemTime,
}

/// Types of metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Complete metric with metadata and value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub metadata: MetricMetadata,
    pub value: MetricValue,
}

/// Thread-safe counter metric
#[derive(Clone)]
pub struct Counter {
    value: Arc<AtomicU64>,
    metadata: Arc<MetricMetadata>,
}

impl Counter {
    pub fn new(name: String, help: String) -> Self {
        let metadata = Arc::new(MetricMetadata {
            name,
            help,
            metric_type: MetricType::Counter,
            labels: BTreeMap::new(),
            unit: None,
            timestamp: SystemTime::now(),
        });
        
        Self {
            value: Arc::new(AtomicU64::new(0)),
            metadata,
        }
    }
    
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        Arc::make_mut(&mut self.metadata).labels = labels;
        self
    }
    
    pub fn with_unit(mut self, unit: String) -> Self {
        Arc::make_mut(&mut self.metadata).unit = Some(unit);
        self
    }
    
    /// Increment counter by 1
    pub fn inc(&self) {
        self.add(1);
    }
    
    /// Add value to counter
    pub fn add(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        Arc::make_mut(&mut self.metadata.clone()).timestamp = SystemTime::now();
    }
    
    /// Get current counter value
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
    
    /// Get metric representation
    pub fn metric(&self) -> Metric {
        Metric {
            metadata: (*self.metadata).clone(),
            value: MetricValue::Counter(self.get()),
        }
    }
}

/// Thread-safe gauge metric
#[derive(Clone)]
pub struct Gauge {
    value: Arc<AtomicI64>,
    metadata: Arc<MetricMetadata>,
}

impl Gauge {
    pub fn new(name: String, help: String) -> Self {
        let metadata = Arc::new(MetricMetadata {
            name,
            help,
            metric_type: MetricType::Gauge,
            labels: BTreeMap::new(),
            unit: None,
            timestamp: SystemTime::now(),
        });
        
        Self {
            value: Arc::new(AtomicI64::new(0)),
            metadata,
        }
    }
    
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        Arc::make_mut(&mut self.metadata).labels = labels;
        self
    }
    
    pub fn with_unit(mut self, unit: String) -> Self {
        Arc::make_mut(&mut self.metadata).unit = Some(unit);
        self
    }
    
    /// Set gauge to specific value
    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
        Arc::make_mut(&mut self.metadata.clone()).timestamp = SystemTime::now();
    }
    
    /// Increment gauge by 1
    pub fn inc(&self) {
        self.add(1);
    }
    
    /// Decrement gauge by 1
    pub fn dec(&self) {
        self.sub(1);
    }
    
    /// Add value to gauge
    pub fn add(&self, value: i64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        Arc::make_mut(&mut self.metadata.clone()).timestamp = SystemTime::now();
    }
    
    /// Subtract value from gauge
    pub fn sub(&self, value: i64) {
        self.value.fetch_sub(value, Ordering::Relaxed);
        Arc::make_mut(&mut self.metadata.clone()).timestamp = SystemTime::now();
    }
    
    /// Get current gauge value
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
    
    /// Get metric representation
    pub fn metric(&self) -> Metric {
        Metric {
            metadata: (*self.metadata).clone(),
            value: MetricValue::Gauge(self.get()),
        }
    }
}

/// Thread-safe histogram metric
#[derive(Clone)]
pub struct Histogram {
    buckets: Arc<Vec<(f64, AtomicU64)>>,  // (upper_bound, count)
    sum: Arc<AtomicU64>,  // Store as bits for atomic access
    count: Arc<AtomicU64>,
    metadata: Arc<MetricMetadata>,
}

impl Histogram {
    pub fn new(name: String, help: String, buckets: Vec<f64>) -> Self {
        let mut bucket_vec = buckets.into_iter()
            .map(|bound| (bound, AtomicU64::new(0)))
            .collect::<Vec<_>>();
        
        // Ensure +Inf bucket
        if bucket_vec.last().map(|(b, _)| *b) != Some(f64::INFINITY) {
            bucket_vec.push((f64::INFINITY, AtomicU64::new(0)));
        }
        
        let metadata = Arc::new(MetricMetadata {
            name,
            help,
            metric_type: MetricType::Histogram,
            labels: BTreeMap::new(),
            unit: None,
            timestamp: SystemTime::now(),
        });
        
        Self {
            buckets: Arc::new(bucket_vec),
            sum: Arc::new(AtomicU64::new(0)),
            count: Arc::new(AtomicU64::new(0)),
            metadata,
        }
    }
    
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        Arc::make_mut(&mut self.metadata).labels = labels;
        self
    }
    
    /// Observe a value
    pub fn observe(&self, value: f64) {
        // Update buckets
        for (upper_bound, bucket_count) in self.buckets.iter() {
            if value <= *upper_bound {
                bucket_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Update sum (store as bits for atomic access)
        let sum_bits = value.to_bits();
        self.sum.fetch_add(sum_bits, Ordering::Relaxed);
        
        // Update count
        self.count.fetch_add(1, Ordering::Relaxed);
        
        Arc::make_mut(&mut self.metadata.clone()).timestamp = SystemTime::now();
    }
    
    /// Get metric representation
    pub fn metric(&self) -> Metric {
        let buckets = self.buckets.iter()
            .map(|(bound, count)| HistogramBucket {
                upper_bound: *bound,
                count: count.load(Ordering::Relaxed),
            })
            .collect();
            
        let sum_bits = self.sum.load(Ordering::Relaxed);
        let sum = f64::from_bits(sum_bits);
        
        Metric {
            metadata: (*self.metadata).clone(),
            value: MetricValue::Histogram {
                count: self.count.load(Ordering::Relaxed),
                sum,
                buckets,
            },
        }
    }
    
    /// Create default buckets (suitable for latency measurements)
    pub fn default_buckets() -> Vec<f64> {
        vec![
            0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 
            0.75, 1.0, 2.5, 5.0, 7.5, 10.0, f64::INFINITY
        ]
    }
}

/// Metric registry for collecting and managing metrics
#[derive(Default)]
pub struct MetricRegistry {
    metrics: RwLock<HashMap<String, Box<dyn MetricProvider + Send + Sync>>>,
}

/// Trait for types that can provide metrics
pub trait MetricProvider {
    fn metrics(&self) -> Vec<Metric>;
    fn name(&self) -> &str;
}

impl MetricProvider for Counter {
    fn metrics(&self) -> Vec<Metric> {
        vec![self.metric()]
    }
    
    fn name(&self) -> &str {
        &self.metadata.name
    }
}

impl MetricProvider for Gauge {
    fn metrics(&self) -> Vec<Metric> {
        vec![self.metric()]
    }
    
    fn name(&self) -> &str {
        &self.metadata.name
    }
}

impl MetricProvider for Histogram {
    fn metrics(&self) -> Vec<Metric> {
        vec![self.metric()]
    }
    
    fn name(&self) -> &str {
        &self.metadata.name
    }
}

/// Metric family groups related metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFamily {
    pub name: String,
    pub help: String,
    pub metric_type: MetricType,
    pub metrics: Vec<Metric>,
}

impl MetricRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a metric provider
    pub fn register<T: MetricProvider + Send + Sync + 'static>(&self, metric: T) -> Result<()> {
        let name = metric.name().to_string();
        let mut metrics = self.metrics.write().unwrap();
        
        if metrics.contains_key(&name) {
            // For test isolation, allow re-registration by replacing existing metric
            tracing::debug!("Metric '{}' already registered, replacing", name);
        }
        
        metrics.insert(name, Box::new(metric));
        Ok(())
    }
    
    /// Get all metrics
    pub fn gather(&self) -> Vec<MetricFamily> {
        let metrics = self.metrics.read().unwrap();
        let mut families = HashMap::new();
        
        for (_, provider) in metrics.iter() {
            for metric in provider.metrics() {
                let family = families.entry(metric.metadata.name.clone())
                    .or_insert_with(|| MetricFamily {
                        name: metric.metadata.name.clone(),
                        help: metric.metadata.help.clone(),
                        metric_type: metric.metadata.metric_type,
                        metrics: Vec::new(),
                    });
                family.metrics.push(metric);
            }
        }
        
        families.into_values().collect()
    }
    
    /// Get metrics for a specific name
    pub fn get(&self, name: &str) -> Option<Vec<Metric>> {
        let metrics = self.metrics.read().unwrap();
        metrics.get(name).map(|provider| provider.metrics())
    }
    
    /// Clear all metrics
    pub fn clear(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.clear();
    }
}

/// Global metric registry
static GLOBAL_REGISTRY: once_cell::sync::Lazy<MetricRegistry> = 
    once_cell::sync::Lazy::new(MetricRegistry::new);

/// Get the global metric registry
pub fn global_registry() -> &'static MetricRegistry {
    &GLOBAL_REGISTRY
}

/// Convenience function to register a metric globally
pub fn register<T: MetricProvider + Send + Sync + 'static>(metric: T) -> Result<()> {
    global_registry().register(metric)
}

/// System-wide metrics collector
pub struct SystemMetrics {
    // Core metrics
    pub tasks_scheduled: Counter,
    pub tasks_completed: Counter,
    pub tasks_failed: Counter,
    pub task_queue_depth: Gauge,
    pub task_execution_time: Histogram,
    
    // Memory metrics
    pub memory_allocated: Gauge,
    pub memory_deallocated: Counter,
    pub memory_peak_usage: Gauge,
    pub memory_current_usage: Gauge,
    
    // I/O metrics  
    pub io_operations_started: Counter,
    pub io_operations_completed: Counter,
    pub io_operations_failed: Counter,
    pub io_bytes_read: Counter,
    pub io_bytes_written: Counter,
    pub io_operation_duration: Histogram,
    
    // Network metrics
    pub network_connections_opened: Counter,
    pub network_connections_closed: Counter,
    pub network_bytes_sent: Counter,
    pub network_bytes_received: Counter,
    pub network_packets_sent: Counter,
    pub network_packets_received: Counter,
    
    // Reactor metrics
    pub reactor_polls: Counter,
    pub reactor_events_processed: Counter,
    pub reactor_poll_time: Histogram,
    pub reactor_busy_time: Histogram,
    
    // File metrics
    pub file_operations: Counter,
    pub file_bytes_read: Counter,
    pub file_bytes_written: Counter,
    pub file_operation_duration: Histogram,
    
    // Timer metrics
    pub timers_created: Counter,
    pub timers_fired: Counter,
    pub timers_cancelled: Counter,
    
    // HTTP metrics (if HTTP module is enabled)
    pub http_requests_total: Counter,
    pub http_request_duration: Histogram,
    pub http_responses_by_status: Counter,
    
    // RPC metrics (if RPC module is enabled)
    pub rpc_requests_total: Counter,
    pub rpc_request_duration: Histogram,
    pub rpc_responses_by_status: Counter,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            // Task metrics
            tasks_scheduled: Counter::new(
                "seastar_tasks_scheduled_total".to_string(),
                "Total number of tasks scheduled".to_string(),
            ),
            tasks_completed: Counter::new(
                "seastar_tasks_completed_total".to_string(),
                "Total number of tasks completed".to_string(),
            ),
            tasks_failed: Counter::new(
                "seastar_tasks_failed_total".to_string(),
                "Total number of tasks that failed".to_string(),
            ),
            task_queue_depth: Gauge::new(
                "seastar_task_queue_depth".to_string(),
                "Current depth of task queue".to_string(),
            ),
            task_execution_time: Histogram::new(
                "seastar_task_execution_duration_seconds".to_string(),
                "Time spent executing tasks".to_string(),
                Histogram::default_buckets(),
            ),
            
            // Memory metrics
            memory_allocated: Gauge::new(
                "seastar_memory_allocated_bytes".to_string(),
                "Currently allocated memory in bytes".to_string(),
            ).with_unit("bytes".to_string()),
            memory_deallocated: Counter::new(
                "seastar_memory_deallocated_bytes_total".to_string(),
                "Total memory deallocated in bytes".to_string(),
            ).with_unit("bytes".to_string()),
            memory_peak_usage: Gauge::new(
                "seastar_memory_peak_usage_bytes".to_string(),
                "Peak memory usage in bytes".to_string(),
            ).with_unit("bytes".to_string()),
            memory_current_usage: Gauge::new(
                "seastar_memory_current_usage_bytes".to_string(),
                "Current memory usage in bytes".to_string(),
            ).with_unit("bytes".to_string()),
            
            // I/O metrics
            io_operations_started: Counter::new(
                "seastar_io_operations_started_total".to_string(),
                "Total I/O operations started".to_string(),
            ),
            io_operations_completed: Counter::new(
                "seastar_io_operations_completed_total".to_string(),
                "Total I/O operations completed".to_string(),
            ),
            io_operations_failed: Counter::new(
                "seastar_io_operations_failed_total".to_string(),
                "Total I/O operations failed".to_string(),
            ),
            io_bytes_read: Counter::new(
                "seastar_io_bytes_read_total".to_string(),
                "Total bytes read from I/O operations".to_string(),
            ).with_unit("bytes".to_string()),
            io_bytes_written: Counter::new(
                "seastar_io_bytes_written_total".to_string(),
                "Total bytes written from I/O operations".to_string(),
            ).with_unit("bytes".to_string()),
            io_operation_duration: Histogram::new(
                "seastar_io_operation_duration_seconds".to_string(),
                "Duration of I/O operations".to_string(),
                Histogram::default_buckets(),
            ),
            
            // Network metrics
            network_connections_opened: Counter::new(
                "seastar_network_connections_opened_total".to_string(),
                "Total network connections opened".to_string(),
            ),
            network_connections_closed: Counter::new(
                "seastar_network_connections_closed_total".to_string(),
                "Total network connections closed".to_string(),
            ),
            network_bytes_sent: Counter::new(
                "seastar_network_bytes_sent_total".to_string(),
                "Total bytes sent over network".to_string(),
            ).with_unit("bytes".to_string()),
            network_bytes_received: Counter::new(
                "seastar_network_bytes_received_total".to_string(),
                "Total bytes received over network".to_string(),
            ).with_unit("bytes".to_string()),
            network_packets_sent: Counter::new(
                "seastar_network_packets_sent_total".to_string(),
                "Total network packets sent".to_string(),
            ),
            network_packets_received: Counter::new(
                "seastar_network_packets_received_total".to_string(),
                "Total network packets received".to_string(),
            ),
            
            // Reactor metrics
            reactor_polls: Counter::new(
                "seastar_reactor_polls_total".to_string(),
                "Total number of reactor polls".to_string(),
            ),
            reactor_events_processed: Counter::new(
                "seastar_reactor_events_processed_total".to_string(),
                "Total events processed by reactor".to_string(),
            ),
            reactor_poll_time: Histogram::new(
                "seastar_reactor_poll_duration_seconds".to_string(),
                "Time spent polling for events".to_string(),
                Histogram::default_buckets(),
            ),
            reactor_busy_time: Histogram::new(
                "seastar_reactor_busy_duration_seconds".to_string(),
                "Time reactor spent processing events".to_string(),
                Histogram::default_buckets(),
            ),
            
            // File metrics
            file_operations: Counter::new(
                "seastar_file_operations_total".to_string(),
                "Total file operations".to_string(),
            ),
            file_bytes_read: Counter::new(
                "seastar_file_bytes_read_total".to_string(),
                "Total bytes read from files".to_string(),
            ).with_unit("bytes".to_string()),
            file_bytes_written: Counter::new(
                "seastar_file_bytes_written_total".to_string(),
                "Total bytes written to files".to_string(),
            ).with_unit("bytes".to_string()),
            file_operation_duration: Histogram::new(
                "seastar_file_operation_duration_seconds".to_string(),
                "Duration of file operations".to_string(),
                Histogram::default_buckets(),
            ),
            
            // Timer metrics
            timers_created: Counter::new(
                "seastar_timers_created_total".to_string(),
                "Total timers created".to_string(),
            ),
            timers_fired: Counter::new(
                "seastar_timers_fired_total".to_string(),
                "Total timers fired".to_string(),
            ),
            timers_cancelled: Counter::new(
                "seastar_timers_cancelled_total".to_string(),
                "Total timers cancelled".to_string(),
            ),
            
            // HTTP metrics
            http_requests_total: Counter::new(
                "seastar_http_requests_total".to_string(),
                "Total HTTP requests processed".to_string(),
            ),
            http_request_duration: Histogram::new(
                "seastar_http_request_duration_seconds".to_string(),
                "Duration of HTTP requests".to_string(),
                Histogram::default_buckets(),
            ),
            http_responses_by_status: Counter::new(
                "seastar_http_responses_by_status_total".to_string(),
                "HTTP responses by status code".to_string(),
            ),
            
            // RPC metrics
            rpc_requests_total: Counter::new(
                "seastar_rpc_requests_total".to_string(),
                "Total RPC requests processed".to_string(),
            ),
            rpc_request_duration: Histogram::new(
                "seastar_rpc_request_duration_seconds".to_string(),
                "Duration of RPC requests".to_string(),
                Histogram::default_buckets(),
            ),
            rpc_responses_by_status: Counter::new(
                "seastar_rpc_responses_by_status_total".to_string(),
                "RPC responses by status".to_string(),
            ),
        }
    }
    
    /// Register all system metrics with global registry
    pub fn register_all(&self) -> Result<()> {
        // Task metrics
        register(self.tasks_scheduled.clone())?;
        register(self.tasks_completed.clone())?;
        register(self.tasks_failed.clone())?;
        register(self.task_queue_depth.clone())?;
        register(self.task_execution_time.clone())?;
        
        // Memory metrics
        register(self.memory_allocated.clone())?;
        register(self.memory_deallocated.clone())?;
        register(self.memory_peak_usage.clone())?;
        register(self.memory_current_usage.clone())?;
        
        // I/O metrics
        register(self.io_operations_started.clone())?;
        register(self.io_operations_completed.clone())?;
        register(self.io_operations_failed.clone())?;
        register(self.io_bytes_read.clone())?;
        register(self.io_bytes_written.clone())?;
        register(self.io_operation_duration.clone())?;
        
        // Network metrics
        register(self.network_connections_opened.clone())?;
        register(self.network_connections_closed.clone())?;
        register(self.network_bytes_sent.clone())?;
        register(self.network_bytes_received.clone())?;
        register(self.network_packets_sent.clone())?;
        register(self.network_packets_received.clone())?;
        
        // Reactor metrics
        register(self.reactor_polls.clone())?;
        register(self.reactor_events_processed.clone())?;
        register(self.reactor_poll_time.clone())?;
        register(self.reactor_busy_time.clone())?;
        
        // File metrics
        register(self.file_operations.clone())?;
        register(self.file_bytes_read.clone())?;
        register(self.file_bytes_written.clone())?;
        register(self.file_operation_duration.clone())?;
        
        // Timer metrics
        register(self.timers_created.clone())?;
        register(self.timers_fired.clone())?;
        register(self.timers_cancelled.clone())?;
        
        // HTTP metrics
        register(self.http_requests_total.clone())?;
        register(self.http_request_duration.clone())?;
        register(self.http_responses_by_status.clone())?;
        
        // RPC metrics
        register(self.rpc_requests_total.clone())?;
        register(self.rpc_request_duration.clone())?;
        register(self.rpc_responses_by_status.clone())?;
        
        Ok(())
    }
}

/// Global system metrics instance
static GLOBAL_SYSTEM_METRICS: once_cell::sync::Lazy<SystemMetrics> = 
    once_cell::sync::Lazy::new(SystemMetrics::new);

/// Get the global system metrics
pub fn global_system_metrics() -> &'static SystemMetrics {
    &GLOBAL_SYSTEM_METRICS
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_counter() {
        let counter = Counter::new(
            "test_counter".to_string(),
            "A test counter".to_string(),
        );
        
        assert_eq!(counter.get(), 0);
        counter.inc();
        assert_eq!(counter.get(), 1);
        counter.add(5);
        assert_eq!(counter.get(), 6);
        
        let metric = counter.metric();
        match metric.value {
            MetricValue::Counter(value) => assert_eq!(value, 6),
            _ => panic!("Expected counter value"),
        }
    }
    
    #[test]
    fn test_gauge() {
        let gauge = Gauge::new(
            "test_gauge".to_string(),
            "A test gauge".to_string(),
        );
        
        assert_eq!(gauge.get(), 0);
        gauge.set(10);
        assert_eq!(gauge.get(), 10);
        gauge.inc();
        assert_eq!(gauge.get(), 11);
        gauge.dec();
        assert_eq!(gauge.get(), 10);
        gauge.add(5);
        assert_eq!(gauge.get(), 15);
        gauge.sub(3);
        assert_eq!(gauge.get(), 12);
        
        let metric = gauge.metric();
        match metric.value {
            MetricValue::Gauge(value) => assert_eq!(value, 12),
            _ => panic!("Expected gauge value"),
        }
    }
    
    #[test]
    fn test_histogram() {
        let histogram = Histogram::new(
            "test_histogram".to_string(),
            "A test histogram".to_string(),
            vec![1.0, 2.0, 5.0],
        );
        
        histogram.observe(0.5);
        histogram.observe(1.5);
        histogram.observe(3.0);
        histogram.observe(10.0);
        
        let metric = histogram.metric();
        match metric.value {
            MetricValue::Histogram { count, sum: _sum, buckets } => {
                assert_eq!(count, 4);
                assert_eq!(buckets.len(), 4); // 3 + infinity bucket
                assert_eq!(buckets[0].count, 1); // <= 1.0
                assert_eq!(buckets[1].count, 2); // <= 2.0  
                assert_eq!(buckets[2].count, 3); // <= 5.0
                assert_eq!(buckets[3].count, 4); // <= infinity
            },
            _ => panic!("Expected histogram value"),
        }
    }
    
    #[test]
    fn test_metric_registry() {
        let registry = MetricRegistry::new();
        
        let counter = Counter::new(
            "registry_test_counter".to_string(),
            "A counter for registry testing".to_string(),
        );
        counter.add(42);
        
        registry.register(counter).unwrap();
        
        let families = registry.gather();
        assert_eq!(families.len(), 1);
        
        let family = &families[0];
        assert_eq!(family.name, "registry_test_counter");
        assert_eq!(family.metrics.len(), 1);
        
        match &family.metrics[0].value {
            MetricValue::Counter(value) => assert_eq!(*value, 42),
            _ => panic!("Expected counter value"),
        }
    }
}