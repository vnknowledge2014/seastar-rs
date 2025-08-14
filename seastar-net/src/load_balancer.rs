//! Load balancing for distributing connections across backends

use crate::connection_pool::{ConnectionPool, PooledConnection, PoolError};
use crate::tcp::TcpConfig;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum LoadBalancerError {
    #[error("Pool error: {0}")]
    Pool(#[from] PoolError),
    #[error("No healthy backends available")]
    NoHealthyBackends,
    #[error("Backend not found: {0}")]
    BackendNotFound(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

pub type LoadBalancerResult<T> = Result<T, LoadBalancerError>;

/// Load balancing strategies
#[derive(Debug, Clone, PartialEq)]
pub enum LoadBalanceStrategy {
    /// Round-robin distribution
    RoundRobin,
    /// Least connections first
    LeastConnections,
    /// Weighted round-robin
    WeightedRoundRobin,
    /// IP hash-based
    IpHash,
    /// Random selection
    Random,
    /// Response time-based
    ResponseTime,
}

/// Backend server configuration
#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub addr: SocketAddr,
    pub weight: u32,
    pub max_connections: usize,
    pub health_check_interval: Duration,
    pub failure_threshold: u32,
    pub recovery_threshold: u32,
}

impl BackendConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            weight: 1,
            max_connections: 100,
            health_check_interval: Duration::from_secs(30),
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }

    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }
}

/// Backend server state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackendState {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Backend server statistics
#[derive(Debug, Clone)]
pub struct BackendStats {
    pub addr: SocketAddr,
    pub state: BackendState,
    pub active_connections: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time: Duration,
    pub last_health_check: Option<Instant>,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
}

/// Backend server instance
struct Backend {
    config: BackendConfig,
    pool: Arc<ConnectionPool>,
    state: BackendState,
    stats: BackendStats,
    last_used: Instant,
    response_times: Vec<Duration>, // Ring buffer for response times
}

impl Backend {
    async fn new(config: BackendConfig, tcp_config: TcpConfig) -> LoadBalancerResult<Self> {
        let pool = Arc::new(
            ConnectionPool::new(config.addr, config.max_connections, tcp_config).await?
        );

        Ok(Self {
            stats: BackendStats {
                addr: config.addr,
                state: BackendState::Healthy,
                active_connections: 0,
                total_requests: 0,
                failed_requests: 0,
                avg_response_time: Duration::from_millis(0),
                last_health_check: None,
                consecutive_failures: 0,
                consecutive_successes: 0,
            },
            config,
            pool,
            state: BackendState::Healthy,
            last_used: Instant::now(),
            response_times: Vec::with_capacity(100),
        })
    }

    fn record_request_start(&mut self) {
        self.last_used = Instant::now();
        self.stats.total_requests += 1;
    }

    fn record_request_end(&mut self, duration: Duration, success: bool) {
        // Update response times
        self.response_times.push(duration);
        if self.response_times.len() > 100 {
            self.response_times.remove(0);
        }

        // Calculate average response time
        let sum: Duration = self.response_times.iter().sum();
        self.stats.avg_response_time = sum / self.response_times.len() as u32;

        // Update success/failure tracking
        if success {
            self.stats.consecutive_failures = 0;
            self.stats.consecutive_successes += 1;
        } else {
            self.stats.failed_requests += 1;
            self.stats.consecutive_successes = 0;
            self.stats.consecutive_failures += 1;
        }

        // Update backend state based on health
        self.update_state();
    }

    fn update_state(&mut self) {
        let old_state = self.state;

        if self.stats.consecutive_failures >= self.config.failure_threshold {
            self.state = BackendState::Unhealthy;
        } else if self.stats.consecutive_successes >= self.config.recovery_threshold {
            self.state = BackendState::Healthy;
        } else if self.stats.consecutive_failures > 0 {
            self.state = BackendState::Degraded;
        }

        if old_state != self.state {
            debug!("Backend {} state changed from {:?} to {:?}", 
                   self.config.addr, old_state, self.state);
        }

        self.stats.state = self.state;
    }

    fn is_available(&self) -> bool {
        matches!(self.state, BackendState::Healthy | BackendState::Degraded)
    }

    async fn get_connection(&self) -> LoadBalancerResult<PooledConnection> {
        if !self.is_available() {
            return Err(LoadBalancerError::NoHealthyBackends);
        }

        self.pool.acquire().await.map_err(LoadBalancerError::Pool)
    }
}

/// High-performance load balancer
pub struct LoadBalancer {
    strategy: LoadBalanceStrategy,
    backends: RwLock<HashMap<SocketAddr, Backend>>,
    round_robin_counter: AtomicUsize,
    total_requests: AtomicU64,
    failed_requests: AtomicU64,
}

impl LoadBalancer {
    /// Create a new load balancer
    pub async fn new(
        backend_addrs: Vec<SocketAddr>,
        strategy: LoadBalanceStrategy,
        tcp_config: TcpConfig,
    ) -> LoadBalancerResult<Self> {
        let mut backends = HashMap::new();

        for addr in backend_addrs {
            let config = BackendConfig::new(addr);
            let backend = Backend::new(config, tcp_config.clone()).await?;
            backends.insert(addr, backend);
        }

        info!("Created load balancer with {} backends using {:?} strategy", 
              backends.len(), strategy);

        Ok(Self {
            strategy,
            backends: RwLock::new(backends),
            round_robin_counter: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
        })
    }

    /// Create load balancer with custom backend configurations
    pub async fn with_backends(
        backend_configs: Vec<BackendConfig>,
        strategy: LoadBalanceStrategy,
        tcp_config: TcpConfig,
    ) -> LoadBalancerResult<Self> {
        let mut backends = HashMap::new();

        for config in backend_configs {
            let backend = Backend::new(config.clone(), tcp_config.clone()).await?;
            backends.insert(config.addr, backend);
        }

        info!("Created load balancer with {} custom backends using {:?} strategy", 
              backends.len(), strategy);

        Ok(Self {
            strategy,
            backends: RwLock::new(backends),
            round_robin_counter: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
        })
    }

    /// Get a connection from the load balancer
    pub async fn get_connection(&self, client_addr: Option<SocketAddr>) -> LoadBalancerResult<(PooledConnection, SocketAddr)> {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        let backend_addr = self.select_backend(client_addr)?;
        
        let result = {
            let mut backends = self.backends.write();
            let backend = backends.get_mut(&backend_addr)
                .ok_or_else(|| LoadBalancerError::BackendNotFound(backend_addr.to_string()))?;
            
            backend.record_request_start();
            backend.get_connection().await
        };

        match result {
            Ok(connection) => {
                debug!("Selected backend {} for request", backend_addr);
                Ok((connection, backend_addr))
            }
            Err(e) => {
                self.record_backend_failure(backend_addr).await;
                self.failed_requests.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Record the completion of a request
    pub async fn record_request_completion(
        &self,
        backend_addr: SocketAddr,
        duration: Duration,
        success: bool,
    ) {
        let mut backends = self.backends.write();
        if let Some(backend) = backends.get_mut(&backend_addr) {
            backend.record_request_end(duration, success);
        }
    }

    fn select_backend(&self, client_addr: Option<SocketAddr>) -> LoadBalancerResult<SocketAddr> {
        let backends = self.backends.read();
        let available_backends: Vec<_> = backends.values()
            .filter(|b| b.is_available())
            .collect();

        if available_backends.is_empty() {
            return Err(LoadBalancerError::NoHealthyBackends);
        }

        let selected = match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                self.select_round_robin(&available_backends)
            }
            LoadBalanceStrategy::LeastConnections => {
                self.select_least_connections(&available_backends)
            }
            LoadBalanceStrategy::WeightedRoundRobin => {
                self.select_weighted_round_robin(&available_backends)
            }
            LoadBalanceStrategy::IpHash => {
                self.select_ip_hash(&available_backends, client_addr)
            }
            LoadBalanceStrategy::Random => {
                self.select_random(&available_backends)
            }
            LoadBalanceStrategy::ResponseTime => {
                self.select_response_time(&available_backends)
            }
        };

        Ok(selected.config.addr)
    }

    fn select_round_robin<'a>(&self, backends: &'a [&'a Backend]) -> &'a Backend {
        let index = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % backends.len();
        backends[index]
    }

    fn select_least_connections<'a>(&self, backends: &'a [&'a Backend]) -> &'a Backend {
        backends.iter()
            .min_by_key(|b| b.pool.stats().active_connections)
            .unwrap()
    }

    fn select_weighted_round_robin<'a>(&self, backends: &'a [&'a Backend]) -> &'a Backend {
        let total_weight: u32 = backends.iter().map(|b| b.config.weight).sum();
        let mut target = (self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as u32) % total_weight;

        for backend in backends {
            if target < backend.config.weight {
                return backend;
            }
            target -= backend.config.weight;
        }

        backends[0] // Fallback
    }

    fn select_ip_hash<'a>(&self, backends: &'a [&'a Backend], client_addr: Option<SocketAddr>) -> &'a Backend {
        if let Some(addr) = client_addr {
            let hash = self.hash_address(addr);
            let index = hash % backends.len();
            backends[index]
        } else {
            // Fallback to round-robin if no client address
            self.select_round_robin(backends)
        }
    }

    fn select_random<'a>(&self, backends: &'a [&'a Backend]) -> &'a Backend {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        Instant::now().hash(&mut hasher);
        let hash = hasher.finish();

        let index = (hash as usize) % backends.len();
        backends[index]
    }

    fn select_response_time<'a>(&self, backends: &'a [&'a Backend]) -> &'a Backend {
        backends.iter()
            .min_by_key(|b| b.stats.avg_response_time)
            .unwrap()
    }

    fn hash_address(&self, addr: SocketAddr) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        addr.ip().hash(&mut hasher);
        hasher.finish() as usize
    }

    async fn record_backend_failure(&self, backend_addr: SocketAddr) {
        let mut backends = self.backends.write();
        if let Some(backend) = backends.get_mut(&backend_addr) {
            backend.record_request_end(Duration::from_millis(0), false);
            warn!("Recorded failure for backend {}", backend_addr);
        }
    }

    /// Add a new backend
    pub async fn add_backend(&self, config: BackendConfig, tcp_config: TcpConfig) -> LoadBalancerResult<()> {
        let backend = Backend::new(config.clone(), tcp_config).await?;
        self.backends.write().insert(config.addr, backend);
        info!("Added backend {}", config.addr);
        Ok(())
    }

    /// Remove a backend
    pub async fn remove_backend(&self, addr: SocketAddr) -> LoadBalancerResult<()> {
        if let Some(backend) = self.backends.write().remove(&addr) {
            backend.pool.shutdown().await;
            info!("Removed backend {}", addr);
            Ok(())
        } else {
            Err(LoadBalancerError::BackendNotFound(addr.to_string()))
        }
    }

    /// Get all backend statistics
    pub fn get_backend_stats(&self) -> Vec<BackendStats> {
        self.backends.read().values()
            .map(|backend| {
                let mut stats = backend.stats.clone();
                let pool_stats = backend.pool.stats();
                stats.active_connections = pool_stats.active_connections;
                stats
            })
            .collect()
    }

    /// Get load balancer statistics
    pub fn stats(&self) -> LoadBalancerStats {
        let backends = self.backends.read();
        let healthy_count = backends.values()
            .filter(|b| b.state == BackendState::Healthy)
            .count();
        let degraded_count = backends.values()
            .filter(|b| b.state == BackendState::Degraded)
            .count();
        let unhealthy_count = backends.values()
            .filter(|b| b.state == BackendState::Unhealthy)
            .count();

        LoadBalancerStats {
            total_backends: backends.len(),
            healthy_backends: healthy_count,
            degraded_backends: degraded_count,
            unhealthy_backends: unhealthy_count,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            strategy: self.strategy.clone(),
        }
    }

    /// Perform health checks on all backends
    pub async fn health_check(&self) {
        // In a real implementation, this would send actual health check requests
        // For now, we'll just mark backends that haven't been used recently as potentially unhealthy
        let mut backends = self.backends.write();
        let now = Instant::now();

        for backend in backends.values_mut() {
            if now.duration_since(backend.last_used) > Duration::from_secs(300) { // 5 minutes
                // Backend hasn't been used recently, might be unhealthy
                if backend.state == BackendState::Healthy {
                    backend.state = BackendState::Degraded;
                    debug!("Marked unused backend {} as degraded", backend.config.addr);
                }
            }
            backend.stats.last_health_check = Some(now);
        }
    }

    /// Shutdown all backends
    pub async fn shutdown(&self) {
        info!("Shutting down load balancer");
        let mut backends = self.backends.write();
        
        for (addr, backend) in backends.drain() {
            backend.pool.shutdown().await;
            debug!("Shutdown backend {}", addr);
        }
        
        info!("Load balancer shutdown completed");
    }
}

/// Load balancer statistics
#[derive(Debug, Clone)]
pub struct LoadBalancerStats {
    pub total_backends: usize,
    pub healthy_backends: usize,
    pub degraded_backends: usize,
    pub unhealthy_backends: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
    pub strategy: LoadBalanceStrategy,
}

impl LoadBalancerStats {
    pub fn success_rate(&self) -> f64 {
        if self.total_requests > 0 {
            1.0 - (self.failed_requests as f64 / self.total_requests as f64)
        } else {
            1.0
        }
    }

    pub fn healthy_percentage(&self) -> f64 {
        if self.total_backends > 0 {
            self.healthy_backends as f64 / self.total_backends as f64 * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_backend_config() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let config = BackendConfig::new(addr)
            .with_weight(5)
            .with_max_connections(200);

        assert_eq!(config.addr, addr);
        assert_eq!(config.weight, 5);
        assert_eq!(config.max_connections, 200);
    }

    #[test]
    fn test_load_balance_strategies() {
        assert_eq!(LoadBalanceStrategy::RoundRobin, LoadBalanceStrategy::RoundRobin);
        assert_ne!(LoadBalanceStrategy::RoundRobin, LoadBalanceStrategy::LeastConnections);
    }

    #[tokio::test]
    async fn test_load_balancer_creation() {
        let backends = vec![
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8081)),
        ];
        
        let config = TcpConfig::default();
        
        // This will fail to connect, but we can test the creation logic
        let result = LoadBalancer::new(backends, LoadBalanceStrategy::RoundRobin, config).await;
        
        // We expect this to fail since there are no servers listening
        assert!(result.is_err());
    }

    #[test]
    fn test_backend_state() {
        assert_eq!(BackendState::Healthy, BackendState::Healthy);
        assert_ne!(BackendState::Healthy, BackendState::Unhealthy);
    }

    #[test]
    fn test_load_balancer_stats() {
        let stats = LoadBalancerStats {
            total_backends: 3,
            healthy_backends: 2,
            degraded_backends: 1,
            unhealthy_backends: 0,
            total_requests: 100,
            failed_requests: 5,
            strategy: LoadBalanceStrategy::RoundRobin,
        };

        assert_eq!(stats.success_rate(), 0.95); // 95% success rate
        assert!((stats.healthy_percentage() - 66.66666666666667).abs() < 0.0000001); // ~67% healthy
    }
}