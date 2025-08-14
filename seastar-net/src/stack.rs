//! Network stack implementation and configuration

use crate::buffer::{BufferPool, GlobalBufferManager, ZeroCopyBuffer};
use crate::tcp::{TcpListener, TcpConnection, TcpConfig};
use crate::udp::{UdpSocket, DatagramChannel};
use crate::zero_copy::{ZeroCopyStream, ZeroCopyUdpSocket};
use crate::connection_pool::ConnectionPool;
use crate::load_balancer::{LoadBalancer, LoadBalanceStrategy};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum StackError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("Protocol error: {0}")]
    Protocol(String),
}

pub type StackResult<T> = Result<T, StackError>;

/// Network stack configuration
#[derive(Debug, Clone)]
pub struct StackConfig {
    pub max_connections: usize,
    pub buffer_pool_size: usize,
    pub default_buffer_size: usize,
    pub tcp_config: TcpConfig,
    pub enable_zero_copy: bool,
    pub connection_pool_size: usize,
    pub load_balance_strategy: LoadBalanceStrategy,
    pub worker_threads: usize,
    pub enable_numa_awareness: bool,
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            max_connections: 10000,
            buffer_pool_size: 2048,
            default_buffer_size: 64 * 1024,
            tcp_config: TcpConfig::default(),
            enable_zero_copy: true,
            connection_pool_size: 100,
            load_balance_strategy: LoadBalanceStrategy::RoundRobin,
            worker_threads: num_cpus::get(),
            enable_numa_awareness: false,
        }
    }
}

/// High-performance user-space network stack
pub struct NetworkStack {
    config: StackConfig,
    buffer_manager: Arc<GlobalBufferManager>,
    tcp_listeners: Arc<RwLock<HashMap<SocketAddr, Arc<TcpListener>>>>,
    udp_sockets: Arc<RwLock<HashMap<SocketAddr, Arc<ZeroCopyUdpSocket>>>>,
    connection_pools: Arc<RwLock<HashMap<String, Arc<ConnectionPool>>>>,
    load_balancers: Arc<RwLock<HashMap<String, Arc<Mutex<LoadBalancer>>>>>,
    runtime_stats: Arc<StackStats>,
}

#[derive(Debug, Default)]
pub struct StackStats {
    pub total_connections: std::sync::atomic::AtomicU64,
    pub active_connections: std::sync::atomic::AtomicUsize,
    pub bytes_sent: std::sync::atomic::AtomicU64,
    pub bytes_received: std::sync::atomic::AtomicU64,
    pub packets_processed: std::sync::atomic::AtomicU64,
    pub errors: std::sync::atomic::AtomicU64,
}

impl NetworkStack {
    /// Create a new network stack with default configuration
    pub fn new() -> StackResult<Self> {
        Self::with_config(StackConfig::default())
    }

    /// Create a new network stack with custom configuration
    pub fn with_config(config: StackConfig) -> StackResult<Self> {
        let buffer_manager = Arc::new(GlobalBufferManager::new()
            .map_err(|e| StackError::Config(e.to_string()))?);

        // Add specialized buffer pools
        let tcp_pool = Arc::new(BufferPool::new(config.buffer_pool_size, config.default_buffer_size)
            .map_err(|e| StackError::Config(e.to_string()))?);
        buffer_manager.add_pool(tcp_pool);

        let udp_pool = Arc::new(BufferPool::new(config.buffer_pool_size / 2, 1500)
            .map_err(|e| StackError::Config(e.to_string()))?);
        buffer_manager.add_pool(udp_pool);

        info!("Created network stack with {} worker threads", config.worker_threads);

        Ok(Self {
            config,
            buffer_manager,
            tcp_listeners: Arc::new(RwLock::new(HashMap::new())),
            udp_sockets: Arc::new(RwLock::new(HashMap::new())),
            connection_pools: Arc::new(RwLock::new(HashMap::new())),
            load_balancers: Arc::new(RwLock::new(HashMap::new())),
            runtime_stats: Arc::new(StackStats::default()),
        })
    }

    /// Create a TCP listener on the specified address
    pub async fn create_tcp_listener(&self, addr: SocketAddr) -> StackResult<Arc<TcpListener>> {
        let listener = TcpListener::bind(addr, self.config.tcp_config.clone()).await
            .map_err(|e| StackError::Config(e.to_string()))?;

        let bound_addr = listener.local_addr().map_err(|e| StackError::Config(e.to_string()))?;
        let listener = Arc::new(listener);
        self.tcp_listeners.write().insert(bound_addr, listener.clone());

        info!("Created TCP listener on {}", bound_addr);
        Ok(listener)
    }

    /// Create a UDP socket on the specified address
    pub async fn create_udp_socket(&self, addr: SocketAddr) -> StackResult<Arc<ZeroCopyUdpSocket>> {
        let socket = ZeroCopyUdpSocket::bind(addr).await
            .map_err(|e| StackError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        let bound_addr = socket.local_addr()
            .map_err(|e| StackError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let socket = Arc::new(socket);
        self.udp_sockets.write().insert(bound_addr, socket.clone());

        info!("Created UDP socket on {}", bound_addr);
        Ok(socket)
    }

    /// Create a connection pool for outbound connections
    pub async fn create_connection_pool(
        &self,
        name: String,
        target_addr: SocketAddr,
    ) -> StackResult<Arc<ConnectionPool>> {
        let pool = ConnectionPool::new(
            target_addr,
            self.config.connection_pool_size,
            self.config.tcp_config.clone(),
        ).await.map_err(|e| StackError::Config(e.to_string()))?;

        let pool = Arc::new(pool);
        self.connection_pools.write().insert(name.clone(), pool.clone());

        info!("Created connection pool '{}' for {}", name, target_addr);
        Ok(pool)
    }

    /// Create a load balancer
    pub async fn create_load_balancer(
        &self,
        name: String,
        backends: Vec<SocketAddr>,
    ) -> StackResult<Arc<Mutex<LoadBalancer>>> {
        let backends_copy = backends.clone();
        let lb = LoadBalancer::new(
            backends_copy,
            self.config.load_balance_strategy.clone(),
            self.config.tcp_config.clone(),
        ).await.map_err(|e| StackError::Config(e.to_string()))?;

        let lb = Arc::new(Mutex::new(lb));
        self.load_balancers.write().insert(name.clone(), lb.clone());

        info!("Created load balancer '{}' with {} backends", name, backends.len());
        Ok(lb)
    }

    /// Get a buffer from the global buffer manager
    pub fn get_buffer(&self, size: usize) -> StackResult<ZeroCopyBuffer> {
        self.buffer_manager.get_buffer(size)
            .map_err(|e| StackError::ResourceExhausted(e.to_string()))
    }

    /// Get TCP listener by address
    pub fn get_tcp_listener(&self, addr: &SocketAddr) -> Option<Arc<TcpListener>> {
        self.tcp_listeners.read().get(addr).cloned()
    }

    /// Get UDP socket by address
    pub fn get_udp_socket(&self, addr: &SocketAddr) -> Option<Arc<ZeroCopyUdpSocket>> {
        self.udp_sockets.read().get(addr).cloned()
    }

    /// Get connection pool by name
    pub fn get_connection_pool(&self, name: &str) -> Option<Arc<ConnectionPool>> {
        self.connection_pools.read().get(name).cloned()
    }

    /// Get load balancer by name
    pub fn get_load_balancer(&self, name: &str) -> Option<Arc<Mutex<LoadBalancer>>> {
        self.load_balancers.read().get(name).cloned()
    }

    /// Get stack configuration
    pub fn config(&self) -> &StackConfig {
        &self.config
    }

    /// Get runtime statistics
    pub fn stats(&self) -> StackStatsSnapshot {
        use std::sync::atomic::Ordering;

        StackStatsSnapshot {
            total_connections: self.runtime_stats.total_connections.load(Ordering::Relaxed),
            active_connections: self.runtime_stats.active_connections.load(Ordering::Relaxed),
            bytes_sent: self.runtime_stats.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.runtime_stats.bytes_received.load(Ordering::Relaxed),
            packets_processed: self.runtime_stats.packets_processed.load(Ordering::Relaxed),
            errors: self.runtime_stats.errors.load(Ordering::Relaxed),
            buffer_stats: self.buffer_manager.combined_stats(),
        }
    }

    /// Shutdown the network stack gracefully
    pub async fn shutdown(&self) -> StackResult<()> {
        info!("Shutting down network stack");

        // Close all TCP listeners
        let listeners: Vec<_> = self.tcp_listeners.read().values().cloned().collect();
        for listener in listeners {
            let _ = listener.close_all_connections().await;
        }

        // Close all connection pools
        let pools: Vec<_> = self.connection_pools.read().values().cloned().collect();
        for pool in pools {
            pool.shutdown().await;
        }

        // Cleanup buffer pools
        let freed = self.buffer_manager.shrink_all();
        debug!("Freed {} unused buffers during shutdown", freed);

        info!("Network stack shutdown completed");
        Ok(())
    }

    /// Perform maintenance tasks (cleanup idle connections, shrink pools, etc.)
    pub async fn maintenance(&self) {
        debug!("Performing stack maintenance");

        // Cleanup idle connections in TCP listeners
        let listeners: Vec<_> = self.tcp_listeners.read().values().cloned().collect();
        let mut total_cleaned = 0;

        for listener in listeners {
            let cleaned = listener.cleanup_idle_connections(Duration::from_secs(300)).await;
            total_cleaned += cleaned;
        }

        // Cleanup connection pools
        let pools: Vec<_> = self.connection_pools.read().values().cloned().collect();
        for pool in pools {
            pool.cleanup_idle_connections().await;
        }

        // Shrink buffer pools if needed
        let freed = self.buffer_manager.shrink_all();

        if total_cleaned > 0 || freed > 0 {
            debug!("Maintenance: cleaned {} connections, freed {} buffers", 
                   total_cleaned, freed);
        }
    }
}

impl Default for NetworkStack {
    fn default() -> Self {
        Self::new().expect("Failed to create default network stack")
    }
}

/// Snapshot of stack statistics
#[derive(Debug, Clone)]
pub struct StackStatsSnapshot {
    pub total_connections: u64,
    pub active_connections: usize,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_processed: u64,
    pub errors: u64,
    pub buffer_stats: Vec<crate::buffer::BufferPoolStats>,
}

impl StackStatsSnapshot {
    pub fn throughput_sent(&self, duration: Duration) -> f64 {
        if duration.as_secs_f64() > 0.0 {
            self.bytes_sent as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn throughput_received(&self, duration: Duration) -> f64 {
        if duration.as_secs_f64() > 0.0 {
            self.bytes_received as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn packet_rate(&self, duration: Duration) -> f64 {
        if duration.as_secs_f64() > 0.0 {
            self.packets_processed as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn error_rate(&self) -> f64 {
        if self.packets_processed > 0 {
            self.errors as f64 / self.packets_processed as f64
        } else {
            0.0
        }
    }

    pub fn total_buffer_utilization(&self) -> f64 {
        if self.buffer_stats.is_empty() {
            return 0.0;
        }

        let total_utilization: f64 = self.buffer_stats.iter()
            .map(|stats| stats.utilization())
            .sum();

        total_utilization / self.buffer_stats.len() as f64
    }
}

/// Builder for creating network stack with custom configuration
pub struct StackBuilder {
    config: StackConfig,
}

impl StackBuilder {
    /// Create a new stack builder
    pub fn new() -> Self {
        Self {
            config: StackConfig::default(),
        }
    }

    /// Set maximum connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }

    /// Set buffer pool configuration
    pub fn buffer_config(mut self, pool_size: usize, buffer_size: usize) -> Self {
        self.config.buffer_pool_size = pool_size;
        self.config.default_buffer_size = buffer_size;
        self
    }

    /// Set TCP configuration
    pub fn tcp_config(mut self, tcp_config: TcpConfig) -> Self {
        self.config.tcp_config = tcp_config;
        self
    }

    /// Enable or disable zero-copy
    pub fn zero_copy(mut self, enabled: bool) -> Self {
        self.config.enable_zero_copy = enabled;
        self
    }

    /// Set connection pool size
    pub fn connection_pool_size(mut self, size: usize) -> Self {
        self.config.connection_pool_size = size;
        self
    }

    /// Set load balancing strategy
    pub fn load_balance_strategy(mut self, strategy: LoadBalanceStrategy) -> Self {
        self.config.load_balance_strategy = strategy;
        self
    }

    /// Set number of worker threads
    pub fn worker_threads(mut self, threads: usize) -> Self {
        self.config.worker_threads = threads;
        self
    }

    /// Enable NUMA awareness
    pub fn numa_aware(mut self, enabled: bool) -> Self {
        self.config.enable_numa_awareness = enabled;
        self
    }

    /// Build the network stack
    pub fn build(self) -> StackResult<NetworkStack> {
        NetworkStack::with_config(self.config)
    }
}

impl Default for StackBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::NetworkBuffer;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[tokio::test]
    async fn test_stack_creation() {
        let stack = NetworkStack::new().unwrap();
        assert_eq!(stack.config().max_connections, 10000);
        assert!(stack.config().enable_zero_copy);
    }

    #[tokio::test]
    async fn test_stack_builder() {
        let stack = StackBuilder::new()
            .max_connections(5000)
            .buffer_config(1024, 32 * 1024)
            .zero_copy(false)
            .worker_threads(4)
            .build()
            .unwrap();

        assert_eq!(stack.config().max_connections, 5000);
        assert_eq!(stack.config().buffer_pool_size, 1024);
        assert_eq!(stack.config().default_buffer_size, 32 * 1024);
        assert!(!stack.config().enable_zero_copy);
        assert_eq!(stack.config().worker_threads, 4);
    }

    #[tokio::test]
    async fn test_tcp_listener_creation() {
        let stack = NetworkStack::new().unwrap();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        
        let listener = stack.create_tcp_listener(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();
        
        let retrieved = stack.get_tcp_listener(&bound_addr);
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_udp_socket_creation() {
        let stack = NetworkStack::new().unwrap();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        
        let result = stack.create_udp_socket(addr).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_buffer_allocation() {
        let stack = NetworkStack::new().unwrap();
        let buffer = stack.get_buffer(1024);
        assert!(buffer.is_ok());
        
        let buffer = buffer.unwrap();
        assert!(buffer.capacity() >= 1024); // Buffer should be at least the requested size
    }

    #[test]
    fn test_stack_stats() {
        let stack = NetworkStack::new().unwrap();
        let stats = stack.stats();
        
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
    }

    #[tokio::test]
    async fn test_stack_shutdown() {
        let stack = NetworkStack::new().unwrap();
        let result = stack.shutdown().await;
        assert!(result.is_ok());
    }
}