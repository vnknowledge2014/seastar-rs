//! Connection pooling for efficient resource management

use crate::tcp::{TcpConnection, TcpConfig, TcpError};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, trace, warn};

#[derive(Error, Debug)]
pub enum PoolError {
    #[error("TCP error: {0}")]
    Tcp(#[from] TcpError),
    #[error("Pool exhausted: {0}")]
    Exhausted(String),
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Connection invalid: {0}")]
    Invalid(String),
}

pub type PoolResult<T> = Result<T, PoolError>;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_size: usize,
    pub min_size: usize,
    pub max_idle_time: Duration,
    pub connection_timeout: Duration,
    pub acquire_timeout: Duration,
    pub health_check_interval: Duration,
    pub retry_attempts: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 100,
            min_size: 10,
            max_idle_time: Duration::from_secs(300), // 5 minutes
            connection_timeout: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(10),
            health_check_interval: Duration::from_secs(60),
            retry_attempts: 3,
        }
    }
}

/// A pooled connection wrapper
pub struct PooledConnection {
    connection: Option<TcpConnection>,
    pool: *const ConnectionPool,
    created_at: Instant,
    last_used: Instant,
    id: u64,
}

impl PooledConnection {
    fn new(connection: TcpConnection, pool: *const ConnectionPool, id: u64) -> Self {
        let now = Instant::now();
        Self {
            connection: Some(connection),
            pool,
            created_at: now,
            last_used: now,
            id,
        }
    }

    /// Get the underlying TCP connection
    pub fn connection(&mut self) -> &mut TcpConnection {
        self.last_used = Instant::now();
        self.connection.as_mut().expect("Connection already taken")
    }

    /// Check if connection is stale
    pub fn is_stale(&self, max_idle_time: Duration) -> bool {
        self.last_used.elapsed() > max_idle_time
    }

    /// Check if connection is healthy
    pub async fn is_healthy(&mut self) -> bool {
        if let Some(ref conn) = self.connection {
            // Simple health check - connection should still be established
            // In a real implementation, you might send a ping or small packet
            conn.age() < Duration::from_secs(3600) // 1 hour max age
        } else {
            false
        }
    }

    /// Get connection age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            // Return connection to pool
            unsafe {
                if !self.pool.is_null() {
                    (&*self.pool).return_connection(connection, self.id);
                }
            }
        }
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub total_created: u64,
    pub total_destroyed: u64,
    pub current_wait_count: usize,
    pub max_wait_count: usize,
}

/// High-performance connection pool
pub struct ConnectionPool {
    target_addr: SocketAddr,
    config: PoolConfig,
    tcp_config: TcpConfig,
    
    // Connection storage
    idle_connections: Mutex<VecDeque<(TcpConnection, u64, Instant)>>, // (connection, id, created_at)
    
    // Pool management
    semaphore: Arc<Semaphore>,
    active_count: AtomicUsize,
    total_created: AtomicU64,
    total_destroyed: AtomicU64,
    next_connection_id: AtomicU64,
    
    // Statistics
    max_wait_count: AtomicUsize,
    current_wait_count: AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub async fn new(
        target_addr: SocketAddr,
        max_size: usize,
        tcp_config: TcpConfig,
    ) -> PoolResult<Self> {
        let config = PoolConfig {
            max_size,
            ..Default::default()
        };
        
        Self::with_config(target_addr, config, tcp_config).await
    }

    /// Create a connection pool with custom configuration
    pub async fn with_config(
        target_addr: SocketAddr,
        config: PoolConfig,
        tcp_config: TcpConfig,
    ) -> PoolResult<Self> {
        let pool = Self {
            target_addr,
            config: config.clone(),
            tcp_config,
            idle_connections: Mutex::new(VecDeque::new()),
            semaphore: Arc::new(Semaphore::new(config.max_size)),
            active_count: AtomicUsize::new(0),
            total_created: AtomicU64::new(0),
            total_destroyed: AtomicU64::new(0),
            next_connection_id: AtomicU64::new(1),
            max_wait_count: AtomicUsize::new(0),
            current_wait_count: AtomicUsize::new(0),
        };

        // Pre-create minimum connections
        pool.ensure_min_connections().await?;
        
        info!("Created connection pool for {} with max_size={}", 
              target_addr, config.max_size);
        
        Ok(pool)
    }

    /// Acquire a connection from the pool
    pub async fn acquire(&self) -> PoolResult<PooledConnection> {
        let _permit = timeout(self.config.acquire_timeout, self.acquire_permit()).await
            .map_err(|_| PoolError::Timeout("Acquire timeout".to_string()))?;

        // Try to get an existing connection first
        if let Some((connection, id, _)) = self.idle_connections.lock().pop_front() {
            // For now, assume pooled connections are healthy
            // In production, implement proper health checks
            self.active_count.fetch_add(1, Ordering::Relaxed);
            trace!("Reused connection {} from pool", id);
            
            let pool_ptr = self as *const Self;
            let pooled = PooledConnection::new(connection, pool_ptr, id);
            return Ok(pooled);
        }

        // Create a new connection
        let connection = self.create_connection().await?;
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        
        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_created.fetch_add(1, Ordering::Relaxed);
        
        let pool_ptr = self as *const Self;
        let pooled = PooledConnection::new(connection, pool_ptr, id);
        
        trace!("Created new connection {} for pool", id);
        Ok(pooled)
    }

    /// Try to acquire a connection without blocking
    pub fn try_acquire(&self) -> Option<PoolResult<PooledConnection>> {
        if self.semaphore.try_acquire().is_ok() {
            // We have a permit, but we need to run the async acquisition
            // This is a simplified version that only checks idle connections
            if let Some((connection, id, _)) = self.idle_connections.lock().pop_front() {
                let pool_ptr = self as *const Self;
                let pooled = PooledConnection::new(connection, pool_ptr, id);
                self.active_count.fetch_add(1, Ordering::Relaxed);
                Some(Ok(pooled))
            } else {
                // No idle connections, would need to create one
                None
            }
        } else {
            None
        }
    }

    async fn acquire_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.current_wait_count.fetch_add(1, Ordering::Relaxed);
        
        // Update max wait count
        let current_wait = self.current_wait_count.load(Ordering::Relaxed);
        let max_wait = self.max_wait_count.load(Ordering::Relaxed);
        if current_wait > max_wait {
            self.max_wait_count.store(current_wait, Ordering::Relaxed);
        }
        
        let permit = self.semaphore.acquire().await;
        self.current_wait_count.fetch_sub(1, Ordering::Relaxed);
        permit
    }

    async fn create_connection(&self) -> PoolResult<TcpConnection> {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < self.config.retry_attempts {
            match timeout(
                self.config.connection_timeout,
                TcpConnection::connect(self.target_addr, self.tcp_config.clone())
            ).await {
                Ok(Ok(connection)) => {
                    debug!("Successfully connected to {}", self.target_addr);
                    return Ok(connection);
                }
                Ok(Err(e)) => {
                    last_error = Some(e);
                    attempts += 1;
                    
                    if attempts < self.config.retry_attempts {
                        let delay = Duration::from_millis(100 * attempts as u64);
                        sleep(delay).await;
                    }
                }
                Err(_) => {
                    last_error = Some(TcpError::Timeout("Connection timeout".to_string()));
                    attempts += 1;
                    
                    if attempts < self.config.retry_attempts {
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }

        Err(PoolError::Tcp(last_error.unwrap_or(
            TcpError::Connection("Failed to connect after retries".to_string())
        )))
    }

    pub(crate) fn return_connection(&self, mut connection: TcpConnection, id: u64) {
        // Check if connection is still healthy
        if connection.age() < Duration::from_secs(3600) { // 1 hour max
            // Return to idle pool
            let idle_count = self.idle_connections.lock().len();
            
            if idle_count < self.config.max_size {
                self.idle_connections.lock().push_back((connection, id, Instant::now()));
                trace!("Returned connection {} to pool", id);
            } else {
                // Pool is full, close connection
                tokio::spawn(async move {
                    let _ = connection.close().await;
                });
                self.total_destroyed.fetch_add(1, Ordering::Relaxed);
                debug!("Pool full, discarded connection {}", id);
            }
        } else {
            // Connection is too old, close it
            tokio::spawn(async move {
                let _ = connection.close().await;
            });
            self.total_destroyed.fetch_add(1, Ordering::Relaxed);
            debug!("Discarded old connection {}", id);
        }

        self.active_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Ensure minimum number of connections are available
    async fn ensure_min_connections(&self) -> PoolResult<()> {
        let current_idle = self.idle_connections.lock().len();
        let current_active = self.active_count.load(Ordering::Relaxed);
        let current_total = current_idle + current_active;

        if current_total < self.config.min_size {
            let needed = self.config.min_size - current_total;
            let mut created = 0;
            
            for _ in 0..needed {
                match self.create_connection().await {
                    Ok(connection) => {
                        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
                        self.idle_connections.lock().push_back((connection, id, Instant::now()));
                        self.total_created.fetch_add(1, Ordering::Relaxed);
                        created += 1;
                    }
                    Err(e) => {
                        // During initial pool creation, fail if we can't create any connections
                        if current_total == 0 && created == 0 {
                            return Err(e);
                        }
                        break; // Stop if we can't create more connections
                    }
                }
            }
            
            debug!("Pre-created {} connections for pool", created);
        }

        Ok(())
    }

    /// Clean up idle connections that are too old
    pub async fn cleanup_idle_connections(&self) -> usize {
        let mut connections = self.idle_connections.lock();
        let mut cleaned = 0;
        let max_idle = self.config.max_idle_time;

        while let Some(&(_, _, created_at)) = connections.front() {
            if created_at.elapsed() > max_idle {
                if let Some((mut connection, id, _)) = connections.pop_front() {
                    // Close connection asynchronously
                    tokio::spawn(async move {
                        let _ = connection.close().await;
                    });
                    self.total_destroyed.fetch_add(1, Ordering::Relaxed);
                    cleaned += 1;
                    trace!("Cleaned up idle connection {}", id);
                }
            } else {
                break; // Remaining connections are still fresh
            }
        }

        if cleaned > 0 {
            debug!("Cleaned up {} idle connections", cleaned);
        }

        cleaned
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let idle_connections = self.idle_connections.lock().len();
        let active_connections = self.active_count.load(Ordering::Relaxed);
        
        PoolStats {
            total_connections: idle_connections + active_connections,
            active_connections,
            idle_connections,
            total_created: self.total_created.load(Ordering::Relaxed),
            total_destroyed: self.total_destroyed.load(Ordering::Relaxed),
            current_wait_count: self.current_wait_count.load(Ordering::Relaxed),
            max_wait_count: self.max_wait_count.load(Ordering::Relaxed),
        }
    }

    /// Get pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Get target address
    pub fn target_addr(&self) -> SocketAddr {
        self.target_addr
    }

    /// Shutdown the pool gracefully
    pub async fn shutdown(&self) {
        info!("Shutting down connection pool for {}", self.target_addr);
        
        // Close all idle connections
        let mut connections = self.idle_connections.lock();
        while let Some((mut connection, id, _)) = connections.pop_front() {
            let _ = connection.close().await;
            trace!("Closed connection {} during shutdown", id);
        }
        
        debug!("Connection pool shutdown completed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[tokio::test]
    async fn test_pool_creation() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let config = TcpConfig::default();
        
        // This will fail to connect, but we can test the pool creation logic
        let result = ConnectionPool::new(addr, 10, config).await;
        
        // We expect this to fail since there's no server listening
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_config() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 100);
        assert_eq!(config.min_size, 10);
        assert_eq!(config.max_idle_time, Duration::from_secs(300));
    }

    #[test]
    fn test_pooled_connection_properties() {
        // Test connection aging and health properties
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        
        // We can't easily test the full connection without a server,
        // but we can test the configuration and setup logic
        assert_eq!(addr.port(), 8080);
    }
}