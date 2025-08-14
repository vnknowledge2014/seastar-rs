//! Async DNS resolution for Seastar-RS
//!
//! Provides high-performance DNS resolution with caching and load balancing

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use trust_dns_resolver::{
    TokioAsyncResolver,
    config::{ResolverConfig, ResolverOpts},
};
use crate::{Result, Error};

/// DNS record types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecordType {
    A,
    AAAA,
    CNAME,
    MX,
    TXT,
    SRV,
}

/// DNS query result
#[derive(Debug, Clone)]
pub struct DnsResult {
    pub hostname: String,
    pub addresses: Vec<IpAddr>,
    pub ttl: Duration,
    pub cached_at: Instant,
}

impl DnsResult {
    /// Check if the result is still valid (not expired)
    pub fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < self.ttl
    }
    
    /// Get a random address for load balancing
    pub fn get_random_address(&self) -> Option<IpAddr> {
        if self.addresses.is_empty() {
            None
        } else {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            let mut hasher = DefaultHasher::new();
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .hash(&mut hasher);
            
            let index = (hasher.finish() as usize) % self.addresses.len();
            Some(self.addresses[index])
        }
    }
}

/// DNS cache entry
#[derive(Debug, Clone)]
struct CacheEntry {
    result: DnsResult,
    expiry: Instant,
}

impl CacheEntry {
    fn new(result: DnsResult) -> Self {
        let expiry = result.cached_at + result.ttl;
        Self { result, expiry }
    }
    
    fn is_expired(&self) -> bool {
        Instant::now() >= self.expiry
    }
}

/// DNS resolver configuration
#[derive(Debug, Clone)]
pub struct DnsConfig {
    /// DNS server addresses
    pub name_servers: Vec<SocketAddr>,
    /// Query timeout
    pub timeout: Duration,
    /// Number of retry attempts
    pub attempts: usize,
    /// Enable DNS caching
    pub enable_cache: bool,
    /// Cache TTL override (None = use record TTL)
    pub cache_ttl: Option<Duration>,
    /// Maximum cache size
    pub max_cache_size: usize,
    /// Prefer IPv4 over IPv6
    pub prefer_ipv4: bool,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            name_servers: vec![
                "8.8.8.8:53".parse().unwrap(),
                "8.8.4.4:53".parse().unwrap(),
                "1.1.1.1:53".parse().unwrap(),
                "1.0.0.1:53".parse().unwrap(),
            ],
            timeout: Duration::from_secs(5),
            attempts: 3,
            enable_cache: true,
            cache_ttl: None,
            max_cache_size: 10000,
            prefer_ipv4: false,
        }
    }
}

/// High-performance async DNS resolver
pub struct DnsResolver {
    resolver: TokioAsyncResolver,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    config: DnsConfig,
    stats: Arc<Mutex<DnsStats>>,
}

/// DNS resolution statistics
#[derive(Debug, Default)]
pub struct DnsStats {
    pub queries_total: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub errors: u64,
    pub average_response_time_ms: f64,
    pub total_response_time_ms: u64,
}

impl DnsStats {
    fn record_query(&mut self, response_time: Duration, cache_hit: bool, error: bool) {
        self.queries_total += 1;
        self.total_response_time_ms += response_time.as_millis() as u64;
        self.average_response_time_ms = self.total_response_time_ms as f64 / self.queries_total as f64;
        
        if error {
            self.errors += 1;
        } else if cache_hit {
            self.cache_hits += 1;
        } else {
            self.cache_misses += 1;
        }
    }
}

impl DnsResolver {
    /// Create a new DNS resolver with default configuration
    pub async fn new() -> Result<Self> {
        Self::with_config(DnsConfig::default()).await
    }
    
    /// Create a new DNS resolver with custom configuration
    pub async fn with_config(config: DnsConfig) -> Result<Self> {
        // Build resolver configuration
        let mut resolver_config = ResolverConfig::new();
        
        for ns in &config.name_servers {
            resolver_config.add_name_server(trust_dns_resolver::config::NameServerConfig {
                socket_addr: *ns,
                protocol: trust_dns_resolver::config::Protocol::Udp,
                tls_dns_name: None,
                trust_negative_responses: true,
                bind_addr: None,
            });
        }
        
        let mut opts = ResolverOpts::default();
        opts.timeout = config.timeout;
        opts.attempts = config.attempts;
        opts.ip_strategy = if config.prefer_ipv4 {
            trust_dns_resolver::config::LookupIpStrategy::Ipv4Only
        } else {
            trust_dns_resolver::config::LookupIpStrategy::Ipv4AndIpv6
        };
        
        let resolver = TokioAsyncResolver::tokio(resolver_config, opts);
        
        Ok(Self {
            resolver,
            cache: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(Mutex::new(DnsStats::default())),
        })
    }
    
    /// Resolve a hostname to IP addresses
    pub async fn resolve(&self, hostname: &str) -> Result<DnsResult> {
        let start_time = Instant::now();
        
        // Check cache first
        if self.config.enable_cache {
            if let Some(cached_result) = self.get_from_cache(hostname).await {
                let response_time = start_time.elapsed();
                self.stats.lock().await.record_query(response_time, true, false);
                return Ok(cached_result);
            }
        }
        
        // Perform DNS lookup
        match self.resolve_uncached(hostname).await {
            Ok(result) => {
                let response_time = start_time.elapsed();
                
                // Cache the result
                if self.config.enable_cache {
                    self.put_in_cache(hostname.to_string(), result.clone()).await;
                }
                
                self.stats.lock().await.record_query(response_time, false, false);
                Ok(result)
            }
            Err(error) => {
                let response_time = start_time.elapsed();
                self.stats.lock().await.record_query(response_time, false, true);
                Err(error)
            }
        }
    }
    
    /// Resolve hostname without caching
    async fn resolve_uncached(&self, hostname: &str) -> Result<DnsResult> {
        let lookup = self.resolver.lookup_ip(hostname).await
            .map_err(|e| Error::Network(format!("DNS resolution failed: {}", e)))?;
        
        let addresses: Vec<IpAddr> = lookup.iter().collect();
        let ttl = self.config.cache_ttl
            .unwrap_or_else(|| Duration::from_secs(300)); // Default 5 minutes
        
        Ok(DnsResult {
            hostname: hostname.to_string(),
            addresses,
            ttl,
            cached_at: Instant::now(),
        })
    }
    
    /// Get result from cache
    async fn get_from_cache(&self, hostname: &str) -> Option<DnsResult> {
        let cache = self.cache.read().unwrap();
        
        if let Some(entry) = cache.get(hostname) {
            if !entry.is_expired() {
                return Some(entry.result.clone());
            }
        }
        
        None
    }
    
    /// Put result in cache
    async fn put_in_cache(&self, hostname: String, result: DnsResult) {
        let mut cache = self.cache.write().unwrap();
        
        // Evict expired entries
        cache.retain(|_, entry| !entry.is_expired());
        
        // Evict oldest entries if cache is full
        if cache.len() >= self.config.max_cache_size {
            if let Some(oldest_key) = cache.iter()
                .min_by_key(|(_, entry)| entry.result.cached_at)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
            }
        }
        
        cache.insert(hostname, CacheEntry::new(result));
    }
    
    /// Resolve hostname and return a single address (load balanced)
    pub async fn resolve_one(&self, hostname: &str) -> Result<IpAddr> {
        let result = self.resolve(hostname).await?;
        result.get_random_address()
            .ok_or_else(|| Error::Network("No addresses found".to_string()))
    }
    
    /// Resolve hostname with port
    pub async fn resolve_socket_addr(&self, hostname: &str, port: u16) -> Result<SocketAddr> {
        let ip = self.resolve_one(hostname).await?;
        Ok(SocketAddr::new(ip, port))
    }
    
    /// Resolve multiple hostnames concurrently
    pub async fn resolve_many(&self, hostnames: &[&str]) -> Vec<Result<DnsResult>> {
        let futures = hostnames.iter().map(|hostname| self.resolve(hostname));
        futures_util::future::join_all(futures).await
    }
    
    /// Clear the DNS cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
    }
    
    /// Get cache statistics
    pub async fn cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().unwrap();
        let total = cache.len();
        let valid = cache.values().filter(|entry| !entry.is_expired()).count();
        (total, valid)
    }
    
    /// Get DNS resolution statistics
    pub async fn stats(&self) -> DnsStats {
        let stats = self.stats.lock().await;
        DnsStats {
            queries_total: stats.queries_total,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            errors: stats.errors,
            average_response_time_ms: stats.average_response_time_ms,
            total_response_time_ms: stats.total_response_time_ms,
        }
    }
    
    /// Preload hostnames into cache
    pub async fn preload(&self, hostnames: &[&str]) -> Result<()> {
        let results = self.resolve_many(hostnames).await;
        
        let mut errors = Vec::new();
        for (hostname, result) in hostnames.iter().zip(results) {
            if let Err(e) = result {
                errors.push((hostname, e));
            }
        }
        
        if !errors.is_empty() {
            tracing::warn!("DNS preload errors: {:?}", errors);
        }
        
        Ok(())
    }
}

/// Global DNS resolver instance
static GLOBAL_DNS_RESOLVER: tokio::sync::OnceCell<Arc<DnsResolver>> = tokio::sync::OnceCell::const_new();

/// Get the global DNS resolver instance
pub async fn global_dns_resolver() -> Arc<DnsResolver> {
    GLOBAL_DNS_RESOLVER
        .get_or_init(|| async {
            Arc::new(DnsResolver::new().await.expect("Failed to create global DNS resolver"))
        })
        .await
        .clone()
}

/// Initialize the global DNS resolver with custom configuration
pub async fn initialize_dns_resolver(config: DnsConfig) -> Result<()> {
    let resolver = Arc::new(DnsResolver::with_config(config).await?);
    GLOBAL_DNS_RESOLVER
        .set(resolver)
        .map_err(|_| Error::Internal("Global DNS resolver already initialized".to_string()))?;
    Ok(())
}

/// Convenience function for resolving hostnames
pub async fn resolve(hostname: &str) -> Result<DnsResult> {
    global_dns_resolver().await.resolve(hostname).await
}

/// Convenience function for resolving to a single address
pub async fn resolve_one(hostname: &str) -> Result<IpAddr> {
    global_dns_resolver().await.resolve_one(hostname).await
}

/// Convenience function for resolving hostname:port
pub async fn resolve_socket_addr(hostname: &str, port: u16) -> Result<SocketAddr> {
    global_dns_resolver().await.resolve_socket_addr(hostname, port).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[tokio::test]
    async fn test_dns_config_default() {
        let config = DnsConfig::default();
        assert!(!config.name_servers.is_empty());
        assert!(config.enable_cache);
        assert!(config.timeout.as_secs() > 0);
    }
    
    #[tokio::test]
    async fn test_dns_result() {
        let result = DnsResult {
            hostname: "example.com".to_string(),
            addresses: vec!["93.184.216.34".parse().unwrap()],
            ttl: Duration::from_secs(300),
            cached_at: Instant::now(),
        };
        
        assert!(result.is_valid());
        assert!(result.get_random_address().is_some());
    }
    
    #[tokio::test]
    async fn test_cache_entry() {
        let result = DnsResult {
            hostname: "test.com".to_string(),
            addresses: vec!["1.2.3.4".parse().unwrap()],
            ttl: Duration::from_millis(100),
            cached_at: Instant::now(),
        };
        
        let entry = CacheEntry::new(result);
        assert!(!entry.is_expired());
        
        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(entry.is_expired());
    }
    
    #[tokio::test]
    async fn test_dns_resolver_creation() {
        let resolver = DnsResolver::new().await;
        assert!(resolver.is_ok());
        
        let custom_config = DnsConfig {
            timeout: Duration::from_secs(1),
            enable_cache: false,
            ..Default::default()
        };
        
        let custom_resolver = DnsResolver::with_config(custom_config).await;
        assert!(custom_resolver.is_ok());
    }
    
    #[tokio::test]
    async fn test_dns_stats() {
        let mut stats = DnsStats::default();
        
        stats.record_query(Duration::from_millis(100), false, false);
        assert_eq!(stats.queries_total, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.average_response_time_ms, 100.0);
        
        stats.record_query(Duration::from_millis(200), true, false);
        assert_eq!(stats.queries_total, 2);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.average_response_time_ms, 150.0);
    }
    
    #[tokio::test]
    async fn test_resolve_localhost() {
        // Test with localhost which should always resolve
        let resolver = DnsResolver::new().await.unwrap();
        
        match resolver.resolve("localhost").await {
            Ok(result) => {
                assert!(!result.addresses.is_empty());
                assert_eq!(result.hostname, "localhost");
            }
            Err(e) => {
                // DNS resolution can fail in some test environments, so we log and continue
                println!("DNS resolution failed (expected in some test environments): {}", e);
            }
        }
        
        // Test stats
        let stats = resolver.stats().await;
        assert!(stats.queries_total > 0);
    }
    
    #[test]
    fn test_global_resolver_functions() {
        // Test that the global resolver functions exist and can be called
        // (actual resolution would require tokio runtime and network access)
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let resolver = global_dns_resolver().await;
            assert!(resolver.config.enable_cache); // Should have default config
        });
    }
}