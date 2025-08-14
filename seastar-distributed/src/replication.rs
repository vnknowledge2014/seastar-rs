//! Data replication and consistency

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, info, warn};

/// Replication strategies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    /// Synchronous replication (wait for all replicas)
    Synchronous,
    /// Asynchronous replication (fire and forget)
    Asynchronous,
    /// Quorum-based replication (wait for majority)
    Quorum,
    /// Chain replication
    Chain,
}

/// Consistency levels
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Strong consistency
    Strong,
    /// Eventual consistency
    Eventual,
    /// Bounded staleness
    BoundedStaleness(Duration),
}

/// Replication entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub id: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
    pub timestamp: SystemTime,
    pub checksum: u64,
}

/// Replica information
#[derive(Debug, Clone)]
pub struct Replica {
    pub node_id: NodeId,
    pub node_info: NodeInfo,
    pub last_sync: SystemTime,
    pub lag: Duration,
    pub is_healthy: bool,
}

/// Replication manager
pub struct ReplicationManager {
    strategy: ReplicationStrategy,
    consistency_level: ConsistencyLevel,
    replication_factor: usize,
    replicas: Arc<RwLock<HashMap<String, Vec<Replica>>>>, // key -> replicas
    pending_replications: Arc<Mutex<HashMap<String, ReplicationEntry>>>,
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(
        strategy: ReplicationStrategy,
        consistency_level: ConsistencyLevel,
        replication_factor: usize,
    ) -> Self {
        Self {
            strategy,
            consistency_level,
            replication_factor,
            replicas: Arc::new(RwLock::new(HashMap::new())),
            pending_replications: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a node to the replication manager
    pub async fn add_node(&self, node: NodeInfo) -> DistributedResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node.clone());
        drop(nodes);

        info!("Added node {} to replication manager", node.id);
        Ok(())
    }

    /// Remove a node from the replication manager
    pub async fn remove_node(&self, node_id: &NodeId) -> DistributedResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
        drop(nodes);

        // Remove from all replica sets
        let mut replicas = self.replicas.write().await;
        for replica_set in replicas.values_mut() {
            replica_set.retain(|r| r.node_id != *node_id);
        }

        info!("Removed node {} from replication manager", node_id);
        Ok(())
    }

    /// Replicate data to replicas
    pub async fn replicate(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        primary_node: NodeId,
    ) -> DistributedResult<()> {
        let entry = ReplicationEntry {
            id: uuid::Uuid::new_v4().to_string(),
            key: key.clone(),
            value,
            version: self.get_next_version(&key).await,
            timestamp: SystemTime::now(),
            checksum: self.calculate_checksum(&key),
        };

        // Get or create replica set for this key
        let replica_nodes = self.get_replica_nodes(&key, &primary_node).await?;

        match self.strategy {
            ReplicationStrategy::Synchronous => {
                self.replicate_synchronous(&entry, &replica_nodes).await
            }
            ReplicationStrategy::Asynchronous => {
                self.replicate_asynchronous(&entry, &replica_nodes).await
            }
            ReplicationStrategy::Quorum => {
                self.replicate_quorum(&entry, &replica_nodes).await
            }
            ReplicationStrategy::Chain => {
                self.replicate_chain(&entry, &replica_nodes).await
            }
        }
    }

    /// Read data with specified consistency
    pub async fn read(
        &self,
        key: &[u8],
        consistency: Option<ConsistencyLevel>,
    ) -> DistributedResult<Option<Vec<u8>>> {
        let consistency = consistency.unwrap_or_else(|| self.consistency_level.clone());

        let key_str = String::from_utf8_lossy(key);
        let replicas = self.replicas.read().await;
        
        if let Some(replica_set) = replicas.get(key_str.as_ref()) {
            match consistency {
                ConsistencyLevel::Strong => {
                    // Read from primary and verify with majority
                    self.read_strong_consistency(key, replica_set).await
                }
                ConsistencyLevel::Eventual => {
                    // Read from any available replica
                    self.read_eventual_consistency(key, replica_set).await
                }
                ConsistencyLevel::BoundedStaleness(max_staleness) => {
                    // Read from replica with acceptable staleness
                    self.read_bounded_staleness(key, replica_set, max_staleness).await
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Get replica nodes for a key
    async fn get_replica_nodes(&self, key: &[u8], primary_node: &NodeId) -> DistributedResult<Vec<NodeId>> {
        let nodes = self.nodes.read().await;
        let available_nodes: Vec<NodeId> = nodes
            .keys()
            .filter(|&id| id != primary_node)
            .cloned()
            .collect();

        if available_nodes.len() < self.replication_factor {
            return Err(DistributedError::Replication(
                "Insufficient nodes for replication".to_string()
            ));
        }

        // Simple selection: take first N available nodes
        Ok(available_nodes.into_iter().take(self.replication_factor).collect())
    }

    /// Synchronous replication
    async fn replicate_synchronous(
        &self,
        entry: &ReplicationEntry,
        replica_nodes: &[NodeId],
    ) -> DistributedResult<()> {
        let mut success_count = 0;
        
        for node_id in replica_nodes {
            match self.send_replication_to_node(node_id, entry).await {
                Ok(()) => {
                    success_count += 1;
                    debug!("Successfully replicated to node {}", node_id);
                }
                Err(e) => {
                    warn!("Failed to replicate to node {}: {}", node_id, e);
                    // In synchronous mode, we fail if any replica fails
                    return Err(e);
                }
            }
        }

        if success_count == replica_nodes.len() {
            self.update_replica_set(entry, replica_nodes).await;
            info!("Synchronous replication completed for key: {:?}", entry.key);
            Ok(())
        } else {
            Err(DistributedError::Replication(
                "Synchronous replication failed".to_string()
            ))
        }
    }

    /// Asynchronous replication
    async fn replicate_asynchronous(
        &self,
        entry: &ReplicationEntry,
        replica_nodes: &[NodeId],
    ) -> DistributedResult<()> {
        // Queue for async processing
        {
            let mut pending = self.pending_replications.lock().await;
            pending.insert(entry.id.clone(), entry.clone());
        }

        // Start async replication
        let entry_clone = entry.clone();
        let replica_nodes_clone = replica_nodes.to_vec();
        let manager = self.clone();

        tokio::spawn(async move {
            manager.process_async_replication(entry_clone, replica_nodes_clone).await;
        });

        info!("Queued asynchronous replication for key: {:?}", entry.key);
        Ok(())
    }

    /// Quorum-based replication
    async fn replicate_quorum(
        &self,
        entry: &ReplicationEntry,
        replica_nodes: &[NodeId],
    ) -> DistributedResult<()> {
        let quorum_size = (replica_nodes.len() / 2) + 1;
        let mut success_count = 0;
        let mut failures = Vec::new();

        for node_id in replica_nodes {
            match self.send_replication_to_node(node_id, entry).await {
                Ok(()) => {
                    success_count += 1;
                    debug!("Successfully replicated to node {}", node_id);
                    
                    if success_count >= quorum_size {
                        self.update_replica_set(entry, &replica_nodes[..success_count]).await;
                        info!("Quorum replication achieved for key: {:?}", entry.key);
                        return Ok(());
                    }
                }
                Err(e) => {
                    failures.push((node_id.clone(), e));
                }
            }
        }

        Err(DistributedError::Replication(
            format!("Failed to achieve quorum: {}/{} replicas succeeded", success_count, quorum_size)
        ))
    }

    /// Chain replication
    async fn replicate_chain(
        &self,
        entry: &ReplicationEntry,
        replica_nodes: &[NodeId],
    ) -> DistributedResult<()> {
        // In chain replication, we send to the first replica,
        // which then forwards to the next, and so on
        
        if replica_nodes.is_empty() {
            return Ok(());
        }

        // Send to the head of the chain
        match self.send_replication_to_node(&replica_nodes[0], entry).await {
            Ok(()) => {
                self.update_replica_set(entry, replica_nodes).await;
                info!("Chain replication initiated for key: {:?}", entry.key);
                Ok(())
            }
            Err(e) => {
                warn!("Chain replication failed at head node: {}", e);
                Err(e)
            }
        }
    }

    /// Send replication to a specific node
    async fn send_replication_to_node(
        &self,
        node_id: &NodeId,
        entry: &ReplicationEntry,
    ) -> DistributedResult<()> {
        // In a real implementation, this would serialize and send over network
        debug!("Sending replication to node {}: {:?}", node_id, entry.id);
        
        // Simulate network call with potential failure
        use rand::Rng;
        let (should_fail, delay_ms) = {
            let mut rng = rand::thread_rng();
            (rng.gen_bool(0.1), rng.gen_range(1..=10))
        };
        
        if should_fail { // 10% failure rate
            return Err(DistributedError::Network("Simulated network failure".to_string()));
        }

        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        
        Ok(())
    }

    /// Update replica set information
    async fn update_replica_set(&self, entry: &ReplicationEntry, replica_nodes: &[NodeId]) {
        let key_str = String::from_utf8_lossy(&entry.key);
        let mut replicas = self.replicas.write().await;
        
        let replica_set = replicas.entry(key_str.to_string()).or_insert_with(Vec::new);
        replica_set.clear();

        let nodes = self.nodes.read().await;
        for node_id in replica_nodes {
            if let Some(node_info) = nodes.get(node_id) {
                let replica = Replica {
                    node_id: node_id.clone(),
                    node_info: node_info.clone(),
                    last_sync: SystemTime::now(),
                    lag: Duration::from_millis(0),
                    is_healthy: true,
                };
                replica_set.push(replica);
            }
        }
    }

    /// Process asynchronous replication
    async fn process_async_replication(&self, entry: ReplicationEntry, replica_nodes: Vec<NodeId>) {
        let mut success_count = 0;
        
        for node_id in &replica_nodes {
            if let Ok(()) = self.send_replication_to_node(node_id, &entry).await {
                success_count += 1;
            }
        }

        if success_count > 0 {
            self.update_replica_set(&entry, &replica_nodes[..success_count]).await;
        }

        // Remove from pending
        {
            let mut pending = self.pending_replications.lock().await;
            pending.remove(&entry.id);
        }

        debug!("Async replication completed: {}/{} replicas", success_count, replica_nodes.len());
    }

    /// Read with strong consistency
    async fn read_strong_consistency(
        &self,
        _key: &[u8],
        _replica_set: &[Replica],
    ) -> DistributedResult<Option<Vec<u8>>> {
        // In a real implementation, this would:
        // 1. Read from primary
        // 2. Verify with majority of replicas
        // 3. Return consistent value
        
        debug!("Reading with strong consistency");
        Ok(Some(b"strong_consistent_value".to_vec()))
    }

    /// Read with eventual consistency
    async fn read_eventual_consistency(
        &self,
        _key: &[u8],
        replica_set: &[Replica],
    ) -> DistributedResult<Option<Vec<u8>>> {
        // Read from any healthy replica
        for replica in replica_set {
            if replica.is_healthy {
                debug!("Reading from replica: {}", replica.node_id);
                return Ok(Some(b"eventually_consistent_value".to_vec()));
            }
        }
        
        Ok(None)
    }

    /// Read with bounded staleness
    async fn read_bounded_staleness(
        &self,
        _key: &[u8],
        replica_set: &[Replica],
        max_staleness: Duration,
    ) -> DistributedResult<Option<Vec<u8>>> {
        // Find replica within staleness bound
        let now = SystemTime::now();
        
        for replica in replica_set {
            if replica.is_healthy {
                if let Ok(age) = now.duration_since(replica.last_sync) {
                    if age <= max_staleness {
                        debug!("Reading from replica within staleness bound: {}", replica.node_id);
                        return Ok(Some(b"bounded_stale_value".to_vec()));
                    }
                }
            }
        }
        
        Ok(None)
    }

    /// Get next version for a key
    async fn get_next_version(&self, _key: &[u8]) -> u64 {
        // In a real implementation, this would track versions per key
        use std::sync::atomic::{AtomicU64, Ordering};
        static VERSION_COUNTER: AtomicU64 = AtomicU64::new(1);
        VERSION_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    /// Calculate checksum for a key
    fn calculate_checksum(&self, key: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get replication statistics
    pub async fn get_statistics(&self) -> ReplicationStatistics {
        let replicas = self.replicas.read().await;
        let pending = self.pending_replications.lock().await;

        ReplicationStatistics {
            total_keys: replicas.len(),
            average_replication_factor: if replicas.is_empty() {
                0.0
            } else {
                replicas.values().map(|r| r.len()).sum::<usize>() as f64 / replicas.len() as f64
            },
            pending_replications: pending.len(),
            strategy: self.strategy.clone(),
            consistency_level: self.consistency_level.clone(),
        }
    }
}

// Manual Clone implementation
impl Clone for ReplicationManager {
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            consistency_level: self.consistency_level.clone(),
            replication_factor: self.replication_factor,
            replicas: self.replicas.clone(),
            pending_replications: self.pending_replications.clone(),
            nodes: self.nodes.clone(),
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStatistics {
    pub total_keys: usize,
    pub average_replication_factor: f64,
    pub pending_replications: usize,
    pub strategy: ReplicationStrategy,
    pub consistency_level: ConsistencyLevel,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[tokio::test]
    async fn test_replication_manager_creation() {
        let manager = ReplicationManager::new(
            ReplicationStrategy::Quorum,
            ConsistencyLevel::Strong,
            3,
        );
        
        assert_eq!(manager.strategy, ReplicationStrategy::Quorum);
        assert_eq!(manager.replication_factor, 3);
    }

    #[tokio::test]
    async fn test_add_nodes() {
        let manager = ReplicationManager::new(
            ReplicationStrategy::Synchronous,
            ConsistencyLevel::Strong,
            2,
        );
        
        let node1 = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        let node2 = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8081)),
        );

        manager.add_node(node1).await.unwrap();
        manager.add_node(node2).await.unwrap();

        let stats = manager.get_statistics().await;
        assert_eq!(stats.strategy, ReplicationStrategy::Synchronous);
    }

    #[tokio::test]
    async fn test_replication_strategies() {
        let strategies = vec![
            ReplicationStrategy::Synchronous,
            ReplicationStrategy::Asynchronous,
            ReplicationStrategy::Quorum,
            ReplicationStrategy::Chain,
        ];

        for strategy in strategies {
            let manager = ReplicationManager::new(
                strategy.clone(),
                ConsistencyLevel::Eventual,
                2,
            );
            assert_eq!(manager.strategy, strategy);
        }
    }
}