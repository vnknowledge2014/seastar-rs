//! Data partitioning and sharding

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Partitioning strategies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Hash-based partitioning
    Hash,
    /// Range-based partitioning
    Range,
    /// Consistent hashing
    ConsistentHash,
    /// Round-robin partitioning
    RoundRobin,
}

/// A partition in the distributed system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub primary_node: NodeId,
    pub replica_nodes: Vec<NodeId>,
    pub status: PartitionStatus,
}

/// Status of a partition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionStatus {
    /// Partition is healthy and operational
    Healthy,
    /// Partition is being migrated
    Migrating,
    /// Partition is temporarily unavailable
    Unavailable,
    /// Partition is being replicated
    Replicating,
}

/// Partition manager for handling data distribution
pub struct PartitionManager {
    strategy: PartitionStrategy,
    partitions: Arc<RwLock<HashMap<u64, Partition>>>,
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    partition_count: usize,
    replication_factor: usize,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(strategy: PartitionStrategy, partition_count: usize, replication_factor: usize) -> Self {
        Self {
            strategy,
            partitions: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            partition_count,
            replication_factor,
        }
    }

    /// Add a node to the partition manager
    pub async fn add_node(&self, node: NodeInfo) -> DistributedResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node.clone());
        drop(nodes);

        // Rebalance partitions
        self.rebalance().await?;
        
        info!("Added node {} to partition manager", node.id);
        Ok(())
    }

    /// Remove a node from the partition manager
    pub async fn remove_node(&self, node_id: &NodeId) -> DistributedResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
        drop(nodes);

        // Migrate partitions from the removed node
        self.migrate_partitions_from_node(node_id).await?;
        
        // Rebalance remaining partitions
        self.rebalance().await?;
        
        info!("Removed node {} from partition manager", node_id);
        Ok(())
    }

    /// Get the partition for a given key
    pub async fn get_partition(&self, key: &[u8]) -> Option<Partition> {
        let partition_id = self.calculate_partition(key).await;
        let partitions = self.partitions.read().await;
        partitions.get(&partition_id).cloned()
    }

    /// Get the node responsible for a key
    pub async fn get_node_for_key(&self, key: &[u8]) -> Option<NodeId> {
        if let Some(partition) = self.get_partition(key).await {
            Some(partition.primary_node)
        } else {
            None
        }
    }

    /// Get all partitions
    pub async fn get_all_partitions(&self) -> Vec<Partition> {
        let partitions = self.partitions.read().await;
        partitions.values().cloned().collect()
    }

    /// Get partitions owned by a specific node
    pub async fn get_partitions_for_node(&self, node_id: &NodeId) -> Vec<Partition> {
        let partitions = self.partitions.read().await;
        partitions
            .values()
            .filter(|p| p.primary_node == *node_id || p.replica_nodes.contains(node_id))
            .cloned()
            .collect()
    }

    /// Rebalance partitions across available nodes
    pub async fn rebalance(&self) -> DistributedResult<()> {
        let nodes = self.nodes.read().await;
        let node_list: Vec<NodeId> = nodes.keys().cloned().collect();
        drop(nodes);

        if node_list.is_empty() {
            return Ok(());
        }

        let mut partitions = self.partitions.write().await;
        
        // Create or update partitions
        for i in 0..self.partition_count {
            let partition_id = i as u64;
            
            // Calculate key range for this partition
            let start_key = self.calculate_partition_start_key(i);
            let end_key = self.calculate_partition_end_key(i);
            
            // Assign primary node
            let primary_node = node_list[i % node_list.len()].clone();
            
            // Assign replica nodes
            let mut replica_nodes = Vec::new();
            for j in 1..=self.replication_factor.min(node_list.len().saturating_sub(1)) {
                let replica_idx = (i + j) % node_list.len();
                replica_nodes.push(node_list[replica_idx].clone());
            }

            let partition = Partition {
                id: partition_id,
                start_key,
                end_key,
                primary_node,
                replica_nodes,
                status: PartitionStatus::Healthy,
            };

            partitions.insert(partition_id, partition);
        }

        debug!("Rebalanced {} partitions across {} nodes", self.partition_count, node_list.len());
        Ok(())
    }

    /// Migrate partitions from a failed node
    async fn migrate_partitions_from_node(&self, failed_node: &NodeId) -> DistributedResult<()> {
        let nodes = self.nodes.read().await;
        let available_nodes: Vec<NodeId> = nodes.keys().cloned().collect();
        drop(nodes);

        if available_nodes.is_empty() {
            return Err(DistributedError::Partition("No available nodes for migration".to_string()));
        }

        let mut partitions = self.partitions.write().await;
        let mut migration_count = 0;

        for partition in partitions.values_mut() {
            let mut needs_update = false;
            
            // Migrate primary ownership
            if partition.primary_node == *failed_node {
                // Promote a replica to primary if available
                if let Some(new_primary) = partition.replica_nodes.first().cloned() {
                    partition.primary_node = new_primary.clone();
                    partition.replica_nodes.retain(|n| n != &new_primary);
                    needs_update = true;
                } else {
                    // Assign to a random available node
                    partition.primary_node = available_nodes[migration_count % available_nodes.len()].clone();
                    needs_update = true;
                }
            }

            // Remove from replicas
            if partition.replica_nodes.contains(failed_node) {
                partition.replica_nodes.retain(|n| n != failed_node);
                
                // Add a new replica if we have available nodes
                let candidates: Vec<_> = available_nodes
                    .iter()
                    .filter(|n| **n != partition.primary_node && !partition.replica_nodes.contains(n))
                    .cloned()
                    .collect();
                
                if let Some(new_replica) = candidates.first() {
                    partition.replica_nodes.push(new_replica.clone());
                }
                
                needs_update = true;
            }

            if needs_update {
                partition.status = PartitionStatus::Migrating;
                migration_count += 1;
            }
        }

        info!("Migrated {} partitions from failed node {}", migration_count, failed_node);
        Ok(())
    }

    /// Calculate which partition a key belongs to
    async fn calculate_partition(&self, key: &[u8]) -> u64 {
        match self.strategy {
            PartitionStrategy::Hash => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish() % (self.partition_count as u64)
            }
            PartitionStrategy::Range => {
                // Simple range partitioning based on first byte
                if key.is_empty() {
                    0
                } else {
                    (key[0] as u64 * self.partition_count as u64) / 256
                }
            }
            PartitionStrategy::ConsistentHash => {
                // Simplified consistent hashing
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish() % (self.partition_count as u64)
            }
            PartitionStrategy::RoundRobin => {
                // For round-robin, we need to track state (simplified here)
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish() % (self.partition_count as u64)
            }
        }
    }

    fn calculate_partition_start_key(&self, partition_index: usize) -> Vec<u8> {
        match self.strategy {
            PartitionStrategy::Range => {
                let start = (partition_index * 256) / self.partition_count;
                vec![start as u8]
            }
            _ => {
                // For hash-based strategies, start key is empty (represents hash space)
                vec![]
            }
        }
    }

    fn calculate_partition_end_key(&self, partition_index: usize) -> Vec<u8> {
        match self.strategy {
            PartitionStrategy::Range => {
                let end = ((partition_index + 1) * 256) / self.partition_count;
                vec![end.min(255) as u8]
            }
            _ => {
                // For hash-based strategies, end key is empty (represents hash space)
                vec![]
            }
        }
    }

    /// Get partition statistics
    pub async fn get_statistics(&self) -> PartitionStatistics {
        let partitions = self.partitions.read().await;
        let nodes = self.nodes.read().await;

        let total_partitions = partitions.len();
        let healthy_partitions = partitions.values().filter(|p| p.status == PartitionStatus::Healthy).count();
        let migrating_partitions = partitions.values().filter(|p| p.status == PartitionStatus::Migrating).count();

        // Calculate load per node
        let mut node_load = HashMap::new();
        for partition in partitions.values() {
            *node_load.entry(partition.primary_node.clone()).or_insert(0) += 1;
        }

        PartitionStatistics {
            total_partitions,
            healthy_partitions,
            migrating_partitions,
            total_nodes: nodes.len(),
            replication_factor: self.replication_factor,
            strategy: self.strategy.clone(),
            node_load,
        }
    }
}

/// Partition statistics
#[derive(Debug, Clone)]
pub struct PartitionStatistics {
    pub total_partitions: usize,
    pub healthy_partitions: usize,
    pub migrating_partitions: usize,
    pub total_nodes: usize,
    pub replication_factor: usize,
    pub strategy: PartitionStrategy,
    pub node_load: HashMap<NodeId, usize>,
}

impl PartitionStatistics {
    /// Calculate the load balance ratio (0.0 = perfect balance, 1.0 = worst case)
    pub fn load_balance_ratio(&self) -> f64 {
        if self.node_load.is_empty() {
            return 0.0;
        }

        let max_load = *self.node_load.values().max().unwrap_or(&0);
        let min_load = *self.node_load.values().min().unwrap_or(&0);
        
        if max_load == 0 {
            0.0
        } else {
            1.0 - (min_load as f64 / max_load as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[tokio::test]
    async fn test_partition_manager_creation() {
        let manager = PartitionManager::new(PartitionStrategy::Hash, 16, 2);
        
        let partitions = manager.get_all_partitions().await;
        assert!(partitions.is_empty()); // No nodes yet
    }

    #[tokio::test]
    async fn test_add_nodes_and_rebalance() {
        let manager = PartitionManager::new(PartitionStrategy::Hash, 8, 2);
        
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

        let partitions = manager.get_all_partitions().await;
        assert_eq!(partitions.len(), 8);
        
        // Check that all partitions are healthy
        assert!(partitions.iter().all(|p| p.status == PartitionStatus::Healthy));
    }

    #[tokio::test]
    async fn test_key_to_partition_mapping() {
        let manager = PartitionManager::new(PartitionStrategy::Hash, 4, 1);
        
        let node = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        manager.add_node(node.clone()).await.unwrap();

        let test_key = b"test_key";
        let partition = manager.get_partition(test_key).await;
        assert!(partition.is_some());

        let node_for_key = manager.get_node_for_key(test_key).await;
        assert_eq!(node_for_key, Some(node.id));
    }

    #[tokio::test]
    async fn test_node_removal_and_migration() {
        let manager = PartitionManager::new(PartitionStrategy::Hash, 4, 2);
        
        let node1 = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        let node2 = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8081)),
        );

        manager.add_node(node1.clone()).await.unwrap();
        manager.add_node(node2.clone()).await.unwrap();

        // Remove a node
        manager.remove_node(&node1.id).await.unwrap();

        let partitions = manager.get_all_partitions().await;
        // All partitions should still exist but reassigned
        assert_eq!(partitions.len(), 4);
        
        // No partition should have the removed node as primary
        assert!(partitions.iter().all(|p| p.primary_node != node1.id));
    }

    #[test]
    fn test_partition_strategies() {
        let strategies = vec![
            PartitionStrategy::Hash,
            PartitionStrategy::Range,
            PartitionStrategy::ConsistentHash,
            PartitionStrategy::RoundRobin,
        ];

        for strategy in strategies {
            let manager = PartitionManager::new(strategy.clone(), 16, 3);
            assert_eq!(manager.strategy, strategy);
        }
    }
}