//! Node identification and metadata

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Unique identifier for a node in the distributed system
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    /// Generate a new random node ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from a UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Convert to bytes
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Node status in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is healthy and operational
    Healthy,
    /// Node is suspected to be failing
    Suspected,
    /// Node has failed or is unreachable
    Failed,
    /// Node is in maintenance mode
    Maintenance,
    /// Node is starting up
    Starting,
    /// Node is shutting down
    Shutting,
}

impl NodeStatus {
    /// Check if the node is considered available for operations
    pub fn is_available(&self) -> bool {
        matches!(self, NodeStatus::Healthy)
    }

    /// Check if the node is considered failed
    pub fn is_failed(&self) -> bool {
        matches!(self, NodeStatus::Failed)
    }
}

/// Information about a node in the distributed system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for this node
    pub id: NodeId,
    /// Network address where this node can be reached
    pub address: SocketAddr,
    /// Current status of the node
    pub status: NodeStatus,
    /// Timestamp when this node was last seen
    pub last_seen: SystemTime,
    /// Version of the node software
    pub version: String,
    /// Additional metadata about the node
    pub metadata: HashMap<String, String>,
    /// Node capabilities
    pub capabilities: Vec<String>,
    /// Data center or zone where the node is located
    pub datacenter: Option<String>,
    /// Rack identifier for network topology awareness
    pub rack: Option<String>,
}

impl NodeInfo {
    /// Create a new NodeInfo
    pub fn new(id: NodeId, address: SocketAddr) -> Self {
        Self {
            id,
            address,
            status: NodeStatus::Starting,
            last_seen: SystemTime::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            metadata: HashMap::new(),
            capabilities: Vec::new(),
            datacenter: None,
            rack: None,
        }
    }

    /// Update the last seen timestamp
    pub fn touch(&mut self) {
        self.last_seen = SystemTime::now();
    }

    /// Check if this node is stale (hasn't been seen recently)
    pub fn is_stale(&self, threshold: Duration) -> bool {
        if let Ok(elapsed) = self.last_seen.elapsed() {
            elapsed > threshold
        } else {
            true // If we can't determine elapsed time, consider it stale
        }
    }

    /// Add a capability to this node
    pub fn add_capability(&mut self, capability: String) {
        if !self.capabilities.contains(&capability) {
            self.capabilities.push(capability);
        }
    }

    /// Check if this node has a specific capability
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.contains(&capability.to_string())
    }

    /// Set metadata key-value pair
    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get metadata value by key
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Set the datacenter for this node
    pub fn set_datacenter(&mut self, datacenter: String) {
        self.datacenter = Some(datacenter);
    }

    /// Set the rack for this node
    pub fn set_rack(&mut self, rack: String) {
        self.rack = Some(rack);
    }

    /// Get the age of this node info
    pub fn age(&self) -> Duration {
        self.last_seen.elapsed().unwrap_or(Duration::from_secs(0))
    }

    /// Update the status of this node
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
        self.touch();
    }
}

/// Builder for creating NodeInfo with fluent API
pub struct NodeInfoBuilder {
    info: NodeInfo,
}

impl NodeInfoBuilder {
    /// Start building with ID and address
    pub fn new(id: NodeId, address: SocketAddr) -> Self {
        Self {
            info: NodeInfo::new(id, address),
        }
    }

    /// Set the version
    pub fn version(mut self, version: String) -> Self {
        self.info.version = version;
        self
    }

    /// Add a capability
    pub fn capability(mut self, capability: String) -> Self {
        self.info.add_capability(capability);
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: String, value: String) -> Self {
        self.info.set_metadata(key, value);
        self
    }

    /// Set datacenter
    pub fn datacenter(mut self, datacenter: String) -> Self {
        self.info.set_datacenter(datacenter);
        self
    }

    /// Set rack
    pub fn rack(mut self, rack: String) -> Self {
        self.info.set_rack(rack);
        self
    }

    /// Set initial status
    pub fn status(mut self, status: NodeStatus) -> Self {
        self.info.status = status;
        self
    }

    /// Build the NodeInfo
    pub fn build(self) -> NodeInfo {
        self.info
    }
}

/// Node registry for managing known nodes
#[derive(Debug)]
pub struct NodeRegistry {
    nodes: dashmap::DashMap<NodeId, NodeInfo>,
    stale_threshold: Duration,
}

impl NodeRegistry {
    /// Create a new node registry
    pub fn new() -> Self {
        Self {
            nodes: dashmap::DashMap::new(),
            stale_threshold: Duration::from_secs(60), // 1 minute default
        }
    }

    /// Create with custom stale threshold
    pub fn with_stale_threshold(threshold: Duration) -> Self {
        Self {
            nodes: dashmap::DashMap::new(),
            stale_threshold: threshold,
        }
    }

    /// Register a new node or update existing one
    pub fn register(&self, info: NodeInfo) {
        self.nodes.insert(info.id.clone(), info);
    }

    /// Get node information by ID
    pub fn get(&self, id: &NodeId) -> Option<NodeInfo> {
        self.nodes.get(id).map(|entry| entry.clone())
    }

    /// Remove a node from the registry
    pub fn remove(&self, id: &NodeId) -> Option<NodeInfo> {
        self.nodes.remove(id).map(|(_, info)| info)
    }

    /// Update node status
    pub fn update_status(&self, id: &NodeId, status: NodeStatus) {
        if let Some(mut entry) = self.nodes.get_mut(id) {
            entry.set_status(status);
        }
    }

    /// Touch a node to update its last seen timestamp
    pub fn touch(&self, id: &NodeId) {
        if let Some(mut entry) = self.nodes.get_mut(id) {
            entry.touch();
        }
    }

    /// Get all healthy nodes
    pub fn healthy_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|entry| entry.status.is_available())
            .map(|entry| entry.clone())
            .collect()
    }

    /// Get all failed nodes
    pub fn failed_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|entry| entry.status.is_failed())
            .map(|entry| entry.clone())
            .collect()
    }

    /// Get all nodes with a specific capability
    pub fn nodes_with_capability(&self, capability: &str) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|entry| entry.has_capability(capability))
            .map(|entry| entry.clone())
            .collect()
    }

    /// Cleanup stale nodes
    pub fn cleanup_stale(&self) -> Vec<NodeId> {
        let mut removed = Vec::new();
        
        self.nodes.retain(|id, info| {
            if info.is_stale(self.stale_threshold) {
                removed.push(id.clone());
                false
            } else {
                true
            }
        });

        removed
    }

    /// Get all node IDs
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all nodes
    pub fn all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|entry| entry.clone()).collect()
    }

    /// Get node count
    pub fn count(&self) -> usize {
        self.nodes.len()
    }

    /// Check if a node exists
    pub fn contains(&self, id: &NodeId) -> bool {
        self.nodes.contains_key(id)
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_node_id_creation() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_node_info_builder() {
        let id = NodeId::new();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        
        let info = NodeInfoBuilder::new(id.clone(), addr)
            .version("1.0.0".to_string())
            .capability("storage".to_string())
            .metadata("region".to_string(), "us-west-1".to_string())
            .datacenter("dc1".to_string())
            .build();

        assert_eq!(info.id, id);
        assert_eq!(info.address, addr);
        assert_eq!(info.version, "1.0.0");
        assert!(info.has_capability("storage"));
        assert_eq!(info.get_metadata("region"), Some(&"us-west-1".to_string()));
        assert_eq!(info.datacenter, Some("dc1".to_string()));
    }

    #[test]
    fn test_node_registry() {
        let registry = NodeRegistry::new();
        let id = NodeId::new();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let info = NodeInfo::new(id.clone(), addr);

        registry.register(info.clone());
        assert!(registry.contains(&id));
        assert_eq!(registry.count(), 1);

        let retrieved = registry.get(&id).unwrap();
        assert_eq!(retrieved.id, id);

        registry.update_status(&id, NodeStatus::Failed);
        let failed_nodes = registry.failed_nodes();
        assert_eq!(failed_nodes.len(), 1);
    }

    #[test]
    fn test_node_status() {
        assert!(NodeStatus::Healthy.is_available());
        assert!(!NodeStatus::Failed.is_available());
        assert!(NodeStatus::Failed.is_failed());
        assert!(!NodeStatus::Healthy.is_failed());
    }
}