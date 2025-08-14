//! Gossip protocol implementation

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Gossip message for information dissemination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    pub sender: NodeId,
    pub timestamp: SystemTime,
    pub data: GossipData,
}

/// Types of gossip data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipData {
    NodeAnnouncement(NodeInfo),
    NodeFailure(NodeId),
    ClusterState(Vec<NodeInfo>),
    Custom(String, Vec<u8>),
}

/// Gossip protocol implementation
pub struct GossipProtocol {
    local_node_id: NodeId,
    known_nodes: Arc<RwLock<HashMap<NodeId, (NodeInfo, Instant)>>>,
    gossip_interval: Duration,
    message_cache: Arc<RwLock<HashMap<String, (GossipMessage, Instant)>>>,
}

impl GossipProtocol {
    /// Create a new gossip protocol instance
    pub fn new(local_node_id: NodeId, gossip_interval: Duration) -> Self {
        Self {
            local_node_id,
            known_nodes: Arc::new(RwLock::new(HashMap::new())),
            gossip_interval,
            message_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a node to the known nodes list
    pub async fn add_node(&self, node: NodeInfo) {
        let mut nodes = self.known_nodes.write().await;
        nodes.insert(node.id.clone(), (node, Instant::now()));
    }

    /// Remove a node from known nodes
    pub async fn remove_node(&self, node_id: &NodeId) {
        let mut nodes = self.known_nodes.write().await;
        nodes.remove(node_id);
    }

    /// Perform a gossip round
    pub async fn gossip_round(&self) -> DistributedResult<()> {
        let nodes = self.known_nodes.read().await;
        if nodes.is_empty() {
            return Ok(());
        }

        // Select random nodes to gossip with
        let target_count = (nodes.len() as f64).sqrt().ceil() as usize;
        let targets: Vec<_> = nodes
            .values()
            .take(target_count)
            .map(|(node, _)| node.clone())
            .collect();

        drop(nodes);

        // Send gossip messages
        for target in targets {
            if let Err(e) = self.send_gossip_to(&target).await {
                warn!("Failed to gossip to {}: {}", target.id, e);
            }
        }

        // Cleanup old messages
        self.cleanup_message_cache().await;

        Ok(())
    }

    /// Send gossip message to a specific node
    async fn send_gossip_to(&self, target: &NodeInfo) -> DistributedResult<()> {
        let nodes = self.known_nodes.read().await;
        let cluster_state: Vec<NodeInfo> = nodes.values().map(|(node, _)| node.clone()).collect();
        drop(nodes);

        let message = GossipMessage {
            sender: self.local_node_id.clone(),
            timestamp: SystemTime::now(),
            data: GossipData::ClusterState(cluster_state),
        };

        // In a real implementation, this would send over the network
        debug!("Gossiping cluster state to {}", target.id);
        
        // Cache the message
        let message_id = format!("{}-{:?}", message.sender, message.timestamp);
        let mut cache = self.message_cache.write().await;
        cache.insert(message_id, (message, Instant::now()));

        Ok(())
    }

    /// Process incoming gossip message
    pub async fn process_gossip(&self, message: GossipMessage) -> DistributedResult<()> {
        // Check if we've seen this message before
        let message_id = format!("{}-{:?}", message.sender, message.timestamp);
        {
            let cache = self.message_cache.read().await;
            if cache.contains_key(&message_id) {
                return Ok(()); // Already processed
            }
        }

        // Cache the message
        {
            let mut cache = self.message_cache.write().await;
            cache.insert(message_id, (message.clone(), Instant::now()));
        }

        // Process based on message type
        match message.data {
            GossipData::NodeAnnouncement(node) => {
                self.add_node(node).await;
                debug!("Processed node announcement via gossip");
            }
            GossipData::NodeFailure(node_id) => {
                self.remove_node(&node_id).await;
                debug!("Processed node failure via gossip: {}", node_id);
            }
            GossipData::ClusterState(nodes) => {
                // Merge with our known nodes
                let mut known_nodes = self.known_nodes.write().await;
                for node in nodes {
                    known_nodes.insert(node.id.clone(), (node, Instant::now()));
                }
                debug!("Processed cluster state via gossip");
            }
            GossipData::Custom(tag, _data) => {
                debug!("Processed custom gossip message: {}", tag);
            }
        }

        Ok(())
    }

    /// Cleanup old messages from cache
    async fn cleanup_message_cache(&self) {
        let mut cache = self.message_cache.write().await;
        let cutoff = Instant::now() - Duration::from_secs(300); // 5 minutes
        
        cache.retain(|_, (_, timestamp)| *timestamp > cutoff);
    }

    /// Get all known nodes
    pub async fn known_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.known_nodes.read().await;
        nodes.values().map(|(node, _)| node.clone()).collect()
    }

    /// Announce a node failure
    pub async fn announce_failure(&self, node_id: NodeId) -> DistributedResult<()> {
        let message = GossipMessage {
            sender: self.local_node_id.clone(),
            timestamp: SystemTime::now(),
            data: GossipData::NodeFailure(node_id),
        };

        // Broadcast to all known nodes
        let nodes = self.known_nodes().await;
        for node in nodes {
            if let Err(e) = self.send_gossip_message(&node, message.clone()).await {
                warn!("Failed to announce failure to {}: {}", node.id, e);
            }
        }

        Ok(())
    }

    /// Send a custom gossip message
    pub async fn send_custom(&self, tag: String, data: Vec<u8>) -> DistributedResult<()> {
        let message = GossipMessage {
            sender: self.local_node_id.clone(),
            timestamp: SystemTime::now(),
            data: GossipData::Custom(tag, data),
        };

        let nodes = self.known_nodes().await;
        for node in nodes {
            if let Err(e) = self.send_gossip_message(&node, message.clone()).await {
                warn!("Failed to send custom gossip to {}: {}", node.id, e);
            }
        }

        Ok(())
    }

    /// Send a gossip message to a specific node
    async fn send_gossip_message(&self, target: &NodeInfo, message: GossipMessage) -> DistributedResult<()> {
        // In a real implementation, this would serialize and send over network
        debug!("Sending gossip message to {}: {:?}", target.id, message.data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[tokio::test]
    async fn test_gossip_protocol_creation() {
        let node_id = NodeId::new();
        let gossip = GossipProtocol::new(node_id.clone(), Duration::from_secs(5));
        
        let nodes = gossip.known_nodes().await;
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let node_id = NodeId::new();
        let gossip = GossipProtocol::new(node_id, Duration::from_secs(5));
        
        let test_node = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        
        gossip.add_node(test_node.clone()).await;
        let nodes = gossip.known_nodes().await;
        assert_eq!(nodes.len(), 1);
        
        gossip.remove_node(&test_node.id).await;
        let nodes = gossip.known_nodes().await;
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_process_gossip_message() {
        let node_id = NodeId::new();
        let gossip = GossipProtocol::new(node_id, Duration::from_secs(5));
        
        let new_node = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        
        let message = GossipMessage {
            sender: NodeId::new(),
            timestamp: SystemTime::now(),
            data: GossipData::NodeAnnouncement(new_node.clone()),
        };
        
        gossip.process_gossip(message).await.unwrap();
        
        let nodes = gossip.known_nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, new_node.id);
    }
}