//! Cluster management and coordination

use crate::node::{NodeId, NodeInfo, NodeRegistry, NodeStatus};
use crate::messaging::{MessageBus, DistributedMessage};
use crate::discovery::DiscoveryService;
use crate::gossip::GossipProtocol;
use crate::consensus::ConsensusEngine;
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn, error};

/// Configuration for cluster behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Local node information
    pub local_node: NodeInfo,
    /// List of seed nodes for initial cluster discovery
    pub seed_nodes: Vec<SocketAddr>,
    /// Interval for health checks
    pub health_check_interval: Duration,
    /// Timeout for considering a node failed
    pub failure_timeout: Duration,
    /// Maximum number of nodes in the cluster
    pub max_nodes: usize,
    /// Minimum number of nodes for cluster operations
    pub min_nodes: usize,
    /// Enable gossip protocol
    pub enable_gossip: bool,
    /// Gossip interval
    pub gossip_interval: Duration,
    /// Enable consensus
    pub enable_consensus: bool,
    /// Consensus timeout
    pub consensus_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            local_node: NodeInfo::new(
                NodeId::new(),
                "127.0.0.1:0".parse().unwrap(),
            ),
            seed_nodes: Vec::new(),
            health_check_interval: Duration::from_secs(30),
            failure_timeout: Duration::from_secs(60),
            max_nodes: 1000,
            min_nodes: 1,
            enable_gossip: true,
            gossip_interval: Duration::from_secs(5),
            enable_consensus: false,
            consensus_timeout: Duration::from_secs(10),
        }
    }
}

/// Events that can occur in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterEvent {
    /// A new node joined the cluster
    NodeJoined(NodeInfo),
    /// A node left the cluster
    NodeLeft(NodeId),
    /// A node's status changed
    NodeStatusChanged { id: NodeId, old_status: NodeStatus, new_status: NodeStatus },
    /// Cluster split (partition) detected
    PartitionDetected { affected_nodes: Vec<NodeId> },
    /// Cluster healed after partition
    PartitionHealed,
    /// Leadership changed
    LeadershipChanged { old_leader: Option<NodeId>, new_leader: Option<NodeId> },
    /// Consensus achieved on a proposal
    ConsensusAchieved { proposal_id: String },
}

/// Cluster node representation
#[derive(Debug, Clone)]
pub struct ClusterNode {
    info: NodeInfo,
    last_heartbeat: std::time::Instant,
    consecutive_failures: u32,
}

impl ClusterNode {
    fn new(info: NodeInfo) -> Self {
        Self {
            info,
            last_heartbeat: std::time::Instant::now(),
            consecutive_failures: 0,
        }
    }

    fn update_heartbeat(&mut self) {
        self.last_heartbeat = std::time::Instant::now();
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
    }

    fn is_healthy(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() < timeout && self.consecutive_failures < 3
    }
}

/// Main cluster management structure
pub struct Cluster {
    config: ClusterConfig,
    local_node_id: NodeId,
    nodes: Arc<RwLock<HashMap<NodeId, ClusterNode>>>,
    node_registry: Arc<NodeRegistry>,
    message_bus: Arc<MessageBus>,
    discovery: Arc<Mutex<DiscoveryService>>,
    gossip: Option<Arc<Mutex<GossipProtocol>>>,
    consensus: Option<Arc<Mutex<dyn ConsensusEngine>>>,
    event_handlers: Arc<RwLock<Vec<Box<dyn Fn(ClusterEvent) + Send + Sync>>>>,
    leader: Arc<RwLock<Option<NodeId>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Cluster {
    /// Create a new cluster instance
    pub async fn new(config: ClusterConfig) -> DistributedResult<Self> {
        let local_node_id = config.local_node.id.clone();
        let node_registry = Arc::new(NodeRegistry::new());
        let message_bus = Arc::new(MessageBus::new(config.local_node.address).await?);
        let discovery = Arc::new(Mutex::new(DiscoveryService::new(config.clone()).await?));

        // Initialize gossip if enabled
        let gossip = if config.enable_gossip {
            Some(Arc::new(Mutex::new(
                GossipProtocol::new(local_node_id.clone(), config.gossip_interval)
            )))
        } else {
            None
        };

        // Register local node
        node_registry.register(config.local_node.clone());

        info!("Created cluster with local node: {}", local_node_id);

        Ok(Self {
            config,
            local_node_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            node_registry,
            message_bus,
            discovery,
            gossip,
            consensus: None, // Will be set if consensus is enabled
            event_handlers: Arc::new(RwLock::new(Vec::new())),
            leader: Arc::new(RwLock::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Start the cluster
    pub async fn start(&self) -> DistributedResult<()> {
        info!("Starting cluster...");
        
        self.running.store(true, std::sync::atomic::Ordering::SeqCst);

        // Start discovery
        self.discovery.lock().await.start().await?;

        // Start message bus
        self.message_bus.start().await?;

        // Connect to seed nodes
        self.connect_to_seeds().await?;

        // Start background tasks
        self.start_health_checker().await;
        
        if let Some(ref gossip) = self.gossip {
            self.start_gossip_protocol(gossip.clone()).await;
        }

        info!("Cluster started successfully");
        Ok(())
    }

    /// Stop the cluster
    pub async fn stop(&self) -> DistributedResult<()> {
        info!("Stopping cluster...");
        
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);

        // Notify other nodes we're leaving
        self.broadcast_leave().await?;

        // Stop services
        self.message_bus.stop().await?;
        self.discovery.lock().await.stop().await?;

        info!("Cluster stopped");
        Ok(())
    }

    /// Join an existing cluster
    pub async fn join(&self, seed_addr: SocketAddr) -> DistributedResult<()> {
        info!("Attempting to join cluster via seed: {}", seed_addr);

        // Send join request to seed node
        let join_message = DistributedMessage::join_request(
            self.local_node_id.clone(),
            self.config.local_node.clone(),
        );

        self.message_bus.send_to(seed_addr, join_message).await?;

        // Wait for acceptance
        // In a real implementation, this would wait for a response
        tokio::time::sleep(Duration::from_millis(100)).await;

        info!("Successfully joined cluster");
        Ok(())
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, node_info: NodeInfo) -> DistributedResult<()> {
        let node_id = node_info.id.clone();
        
        // Add to registry
        self.node_registry.register(node_info.clone());
        
        // Add to cluster nodes
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node_id.clone(), ClusterNode::new(node_info.clone()));
        }

        // Emit event
        self.emit_event(ClusterEvent::NodeJoined(node_info)).await;

        debug!("Added node to cluster: {}", node_id);
        Ok(())
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: &NodeId) -> DistributedResult<()> {
        // Remove from registry
        self.node_registry.remove(node_id);

        // Remove from cluster nodes
        {
            let mut nodes = self.nodes.write().await;
            nodes.remove(node_id);
        }

        // Update leader if necessary
        {
            let mut leader = self.leader.write().await;
            if leader.as_ref() == Some(node_id) {
                *leader = None;
                // Trigger leader election
                self.elect_leader().await?;
            }
        }

        // Emit event
        self.emit_event(ClusterEvent::NodeLeft(node_id.clone())).await;

        debug!("Removed node from cluster: {}", node_id);
        Ok(())
    }

    /// Get all healthy nodes
    pub async fn healthy_nodes(&self) -> Vec<NodeInfo> {
        self.node_registry.healthy_nodes()
    }

    /// Get the current leader
    pub async fn leader(&self) -> Option<NodeId> {
        self.leader.read().await.clone()
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let leader = self.leader.read().await;
        leader.as_ref() == Some(&self.local_node_id)
    }

    /// Get cluster size
    pub async fn size(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Add an event handler
    pub async fn add_event_handler<F>(&self, handler: F)
    where
        F: Fn(ClusterEvent) + Send + Sync + 'static,
    {
        self.event_handlers.write().await.push(Box::new(handler));
    }

    /// Broadcast a message to all nodes
    pub async fn broadcast(&self, message: DistributedMessage) -> DistributedResult<()> {
        let nodes = self.healthy_nodes().await;
        
        for node in nodes {
            if node.id != self.local_node_id {
                if let Err(e) = self.message_bus.send_to(node.address, message.clone()).await {
                    warn!("Failed to send message to {}: {}", node.id, e);
                }
            }
        }

        Ok(())
    }

    /// Send a message to a specific node
    pub async fn send_to(&self, node_id: &NodeId, message: DistributedMessage) -> DistributedResult<()> {
        if let Some(node_info) = self.node_registry.get(node_id) {
            self.message_bus.send_to(node_info.address, message).await?;
            Ok(())
        } else {
            Err(DistributedError::NodeUnreachable(node_id.to_string()))
        }
    }

    // Private methods

    async fn connect_to_seeds(&self) -> DistributedResult<()> {
        for seed_addr in &self.config.seed_nodes {
            if let Err(e) = self.join(*seed_addr).await {
                warn!("Failed to connect to seed {}: {}", seed_addr, e);
            }
        }
        Ok(())
    }

    async fn start_health_checker(&self) {
        let nodes = self.nodes.clone();
        let config = self.config.clone();
        let running = self.running.clone();
        let cluster = self.clone();

        tokio::spawn(async move {
            let mut interval = interval(config.health_check_interval);
            
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                interval.tick().await;
                cluster.check_node_health().await;
            }
        });
    }

    async fn start_gossip_protocol(&self, gossip: Arc<Mutex<GossipProtocol>>) {
        let running = self.running.clone();
        let interval_duration = self.config.gossip_interval;
        
        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                interval.tick().await;
                
                if let Err(e) = gossip.lock().await.gossip_round().await {
                    warn!("Gossip round failed: {}", e);
                }
            }
        });
    }

    async fn check_node_health(&self) {
        let mut failed_nodes = Vec::new();
        
        {
            let mut nodes = self.nodes.write().await;
            for (node_id, node) in nodes.iter_mut() {
                if !node.is_healthy(self.config.failure_timeout) {
                    node.record_failure();
                    
                    if node.consecutive_failures >= 3 {
                        failed_nodes.push(node_id.clone());
                    }
                }
            }
        }

        // Remove failed nodes
        for node_id in failed_nodes {
            if let Err(e) = self.remove_node(&node_id).await {
                error!("Failed to remove failed node {}: {}", node_id, e);
            }
        }
    }

    async fn elect_leader(&self) -> DistributedResult<()> {
        // Simple leader election: choose node with smallest ID
        let healthy_nodes = self.healthy_nodes().await;
        
        if let Some(new_leader) = healthy_nodes.iter().min_by_key(|node| &node.id) {
            let old_leader = {
                let mut leader = self.leader.write().await;
                let old = leader.clone();
                *leader = Some(new_leader.id.clone());
                old
            };

            self.emit_event(ClusterEvent::LeadershipChanged {
                old_leader,
                new_leader: Some(new_leader.id.clone()),
            }).await;

            info!("Elected new leader: {}", new_leader.id);
        }

        Ok(())
    }

    async fn broadcast_leave(&self) -> DistributedResult<()> {
        let leave_message = DistributedMessage::leave_notification(self.local_node_id.clone());
        self.broadcast(leave_message).await
    }

    async fn emit_event(&self, event: ClusterEvent) {
        debug!("Cluster event: {:?}", event);
        
        let handlers = self.event_handlers.read().await;
        for handler in handlers.iter() {
            handler(event.clone());
        }
    }
}

// Manual Clone implementation to avoid requiring Clone on trait objects
impl Clone for Cluster {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_node_id: self.local_node_id.clone(),
            nodes: self.nodes.clone(),
            node_registry: self.node_registry.clone(),
            message_bus: self.message_bus.clone(),
            discovery: self.discovery.clone(),
            gossip: self.gossip.clone(),
            consensus: self.consensus.clone(),
            event_handlers: self.event_handlers.clone(),
            leader: self.leader.clone(),
            running: self.running.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[tokio::test]
    async fn test_cluster_creation() {
        let config = ClusterConfig::default();
        let cluster = Cluster::new(config).await.unwrap();
        
        assert_eq!(cluster.size().await, 0);
        assert!(cluster.leader().await.is_none());
        assert!(!cluster.is_leader().await);
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let config = ClusterConfig::default();
        let cluster = Cluster::new(config).await.unwrap();
        
        let node_id = NodeId::new();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
        let node_info = NodeInfo::new(node_id.clone(), addr);
        
        cluster.add_node(node_info).await.unwrap();
        assert_eq!(cluster.size().await, 1);
        
        cluster.remove_node(&node_id).await.unwrap();
        assert_eq!(cluster.size().await, 0);
    }
}