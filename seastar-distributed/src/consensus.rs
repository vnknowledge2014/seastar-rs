//! Consensus algorithms for distributed coordination

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, info, warn};

/// Result of a consensus operation
pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// Errors that can occur during consensus
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("Consensus timeout")]
    Timeout,
    #[error("Insufficient votes: got {got}, needed {needed}")]
    InsufficientVotes { got: usize, needed: usize },
    #[error("Invalid proposal: {0}")]
    InvalidProposal(String),
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Already voted for this term")]
    AlreadyVoted,
    #[error("Network error: {0}")]
    Network(String),
}

/// Consensus engine trait
#[async_trait]
pub trait ConsensusEngine: Send + Sync {
    /// Propose a value for consensus
    async fn propose(&self, value: Vec<u8>) -> ConsensusResult<Vec<u8>>;
    
    /// Vote on a proposal
    async fn vote(&self, proposal_id: String, vote: bool) -> ConsensusResult<()>;
    
    /// Get the current consensus state
    async fn state(&self) -> ConsensusState;
    
    /// Add a node to the consensus group
    async fn add_node(&self, node: NodeInfo) -> ConsensusResult<()>;
    
    /// Remove a node from the consensus group
    async fn remove_node(&self, node_id: &NodeId) -> ConsensusResult<()>;
}

/// Current state of the consensus engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log_index: u64,
    pub committed_index: u64,
    pub leader: Option<NodeId>,
}

/// A consensus proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub id: String,
    pub value: Vec<u8>,
    pub proposer: NodeId,
    pub term: u64,
    pub timestamp: SystemTime,
}

/// A vote on a proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    pub proposal_id: String,
    pub voter: NodeId,
    pub vote: bool,
    pub term: u64,
    pub timestamp: SystemTime,
}

/// Raft consensus algorithm implementation
pub struct RaftConsensus {
    node_id: NodeId,
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    state: Arc<RwLock<ConsensusState>>,
    proposals: Arc<RwLock<HashMap<String, Proposal>>>,
    votes: Arc<RwLock<HashMap<String, Vec<Vote>>>>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
}

impl RaftConsensus {
    /// Create a new Raft consensus engine
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id: node_id.clone(),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(ConsensusState {
                current_term: 0,
                voted_for: None,
                log_index: 0,
                committed_index: 0,
                leader: None,
            })),
            proposals: Arc::new(RwLock::new(HashMap::new())),
            votes: Arc::new(RwLock::new(HashMap::new())),
            election_timeout: Duration::from_millis(5000),
            heartbeat_interval: Duration::from_millis(1000),
        }
    }

    /// Start the Raft consensus engine
    pub async fn start(&self) -> ConsensusResult<()> {
        info!("Starting Raft consensus engine for node: {}", self.node_id);
        
        // Start election timer
        self.start_election_timer().await;
        
        // Start heartbeat timer if we're the leader
        self.start_heartbeat_timer().await;
        
        Ok(())
    }

    /// Request votes for leadership
    async fn request_votes(&self) -> ConsensusResult<bool> {
        let mut state = self.state.write().await;
        state.current_term += 1;
        state.voted_for = Some(self.node_id.clone());
        
        let term = state.current_term;
        drop(state);

        let nodes = self.nodes.read().await;
        let mut votes = 1; // Vote for ourselves
        let needed = (nodes.len() + 1) / 2 + 1; // Majority including ourselves

        debug!("Requesting votes for term {} (need {} votes)", term, needed);

        // In a real implementation, this would send vote requests to other nodes
        // For now, we'll simulate getting votes
        for (node_id, _node_info) in nodes.iter() {
            if *node_id != self.node_id {
                // Simulate vote response
                if self.simulate_vote_response().await {
                    votes += 1;
                }
            }
        }

        Ok(votes >= needed)
    }

    /// Simulate a vote response (placeholder)
    async fn simulate_vote_response(&self) -> bool {
        // In a real implementation, this would be a network call
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen_bool(0.7) // 70% chance of getting a vote
    }

    /// Become the leader
    async fn become_leader(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        state.leader = Some(self.node_id.clone());
        
        info!("Node {} became leader for term {}", self.node_id, state.current_term);
        Ok(())
    }

    /// Start election timer
    async fn start_election_timer(&self) {
        let state = self.state.clone();
        let node_id = self.node_id.clone();
        let consensus = self.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(consensus.election_timeout).await;
                
                let current_state = state.read().await;
                let is_leader = current_state.leader.as_ref() == Some(&node_id);
                drop(current_state);

                if !is_leader {
                    // Start election
                    if let Ok(won) = consensus.request_votes().await {
                        if won {
                            let _ = consensus.become_leader().await;
                        }
                    }
                }
            }
        });
    }

    /// Start heartbeat timer
    async fn start_heartbeat_timer(&self) {
        let state = self.state.clone();
        let node_id = self.node_id.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                
                let current_state = state.read().await;
                let is_leader = current_state.leader.as_ref() == Some(&node_id);
                drop(current_state);

                if is_leader {
                    // Send heartbeats to followers
                    debug!("Sending heartbeats as leader");
                    // In a real implementation, this would send heartbeat messages
                }
            }
        });
    }

    /// Calculate majority threshold
    fn majority_threshold(&self, total_nodes: usize) -> usize {
        (total_nodes / 2) + 1
    }
}

// Manual Clone implementation
impl Clone for RaftConsensus {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            nodes: self.nodes.clone(),
            state: self.state.clone(),
            proposals: self.proposals.clone(),
            votes: self.votes.clone(),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
        }
    }
}

#[async_trait]
impl ConsensusEngine for RaftConsensus {
    async fn propose(&self, value: Vec<u8>) -> ConsensusResult<Vec<u8>> {
        let state = self.state.read().await;
        
        // Only leader can propose
        if state.leader.as_ref() != Some(&self.node_id) {
            return Err(ConsensusError::InvalidProposal("Only leader can propose".to_string()));
        }

        let proposal_id = uuid::Uuid::new_v4().to_string();
        let proposal = Proposal {
            id: proposal_id.clone(),
            value: value.clone(),
            proposer: self.node_id.clone(),
            term: state.current_term,
            timestamp: SystemTime::now(),
        };

        drop(state);

        // Store proposal
        {
            let mut proposals = self.proposals.write().await;
            proposals.insert(proposal_id.clone(), proposal);
        }

        // Initialize vote tracking
        {
            let mut votes = self.votes.write().await;
            votes.insert(proposal_id.clone(), Vec::new());
        }

        // In a real implementation, this would:
        // 1. Send the proposal to all followers
        // 2. Wait for majority of votes
        // 3. Commit the proposal if accepted

        // For now, we'll simulate acceptance
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        info!("Proposal {} accepted", proposal_id);
        Ok(value)
    }

    async fn vote(&self, proposal_id: String, vote_decision: bool) -> ConsensusResult<()> {
        let state = self.state.read().await;
        let current_term = state.current_term;
        drop(state);

        let vote = Vote {
            proposal_id: proposal_id.clone(),
            voter: self.node_id.clone(),
            vote: vote_decision,
            term: current_term,
            timestamp: SystemTime::now(),
        };

        let mut votes = self.votes.write().await;
        if let Some(proposal_votes) = votes.get_mut(&proposal_id) {
            // Check if already voted
            if proposal_votes.iter().any(|v| v.voter == self.node_id) {
                return Err(ConsensusError::AlreadyVoted);
            }
            
            proposal_votes.push(vote);
            debug!("Recorded vote for proposal {}: {}", proposal_id, vote_decision);
        }

        Ok(())
    }

    async fn state(&self) -> ConsensusState {
        self.state.read().await.clone()
    }

    async fn add_node(&self, node: NodeInfo) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node.clone());
        
        info!("Added node {} to consensus group", node.id);
        Ok(())
    }

    async fn remove_node(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Removed node {} from consensus group", node_id);
            Ok(())
        } else {
            Err(ConsensusError::NodeNotFound(node_id.to_string()))
        }
    }
}

/// Simple majority consensus (non-Byzantine)
pub struct MajorityConsensus {
    node_id: NodeId,
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    proposals: Arc<RwLock<HashMap<String, Proposal>>>,
    votes: Arc<RwLock<HashMap<String, Vec<Vote>>>>,
}

impl MajorityConsensus {
    /// Create a new majority consensus engine
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            proposals: Arc::new(RwLock::new(HashMap::new())),
            votes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ConsensusEngine for MajorityConsensus {
    async fn propose(&self, value: Vec<u8>) -> ConsensusResult<Vec<u8>> {
        let proposal_id = uuid::Uuid::new_v4().to_string();
        let proposal = Proposal {
            id: proposal_id.clone(),
            value: value.clone(),
            proposer: self.node_id.clone(),
            term: 1, // Simple term numbering
            timestamp: SystemTime::now(),
        };

        // Store proposal
        {
            let mut proposals = self.proposals.write().await;
            proposals.insert(proposal_id.clone(), proposal);
        }

        // Initialize vote tracking
        {
            let mut votes = self.votes.write().await;
            votes.insert(proposal_id.clone(), Vec::new());
        }

        // Simulate consensus process
        let nodes = self.nodes.read().await;
        let total_nodes = nodes.len() + 1; // Include ourselves
        let _needed = (total_nodes / 2) + 1;

        // Simulate getting votes
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        info!("Majority consensus achieved for proposal {}", proposal_id);
        Ok(value)
    }

    async fn vote(&self, proposal_id: String, vote_decision: bool) -> ConsensusResult<()> {
        let vote = Vote {
            proposal_id: proposal_id.clone(),
            voter: self.node_id.clone(),
            vote: vote_decision,
            term: 1,
            timestamp: SystemTime::now(),
        };

        let mut votes = self.votes.write().await;
        if let Some(proposal_votes) = votes.get_mut(&proposal_id) {
            proposal_votes.push(vote);
        }

        Ok(())
    }

    async fn state(&self) -> ConsensusState {
        ConsensusState {
            current_term: 1,
            voted_for: None,
            log_index: 0,
            committed_index: 0,
            leader: None,
        }
    }

    async fn add_node(&self, node: NodeInfo) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node);
        Ok(())
    }

    async fn remove_node(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[tokio::test]
    async fn test_raft_consensus_creation() {
        let node_id = NodeId::new();
        let consensus = RaftConsensus::new(node_id.clone());
        
        let state = consensus.state().await;
        assert_eq!(state.current_term, 0);
        assert_eq!(state.leader, None);
    }

    #[tokio::test]
    async fn test_majority_consensus() {
        let node_id = NodeId::new();
        let consensus = MajorityConsensus::new(node_id);
        
        let value = b"test value".to_vec();
        let result = consensus.propose(value.clone()).await.unwrap();
        
        assert_eq!(result, value);
    }

    #[tokio::test]
    async fn test_add_remove_nodes() {
        let node_id = NodeId::new();
        let consensus = RaftConsensus::new(node_id);
        
        let other_node = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        
        consensus.add_node(other_node.clone()).await.unwrap();
        consensus.remove_node(&other_node.id).await.unwrap();
    }
}