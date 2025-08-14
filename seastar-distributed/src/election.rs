//! Leader election algorithms

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{RwLock, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Leadership state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LeadershipState {
    /// Node is a follower
    Follower,
    /// Node is a candidate in election
    Candidate,
    /// Node is the leader
    Leader,
}

/// Election vote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    pub voter_id: NodeId,
    pub candidate_id: NodeId,
    pub term: u64,
    pub granted: bool,
    pub timestamp: SystemTime,
}

/// Election configuration
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub vote_timeout: Duration,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(5000),
            election_timeout_max: Duration::from_millis(10000),
            heartbeat_interval: Duration::from_millis(1000),
            vote_timeout: Duration::from_millis(2000),
        }
    }
}

/// Leader election implementation
pub struct LeaderElection {
    node_id: NodeId,
    config: ElectionConfig,
    state: Arc<RwLock<LeadershipState>>,
    current_term: Arc<Mutex<u64>>,
    voted_for: Arc<Mutex<Option<NodeId>>>,
    current_leader: Arc<RwLock<Option<NodeId>>>,
    known_nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    votes_received: Arc<Mutex<HashMap<NodeId, Vote>>>,
    last_heartbeat: Arc<Mutex<Instant>>,
    election_timer: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    heartbeat_timer: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl LeaderElection {
    /// Create a new leader election instance
    pub fn new(node_id: NodeId, config: ElectionConfig) -> Self {
        Self {
            node_id: node_id.clone(),
            config,
            state: Arc::new(RwLock::new(LeadershipState::Follower)),
            current_term: Arc::new(Mutex::new(0)),
            voted_for: Arc::new(Mutex::new(None)),
            current_leader: Arc::new(RwLock::new(None)),
            known_nodes: Arc::new(RwLock::new(HashMap::new())),
            votes_received: Arc::new(Mutex::new(HashMap::new())),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            election_timer: Arc::new(Mutex::new(None)),
            heartbeat_timer: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the election process
    pub async fn start(&self) -> DistributedResult<()> {
        info!("Starting leader election for node: {}", self.node_id);
        
        // Reset to follower state
        {
            let mut state = self.state.write().await;
            *state = LeadershipState::Follower;
        }

        // Start election timer
        self.start_election_timer().await;
        
        Ok(())
    }

    /// Stop the election process
    pub async fn stop(&self) -> DistributedResult<()> {
        info!("Stopping leader election for node: {}", self.node_id);
        
        // Stop timers
        {
            let mut election_timer = self.election_timer.lock().await;
            if let Some(timer) = election_timer.take() {
                timer.abort();
            }
        }

        {
            let mut heartbeat_timer = self.heartbeat_timer.lock().await;
            if let Some(timer) = heartbeat_timer.take() {
                timer.abort();
            }
        }

        Ok(())
    }

    /// Add a node to the election group
    pub async fn add_node(&self, node: NodeInfo) {
        let mut nodes = self.known_nodes.write().await;
        nodes.insert(node.id.clone(), node);
    }

    /// Remove a node from the election group
    pub async fn remove_node(&self, node_id: &NodeId) {
        let mut nodes = self.known_nodes.write().await;
        nodes.remove(node_id);

        // If the removed node was the leader, trigger election
        let current_leader = self.current_leader.read().await;
        if current_leader.as_ref() == Some(node_id) {
            drop(current_leader);
            self.trigger_election().await;
        }
    }

    /// Get the current leader
    pub async fn get_leader(&self) -> Option<NodeId> {
        self.current_leader.read().await.clone()
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        *state == LeadershipState::Leader
    }

    /// Get the current leadership state
    pub async fn get_state(&self) -> LeadershipState {
        self.state.read().await.clone()
    }

    /// Get the current term
    pub async fn get_term(&self) -> u64 {
        *self.current_term.lock().await
    }

    /// Handle a vote request
    pub async fn handle_vote_request(
        &self,
        candidate_id: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> DistributedResult<Vote> {
        let mut current_term = self.current_term.lock().await;
        let mut voted_for = self.voted_for.lock().await;

        // If term is newer, update our term and convert to follower
        if term > *current_term {
            *current_term = term;
            *voted_for = None;
            
            let mut state = self.state.write().await;
            *state = LeadershipState::Follower;
        }

        // Grant vote if we haven't voted in this term or already voted for this candidate
        let vote_granted = term >= *current_term && 
            (voted_for.is_none() || voted_for.as_ref() == Some(&candidate_id));

        if vote_granted {
            *voted_for = Some(candidate_id.clone());
            debug!("Granted vote to {} for term {}", candidate_id, term);
        } else {
            debug!("Denied vote to {} for term {}", candidate_id, term);
        }

        let vote = Vote {
            voter_id: self.node_id.clone(),
            candidate_id,
            term: *current_term,
            granted: vote_granted,
            timestamp: SystemTime::now(),
        };

        Ok(vote)
    }

    /// Handle a heartbeat from the leader
    pub async fn handle_heartbeat(&self, leader_id: NodeId, term: u64) -> DistributedResult<()> {
        let mut current_term = self.current_term.lock().await;
        
        // If term is newer or equal, accept leadership
        if term >= *current_term {
            *current_term = term;
            
            // Update current leader
            {
                let mut current_leader = self.current_leader.write().await;
                *current_leader = Some(leader_id.clone());
            }

            // Convert to follower
            {
                let mut state = self.state.write().await;
                *state = LeadershipState::Follower;
            }

            // Reset election timer
            {
                let mut last_heartbeat = self.last_heartbeat.lock().await;
                *last_heartbeat = Instant::now();
            }

            debug!("Received heartbeat from leader {} for term {}", leader_id, term);
        }

        Ok(())
    }

    /// Start an election
    async fn start_election(&self) {
        info!("Starting election for node: {}", self.node_id);

        // Convert to candidate
        {
            let mut state = self.state.write().await;
            *state = LeadershipState::Candidate;
        }

        // Increment term
        let term = {
            let mut current_term = self.current_term.lock().await;
            *current_term += 1;
            *current_term
        };

        // Vote for ourselves
        {
            let mut voted_for = self.voted_for.lock().await;
            *voted_for = Some(self.node_id.clone());
        }

        // Clear previous votes
        {
            let mut votes = self.votes_received.lock().await;
            votes.clear();
        }

        // Request votes from other nodes
        self.request_votes(term).await;
    }

    /// Request votes from all known nodes
    async fn request_votes(&self, term: u64) {
        let nodes = self.known_nodes.read().await;
        let node_count = nodes.len() + 1; // Include ourselves
        let majority = (node_count / 2) + 1;

        debug!("Requesting votes from {} nodes (need {} for majority)", nodes.len(), majority);

        // Start with our own vote
        let mut vote_count = 1;

        // In a real implementation, this would send vote requests over the network
        // For now, we'll simulate receiving votes
        for (node_id, _node_info) in nodes.iter() {
            if *node_id != self.node_id {
                // Simulate vote response
                if self.simulate_vote_response(node_id, term).await {
                    vote_count += 1;
                    
                    if vote_count >= majority {
                        drop(nodes);
                        self.become_leader(term).await;
                        return;
                    }
                }
            }
        }

        debug!("Did not receive majority votes ({}/{})", vote_count, majority);
        
        // Convert back to follower if election failed
        {
            let mut state = self.state.write().await;
            *state = LeadershipState::Follower;
        }
    }

    /// Simulate a vote response (placeholder for network communication)
    async fn simulate_vote_response(&self, _node_id: &NodeId, _term: u64) -> bool {
        use rand::Rng;
        let (delay_ms, vote_result) = {
            let mut rng = rand::thread_rng();
            (rng.gen_range(10..=100), rng.gen_bool(0.7))
        };
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        
        // 70% chance of getting a vote
        vote_result
    }

    /// Become the leader
    async fn become_leader(&self, term: u64) {
        info!("Node {} became leader for term {}", self.node_id, term);

        // Update state
        {
            let mut state = self.state.write().await;
            *state = LeadershipState::Leader;
        }

        // Set ourselves as the current leader
        {
            let mut current_leader = self.current_leader.write().await;
            *current_leader = Some(self.node_id.clone());
        }

        // Start sending heartbeats
        self.start_heartbeat_timer().await;
    }

    /// Trigger an election
    async fn trigger_election(&self) {
        debug!("Election triggered for node: {}", self.node_id);
        
        // Clear current leader
        {
            let mut current_leader = self.current_leader.write().await;
            *current_leader = None;
        }

        // Start election after a short delay
        let election = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            election.start_election().await;
        });
    }

    /// Start the election timer
    async fn start_election_timer(&self) {
        let election = self.clone();
        let timeout = self.random_election_timeout();
        
        let timer = tokio::spawn(async move {
            loop {
                tokio::time::sleep(timeout).await;
                
                // Check if we've received a heartbeat recently
                let last_heartbeat = *election.last_heartbeat.lock().await;
                let state = election.state.read().await.clone();
                
                if last_heartbeat.elapsed() > timeout && state != LeadershipState::Leader {
                    drop(state);
                    election.trigger_election().await;
                }
            }
        });

        let mut election_timer = self.election_timer.lock().await;
        if let Some(old_timer) = election_timer.take() {
            old_timer.abort();
        }
        *election_timer = Some(timer);
    }

    /// Start the heartbeat timer (for leaders)
    async fn start_heartbeat_timer(&self) {
        let election = self.clone();
        let interval_duration = self.config.heartbeat_interval;
        
        let timer = tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            loop {
                interval.tick().await;
                
                let state = election.state.read().await.clone();
                if state == LeadershipState::Leader {
                    election.send_heartbeats().await;
                } else {
                    break; // Stop if no longer leader
                }
            }
        });

        let mut heartbeat_timer = self.heartbeat_timer.lock().await;
        if let Some(old_timer) = heartbeat_timer.take() {
            old_timer.abort();
        }
        *heartbeat_timer = Some(timer);
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self) {
        let term = *self.current_term.lock().await;
        let nodes = self.known_nodes.read().await;
        
        debug!("Sending heartbeats to {} followers", nodes.len());
        
        // In a real implementation, this would send heartbeat messages over the network
        for (node_id, _node_info) in nodes.iter() {
            if *node_id != self.node_id {
                debug!("Sent heartbeat to {}", node_id);
            }
        }
    }

    /// Generate a random election timeout
    fn random_election_timeout(&self) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        let min_ms = self.config.election_timeout_min.as_millis() as u64;
        let max_ms = self.config.election_timeout_max.as_millis() as u64;
        
        Duration::from_millis(rng.gen_range(min_ms..=max_ms))
    }
}

// Manual Clone implementation
impl Clone for LeaderElection {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            current_term: self.current_term.clone(),
            voted_for: self.voted_for.clone(),
            current_leader: self.current_leader.clone(),
            known_nodes: self.known_nodes.clone(),
            votes_received: self.votes_received.clone(),
            last_heartbeat: self.last_heartbeat.clone(),
            election_timer: self.election_timer.clone(),
            heartbeat_timer: self.heartbeat_timer.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[tokio::test]
    async fn test_leader_election_creation() {
        let node_id = NodeId::new();
        let config = ElectionConfig::default();
        let election = LeaderElection::new(node_id.clone(), config);
        
        assert_eq!(election.get_state().await, LeadershipState::Follower);
        assert_eq!(election.get_term().await, 0);
        assert!(!election.is_leader().await);
    }

    #[tokio::test]
    async fn test_add_remove_nodes() {
        let node_id = NodeId::new();
        let config = ElectionConfig::default();
        let election = LeaderElection::new(node_id, config);
        
        let test_node = NodeInfo::new(
            NodeId::new(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
        );
        
        election.add_node(test_node.clone()).await;
        
        let nodes = election.known_nodes.read().await;
        assert!(nodes.contains_key(&test_node.id));
        drop(nodes);
        
        election.remove_node(&test_node.id).await;
        
        let nodes = election.known_nodes.read().await;
        assert!(!nodes.contains_key(&test_node.id));
    }

    #[tokio::test]
    async fn test_vote_handling() {
        let node_id = NodeId::new();
        let candidate_id = NodeId::new();
        let config = ElectionConfig::default();
        let election = LeaderElection::new(node_id, config);
        
        let vote = election.handle_vote_request(candidate_id.clone(), 1, 0, 0).await.unwrap();
        
        assert_eq!(vote.candidate_id, candidate_id);
        assert!(vote.granted);
        assert_eq!(vote.term, 1);
    }

    #[tokio::test]
    async fn test_heartbeat_handling() {
        let node_id = NodeId::new();
        let leader_id = NodeId::new();
        let config = ElectionConfig::default();
        let election = LeaderElection::new(node_id, config);
        
        election.handle_heartbeat(leader_id.clone(), 1).await.unwrap();
        
        assert_eq!(election.get_leader().await, Some(leader_id));
        assert_eq!(election.get_state().await, LeadershipState::Follower);
        assert_eq!(election.get_term().await, 1);
    }
}