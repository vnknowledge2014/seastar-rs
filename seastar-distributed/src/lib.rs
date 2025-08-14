//! # Seastar-RS Distributed Computing Framework
//! 
//! This crate provides distributed computing capabilities for Seastar-RS applications,
//! including node discovery, consensus algorithms, distributed state management,
//! and fault tolerance mechanisms.

pub mod cluster;
pub mod consensus;
pub mod discovery;
pub mod gossip;
pub mod messaging;
pub mod partitioning;
pub mod replication;
pub mod state;
pub mod node;
pub mod election;

// Re-export main types
pub use cluster::{Cluster, ClusterConfig, ClusterNode};
pub use consensus::{ConsensusEngine, ConsensusResult, ConsensusError};
pub use discovery::{NodeDiscovery, DiscoveryService};
pub use gossip::{GossipProtocol, GossipMessage};
pub use messaging::{DistributedMessage, MessageBus, MessageHandler};
pub use partitioning::{PartitionManager, PartitionStrategy};
pub use replication::{ReplicationManager, ReplicationStrategy};
pub use state::{DistributedState, StateManager, StateMachine};
pub use node::{NodeId, NodeInfo, NodeStatus};
pub use election::{LeaderElection, LeadershipState};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DistributedError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Consensus error: {0}")]
    Consensus(String),
    #[error("Node unreachable: {0}")]
    NodeUnreachable(String),
    #[error("Partition error: {0}")]
    Partition(String),
    #[error("Replication error: {0}")]
    Replication(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type DistributedResult<T> = Result<T, DistributedError>;