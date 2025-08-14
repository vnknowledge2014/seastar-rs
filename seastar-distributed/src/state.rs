//! Distributed state management

use crate::node::NodeId;
use crate::{DistributedError, DistributedResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, info};

/// Distributed state machine trait
pub trait StateMachine: Send + Sync {
    /// Apply a command to the state machine
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String>;
    
    /// Get a snapshot of the current state
    fn snapshot(&self) -> Vec<u8>;
    
    /// Restore from a snapshot
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String>;
}

/// State machine entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEntry {
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
    pub timestamp: SystemTime,
}

/// Distributed state manager
pub struct StateManager {
    state_machine: Arc<Mutex<Box<dyn StateMachine>>>,
    log: Arc<RwLock<Vec<StateEntry>>>,
    committed_index: Arc<Mutex<u64>>,
    applied_index: Arc<Mutex<u64>>,
    snapshots: Arc<RwLock<HashMap<u64, Vec<u8>>>>,
    current_term: Arc<Mutex<u64>>,
}

impl StateManager {
    /// Create a new state manager
    pub fn new(state_machine: Box<dyn StateMachine>) -> Self {
        Self {
            state_machine: Arc::new(Mutex::new(state_machine)),
            log: Arc::new(RwLock::new(Vec::new())),
            committed_index: Arc::new(Mutex::new(0)),
            applied_index: Arc::new(Mutex::new(0)),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            current_term: Arc::new(Mutex::new(0)),
        }
    }

    /// Append an entry to the log
    pub async fn append_entry(&self, command: Vec<u8>) -> DistributedResult<u64> {
        let term = *self.current_term.lock().await;
        let mut log = self.log.write().await;
        
        let index = log.len() as u64 + 1;
        let entry = StateEntry {
            index,
            term,
            command,
            timestamp: SystemTime::now(),
        };

        log.push(entry);
        debug!("Appended entry at index {}", index);
        Ok(index)
    }

    /// Commit entries up to the given index
    pub async fn commit_entries(&self, up_to_index: u64) -> DistributedResult<()> {
        let mut committed_index = self.committed_index.lock().await;
        
        if up_to_index > *committed_index {
            *committed_index = up_to_index;
            debug!("Committed entries up to index {}", up_to_index);
            
            // Apply committed entries
            drop(committed_index);
            self.apply_committed_entries().await?;
        }

        Ok(())
    }

    /// Apply committed entries to the state machine
    async fn apply_committed_entries(&self) -> DistributedResult<()> {
        let committed_index = *self.committed_index.lock().await;
        let mut applied_index = self.applied_index.lock().await;

        if committed_index > *applied_index {
            let log = self.log.read().await;
            let mut state_machine = self.state_machine.lock().await;

            for i in (*applied_index + 1)..=committed_index {
                if let Some(entry) = log.get((i - 1) as usize) {
                    match state_machine.apply(&entry.command) {
                        Ok(_result) => {
                            debug!("Applied entry at index {}", i);
                        }
                        Err(e) => {
                            return Err(DistributedError::Network(
                                format!("Failed to apply entry {}: {}", i, e)
                            ));
                        }
                    }
                }
            }

            *applied_index = committed_index;
            info!("Applied entries up to index {}", committed_index);
        }

        Ok(())
    }

    /// Create a snapshot at the current applied index
    pub async fn create_snapshot(&self) -> DistributedResult<u64> {
        let applied_index = *self.applied_index.lock().await;
        let state_machine = self.state_machine.lock().await;
        
        let snapshot = state_machine.snapshot();
        
        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(applied_index, snapshot);

        info!("Created snapshot at index {}", applied_index);
        Ok(applied_index)
    }

    /// Restore from a snapshot
    pub async fn restore_snapshot(&self, index: u64) -> DistributedResult<()> {
        let snapshots = self.snapshots.read().await;
        
        if let Some(snapshot_data) = snapshots.get(&index) {
            let mut state_machine = self.state_machine.lock().await;
            
            state_machine.restore(snapshot_data)
                .map_err(|e| DistributedError::Network(format!("Snapshot restore failed: {}", e)))?;

            drop(state_machine);
            drop(snapshots);

            // Update indices
            let mut applied_index = self.applied_index.lock().await;
            *applied_index = index;

            let mut committed_index = self.committed_index.lock().await;
            if index > *committed_index {
                *committed_index = index;
            }

            info!("Restored from snapshot at index {}", index);
            Ok(())
        } else {
            Err(DistributedError::Network(
                format!("Snapshot not found at index {}", index)
            ))
        }
    }

    /// Get the current state
    pub async fn get_state(&self) -> StateInfo {
        let applied_index = *self.applied_index.lock().await;
        let committed_index = *self.committed_index.lock().await;
        let current_term = *self.current_term.lock().await;
        let log = self.log.read().await;

        StateInfo {
            applied_index,
            committed_index,
            current_term,
            log_size: log.len(),
            last_log_index: log.last().map(|e| e.index).unwrap_or(0),
            last_log_term: log.last().map(|e| e.term).unwrap_or(0),
        }
    }

    /// Truncate log entries after the given index
    pub async fn truncate_log(&self, after_index: u64) -> DistributedResult<()> {
        let mut log = self.log.write().await;
        
        if after_index < log.len() as u64 {
            log.truncate(after_index as usize);
            debug!("Truncated log after index {}", after_index);
        }

        Ok(())
    }

    /// Get log entries in a range
    pub async fn get_log_entries(&self, start_index: u64, end_index: u64) -> Vec<StateEntry> {
        let log = self.log.read().await;
        
        let start = (start_index.saturating_sub(1)) as usize;
        let end = (end_index as usize).min(log.len());
        
        if start < end {
            log[start..end].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Set the current term
    pub async fn set_term(&self, term: u64) {
        let mut current_term = self.current_term.lock().await;
        *current_term = term;
        debug!("Set current term to {}", term);
    }

    /// Cleanup old snapshots
    pub async fn cleanup_snapshots(&self, keep_count: usize) {
        let mut snapshots = self.snapshots.write().await;
        
        if snapshots.len() > keep_count {
            let mut indices: Vec<_> = snapshots.keys().cloned().collect();
            indices.sort();
            
            // Keep the most recent snapshots
            let to_remove = indices.len().saturating_sub(keep_count);
            for &index in &indices[..to_remove] {
                snapshots.remove(&index);
            }
            
            debug!("Cleaned up {} old snapshots", to_remove);
        }
    }
}

/// Information about the current state
#[derive(Debug, Clone)]
pub struct StateInfo {
    pub applied_index: u64,
    pub committed_index: u64,
    pub current_term: u64,
    pub log_size: usize,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Simple distributed state implementation
pub struct DistributedState {
    data: HashMap<String, Vec<u8>>,
}

impl DistributedState {
    /// Create a new distributed state
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    /// Set a value by key
    pub fn set(&mut self, key: String, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    /// Remove a value by key
    pub fn remove(&mut self, key: &str) -> Option<Vec<u8>> {
        self.data.remove(key)
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for DistributedState {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for DistributedState {
    fn apply(&mut self, command: &[u8]) -> Result<Vec<u8>, String> {
        // Simple command format: "SET key value" or "GET key" or "DEL key"
        let command_str = String::from_utf8_lossy(command);
        let parts: Vec<&str> = command_str.split_whitespace().collect();

        match parts.get(0).map(|s| s.to_uppercase()).as_deref() {
            Some("SET") if parts.len() >= 3 => {
                let key = parts[1].to_string();
                let value = parts[2..].join(" ").into_bytes();
                self.set(key.clone(), value);
                Ok(format!("OK: Set {}", key).into_bytes())
            }
            Some("GET") if parts.len() >= 2 => {
                let key = parts[1];
                match self.get(key) {
                    Some(value) => Ok(value.clone()),
                    None => Ok(b"NOT_FOUND".to_vec()),
                }
            }
            Some("DEL") if parts.len() >= 2 => {
                let key = parts[1];
                match self.remove(key) {
                    Some(_) => Ok(b"DELETED".to_vec()),
                    None => Ok(b"NOT_FOUND".to_vec()),
                }
            }
            _ => Err("Invalid command format".to_string()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&self.data).unwrap_or_default()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), String> {
        match bincode::deserialize::<HashMap<String, Vec<u8>>>(snapshot) {
            Ok(data) => {
                self.data = data;
                Ok(())
            }
            Err(e) => Err(format!("Failed to deserialize snapshot: {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_state_manager() {
        let state_machine = Box::new(DistributedState::new());
        let manager = StateManager::new(state_machine);

        let index = manager.append_entry(b"SET test_key test_value".to_vec()).await.unwrap();
        assert_eq!(index, 1);

        manager.commit_entries(1).await.unwrap();
        
        let state_info = manager.get_state().await;
        assert_eq!(state_info.applied_index, 1);
        assert_eq!(state_info.committed_index, 1);
    }

    #[test]
    fn test_distributed_state() {
        let mut state = DistributedState::new();
        
        // Test SET command
        let result = state.apply(b"SET hello world").unwrap();
        assert_eq!(String::from_utf8_lossy(&result), "OK: Set hello");
        
        // Test GET command
        let result = state.apply(b"GET hello").unwrap();
        assert_eq!(result, b"world");
        
        // Test DELETE command
        let result = state.apply(b"DEL hello").unwrap();
        assert_eq!(result, b"DELETED");
        
        // Test GET after delete
        let result = state.apply(b"GET hello").unwrap();
        assert_eq!(result, b"NOT_FOUND");
    }

    #[tokio::test]
    async fn test_snapshot_restore() {
        let state_machine = Box::new(DistributedState::new());
        let manager = StateManager::new(state_machine);

        // Add some entries
        manager.append_entry(b"SET key1 value1".to_vec()).await.unwrap();
        manager.append_entry(b"SET key2 value2".to_vec()).await.unwrap();
        manager.commit_entries(2).await.unwrap();

        // Create snapshot
        let snapshot_index = manager.create_snapshot().await.unwrap();
        assert_eq!(snapshot_index, 2);

        // Restore from snapshot
        manager.restore_snapshot(snapshot_index).await.unwrap();
        
        let state_info = manager.get_state().await;
        assert_eq!(state_info.applied_index, 2);
    }
}