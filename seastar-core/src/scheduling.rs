//! Task scheduling and execution control
//!
//! Provides scheduling primitives for controlling task execution order
//! and resource allocation within the reactor.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use crate::task::{Task, TaskPriority};

/// A scheduling group for managing related tasks
#[derive(Debug)]
pub struct SchedulingGroup {
    id: SchedulingGroupId,
    name: String,
    priority: TaskPriority,
    shares: u32,
    max_rate: Option<u64>, // Operations per second
    current_usage: AtomicU64,
    last_reset: std::sync::Mutex<Instant>,
}

/// Unique identifier for scheduling groups
pub type SchedulingGroupId = u64;

static NEXT_SCHEDULING_GROUP_ID: AtomicU64 = AtomicU64::new(1);

impl SchedulingGroup {
    /// Create a new scheduling group
    pub fn new(name: String, priority: TaskPriority, shares: u32) -> Self {
        Self {
            id: NEXT_SCHEDULING_GROUP_ID.fetch_add(1, AtomicOrdering::SeqCst),
            name,
            priority,
            shares,
            max_rate: None,
            current_usage: AtomicU64::new(0),
            last_reset: std::sync::Mutex::new(Instant::now()),
        }
    }
    
    /// Create a new scheduling group with rate limiting
    pub fn with_rate_limit(
        name: String, 
        priority: TaskPriority, 
        shares: u32, 
        max_ops_per_sec: u64
    ) -> Self {
        Self {
            id: NEXT_SCHEDULING_GROUP_ID.fetch_add(1, AtomicOrdering::SeqCst),
            name,
            priority,
            shares,
            max_rate: Some(max_ops_per_sec),
            current_usage: AtomicU64::new(0),
            last_reset: std::sync::Mutex::new(Instant::now()),
        }
    }
    
    /// Get the scheduling group ID
    pub fn id(&self) -> SchedulingGroupId {
        self.id
    }
    
    /// Get the scheduling group name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the priority
    pub fn priority(&self) -> TaskPriority {
        self.priority
    }
    
    /// Get the number of shares
    pub fn shares(&self) -> u32 {
        self.shares
    }
    
    /// Check if this scheduling group can accept more work
    pub fn can_accept_work(&self) -> bool {
        if let Some(max_rate) = self.max_rate {
            let now = Instant::now();
            
            // Reset usage counter if a second has passed
            if let Ok(mut last_reset) = self.last_reset.lock() {
                if now.duration_since(*last_reset) >= Duration::from_secs(1) {
                    self.current_usage.store(0, AtomicOrdering::SeqCst);
                    *last_reset = now;
                }
            }
            
            self.current_usage.load(AtomicOrdering::SeqCst) < max_rate
        } else {
            true
        }
    }
    
    /// Record work being done in this scheduling group
    pub fn record_work(&self, amount: u64) {
        self.current_usage.fetch_add(amount, AtomicOrdering::SeqCst);
    }
    
    /// Get current usage
    pub fn current_usage(&self) -> u64 {
        self.current_usage.load(AtomicOrdering::SeqCst)
    }
}

/// A task queue that maintains priority ordering
pub struct TaskQueue {
    /// Tasks organized by priority
    priority_queues: [VecDeque<Box<dyn Task>>; 5],
    
    /// Total number of tasks
    total_tasks: AtomicUsize,
}

impl TaskQueue {
    /// Create a new task queue
    pub fn new() -> Self {
        Self {
            priority_queues: [
                VecDeque::new(), // Idle
                VecDeque::new(), // Background  
                VecDeque::new(), // Normal
                VecDeque::new(), // High
                VecDeque::new(), // Critical
            ],
            total_tasks: AtomicUsize::new(0),
        }
    }
    
    /// Add a task to the queue
    pub fn push(&mut self, task: Box<dyn Task>) {
        let priority = task.priority() as usize;
        self.priority_queues[priority].push_back(task);
        self.total_tasks.fetch_add(1, AtomicOrdering::SeqCst);
    }
    
    /// Remove and return the highest priority task
    pub fn pop(&mut self) -> Option<Box<dyn Task>> {
        // Start from highest priority and work down
        for queue in self.priority_queues.iter_mut().rev() {
            if let Some(task) = queue.pop_front() {
                self.total_tasks.fetch_sub(1, AtomicOrdering::SeqCst);
                return Some(task);
            }
        }
        None
    }
    
    /// Get the number of tasks in the queue
    pub fn len(&self) -> usize {
        self.total_tasks.load(AtomicOrdering::SeqCst)
    }
    
    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Get the number of tasks at each priority level
    pub fn priority_counts(&self) -> [usize; 5] {
        [
            self.priority_queues[0].len(),
            self.priority_queues[1].len(), 
            self.priority_queues[2].len(),
            self.priority_queues[3].len(),
            self.priority_queues[4].len(),
        ]
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// A scheduler that manages task execution across multiple scheduling groups
pub struct Scheduler {
    /// All scheduling groups
    groups: Vec<SchedulingGroup>,
    
    /// Task queues for each scheduling group
    group_queues: std::collections::HashMap<SchedulingGroupId, TaskQueue>,
    
    /// Current scheduling round
    current_round: u64,
    
    /// Statistics
    stats: SchedulerStats,
}

/// Statistics collected by the scheduler
#[derive(Debug, Default)]
struct SchedulerStats {
    tasks_scheduled: u64,
    groups_processed: u64,
    scheduling_decisions: u64,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            group_queues: std::collections::HashMap::new(),
            current_round: 0,
            stats: SchedulerStats::default(),
        }
    }
    
    /// Add a scheduling group
    pub fn add_group(&mut self, group: SchedulingGroup) -> SchedulingGroupId {
        let id = group.id();
        self.group_queues.insert(id, TaskQueue::new());
        self.groups.push(group);
        id
    }
    
    /// Submit a task to a specific scheduling group
    pub fn submit_to_group(&mut self, group_id: SchedulingGroupId, task: Box<dyn Task>) -> Result<(), Box<dyn Task>> {
        if let Some(queue) = self.group_queues.get_mut(&group_id) {
            queue.push(task);
            self.stats.tasks_scheduled += 1;
            Ok(())
        } else {
            Err(task)
        }
    }
    
    /// Get the next task to execute using fair scheduling
    pub fn next_task(&mut self) -> Option<(SchedulingGroupId, Box<dyn Task>)> {
        self.current_round += 1;
        self.stats.scheduling_decisions += 1;
        
        // Find the scheduling group with the highest priority that has work
        // and can accept more work
        let mut best_group = None;
        let mut best_priority = TaskPriority::Idle;
        
        for group in &self.groups {
            if group.can_accept_work() {
                if let Some(queue) = self.group_queues.get(&group.id()) {
                    if !queue.is_empty() && group.priority() > best_priority {
                        best_group = Some(group.id());
                        best_priority = group.priority();
                    }
                }
            }
        }
        
        // Get a task from the best group
        if let Some(group_id) = best_group {
            if let Some(queue) = self.group_queues.get_mut(&group_id) {
                if let Some(task) = queue.pop() {
                    // Record work in the scheduling group
                    if let Some(group) = self.groups.iter().find(|g| g.id() == group_id) {
                        group.record_work(1);
                    }
                    
                    self.stats.groups_processed += 1;
                    return Some((group_id, task));
                }
            }
        }
        
        None
    }
    
    /// Get statistics about the scheduler
    pub fn stats(&self) -> &SchedulerStats {
        &self.stats
    }
    
    /// Get information about all scheduling groups
    pub fn group_info(&self) -> Vec<SchedulingGroupInfo> {
        self.groups
            .iter()
            .map(|group| {
                let queue_size = self
                    .group_queues
                    .get(&group.id())
                    .map(|q| q.len())
                    .unwrap_or(0);
                
                SchedulingGroupInfo {
                    id: group.id(),
                    name: group.name().to_string(),
                    priority: group.priority(),
                    shares: group.shares(),
                    current_usage: group.current_usage(),
                    queue_size,
                }
            })
            .collect()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a scheduling group
#[derive(Debug, Clone)]
pub struct SchedulingGroupInfo {
    pub id: SchedulingGroupId,
    pub name: String,
    pub priority: TaskPriority,
    pub shares: u32,
    pub current_usage: u64,
    pub queue_size: usize,
}

/// Default scheduling groups
pub mod default_groups {
    use super::*;
    
    /// Create the default background scheduling group
    pub fn background() -> SchedulingGroup {
        SchedulingGroup::new(
            "background".to_string(),
            TaskPriority::Background,
            100,
        )
    }
    
    /// Create the default main scheduling group
    pub fn main() -> SchedulingGroup {
        SchedulingGroup::new(
            "main".to_string(),
            TaskPriority::Normal,
            1000,
        )
    }
    
    /// Create a scheduling group for maintenance tasks
    pub fn maintenance() -> SchedulingGroup {
        SchedulingGroup::with_rate_limit(
            "maintenance".to_string(),
            TaskPriority::Background,
            50,
            100, // Max 100 ops per second
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::make_closure_task;
    
    #[test]
    fn test_scheduling_group_rate_limit() {
        let group = SchedulingGroup::with_rate_limit(
            "test".to_string(),
            TaskPriority::Normal,
            100,
            10
        );
        
        assert!(group.can_accept_work());
        
        // Use up the rate limit
        for _ in 0..10 {
            group.record_work(1);
        }
        
        assert!(!group.can_accept_work());
    }
    
    #[test]
    fn test_task_queue_priority() {
        let mut queue = TaskQueue::new();
        
        let low_task = make_closure_task(|| {});
        let high_task = crate::task::make_prioritized_task(
            async {}, 
            "high".to_string(), 
            TaskPriority::High
        );
        
        queue.push(low_task);
        queue.push(high_task);
        
        // Should get high priority task first
        let task = queue.pop().unwrap();
        assert_eq!(task.priority(), TaskPriority::High);
    }
    
    #[test]
    fn test_scheduler_basic() {
        let mut scheduler = Scheduler::new();
        
        let group = SchedulingGroup::new(
            "test".to_string(),
            TaskPriority::Normal,
            100
        );
        let group_id = scheduler.add_group(group);
        
        let task = make_closure_task(|| {});
        scheduler.submit_to_group(group_id, task).map_err(|_| "Failed to submit task").unwrap();
        
        let (returned_group_id, _task) = scheduler.next_task().unwrap();
        assert_eq!(returned_group_id, group_id);
    }
}