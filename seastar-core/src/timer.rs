//! Timer system for Seastar-RS
//!
//! Provides high-resolution timers and scheduling capabilities.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use async_trait::async_trait;

use crate::task::{Task, TaskState, TaskPriority, TaskMetadata, NEXT_TASK_ID};
use crate::Result;

/// Unique identifier for timers
pub type TimerId = u64;

/// Global timer ID counter
static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);

/// A timer that can be scheduled for future execution
pub struct Timer {
    id: TimerId,
    deadline: Instant,
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
    repeat_interval: Option<Duration>,
    state: TimerState,
}

impl std::fmt::Debug for Timer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timer")
            .field("id", &self.id)
            .field("deadline", &self.deadline)
            .field("has_callback", &self.callback.is_some())
            .field("repeat_interval", &self.repeat_interval)
            .field("state", &self.state)
            .finish()
    }
}

/// States that a timer can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerState {
    Pending,
    Fired,
    Cancelled,
}

impl Timer {
    /// Create a new timer that fires once at the specified deadline
    pub fn new<F>(deadline: Instant, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            id: NEXT_TIMER_ID.fetch_add(1, AtomicOrdering::SeqCst),
            deadline,
            callback: Some(Box::new(callback)),
            repeat_interval: None,
            state: TimerState::Pending,
        }
    }

    /// Create a new repeating timer
    pub fn new_repeating<F>(deadline: Instant, interval: Duration, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            id: NEXT_TIMER_ID.fetch_add(1, AtomicOrdering::SeqCst),
            deadline,
            callback: Some(Box::new(callback)),
            repeat_interval: Some(interval),
            state: TimerState::Pending,
        }
    }

    /// Create a timer that fires after a delay from now
    pub fn after<F>(delay: Duration, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self::new(Instant::now() + delay, callback)
    }

    /// Get the timer ID
    pub fn id(&self) -> TimerId {
        self.id
    }

    /// Get the deadline for this timer
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Get the repeat interval if this is a repeating timer
    pub fn repeat_interval(&self) -> Option<Duration> {
        self.repeat_interval
    }

    /// Get the timer state
    pub fn state(&self) -> TimerState {
        self.state
    }

    /// Check if the timer is ready to fire
    pub fn is_ready(&self) -> bool {
        self.state == TimerState::Pending && Instant::now() >= self.deadline
    }

    /// Fire the timer, executing its callback
    pub fn fire(&mut self) {
        if self.state == TimerState::Pending {
            if let Some(callback) = self.callback.take() {
                callback();
                self.state = TimerState::Fired;
            }
        }
    }

    /// Cancel the timer
    pub fn cancel(&mut self) {
        self.state = TimerState::Cancelled;
        self.callback.take(); // Drop the callback
    }

    /// Get a handle to this timer
    pub fn handle(&self) -> TimerHandle {
        TimerHandle {
            id: self.id,
            deadline: self.deadline,
            state: self.state,
        }
    }
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering so BinaryHeap acts as a min-heap
        other.deadline.cmp(&self.deadline)
            .then_with(|| other.id.cmp(&self.id))
    }
}

/// Handle to a timer for monitoring and control
#[derive(Debug, Clone)]
pub struct TimerHandle {
    pub id: TimerId,
    pub deadline: Instant,
    pub state: TimerState,
}

impl TimerHandle {
    /// Check if the timer has fired
    pub fn has_fired(&self) -> bool {
        self.state == TimerState::Fired
    }

    /// Check if the timer was cancelled
    pub fn is_cancelled(&self) -> bool {
        self.state == TimerState::Cancelled
    }

    /// Check if the timer is still pending
    pub fn is_pending(&self) -> bool {
        self.state == TimerState::Pending
    }

    /// Get time remaining until the timer fires
    pub fn time_remaining(&self) -> Duration {
        let now = Instant::now();
        if now >= self.deadline {
            Duration::ZERO
        } else {
            self.deadline - now
        }
    }
}

/// A timer wheel for efficiently managing large numbers of timers
pub struct TimerWheel {
    /// Heap of timers ordered by deadline
    timers: BinaryHeap<Timer>,
    
    /// Statistics
    stats: TimerWheelStats,
}

/// Statistics for the timer wheel
#[derive(Debug, Default)]
struct TimerWheelStats {
    timers_created: u64,
    timers_fired: u64,
    timers_cancelled: u64,
}

impl TimerWheel {
    /// Create a new timer wheel
    pub fn new() -> Self {
        Self {
            timers: BinaryHeap::new(),
            stats: TimerWheelStats::default(),
        }
    }

    /// Add a timer to the wheel
    pub fn add_timer(&mut self, timer: Timer) -> TimerHandle {
        let handle = timer.handle();
        self.timers.push(timer);
        self.stats.timers_created += 1;
        handle
    }

    /// Process expired timers
    pub fn process_expired(&mut self) -> Vec<TimerHandle> {
        let mut fired_timers = Vec::new();
        let now = Instant::now();

        while let Some(mut timer) = self.timers.peek_mut() {
            if timer.deadline <= now && timer.state == TimerState::Pending {
                let handle = timer.handle();
                timer.fire();
                
                // If it's a repeating timer, reschedule it
                if let Some(interval) = timer.repeat_interval {
                    timer.deadline = now + interval;
                    timer.state = TimerState::Pending;
                    timer.callback = None; // Would need to store a repeatable callback
                } else {
                    // Remove non-repeating fired timer
                    std::collections::binary_heap::PeekMut::pop(timer);
                    self.stats.timers_fired += 1;
                }
                
                fired_timers.push(handle);
            } else {
                break;
            }
        }

        // Remove cancelled timers
        self.timers.retain(|timer| timer.state != TimerState::Cancelled);

        fired_timers
    }

    /// Get the time until the next timer fires
    pub fn next_deadline(&self) -> Option<Instant> {
        self.timers
            .iter()
            .filter(|timer| timer.state == TimerState::Pending)
            .map(|timer| timer.deadline)
            .min()
    }

    /// Cancel a timer by ID
    pub fn cancel_timer(&mut self, timer_id: TimerId) -> bool {
        // Convert to vector temporarily to iterate mutably
        let mut timers_vec: Vec<Timer> = self.timers.drain().collect();
        let mut found = false;
        
        for timer in &mut timers_vec {
            if timer.id == timer_id && timer.state == TimerState::Pending {
                timer.cancel();
                self.stats.timers_cancelled += 1;
                found = true;
                break;
            }
        }
        
        // Put timers back into the heap
        for timer in timers_vec {
            self.timers.push(timer);
        }
        
        found
    }

    /// Get the number of active timers
    pub fn active_count(&self) -> usize {
        self.timers
            .iter()
            .filter(|timer| timer.state == TimerState::Pending)
            .count()
    }

    /// Get statistics
    pub fn stats(&self) -> &TimerWheelStats {
        &self.stats
    }
    
    /// Shutdown the timer wheel and cancel all pending timers
    pub async fn shutdown(&mut self) -> Result<()> {
        tracing::info!("Shutting down timer wheel");
        
        let cancelled_count = self.timers.len();
        
        // Cancel all pending timers
        self.timers.clear();
        
        // Update stats
        self.stats.timers_cancelled = self.stats.timers_cancelled + cancelled_count as u64;
        
        tracing::info!("Timer wheel shutdown complete, cancelled {} timers", cancelled_count);
        Ok(())
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

/// A task that sleeps for a specified duration
pub struct SleepTask {
    deadline: Instant,
    metadata: TaskMetadata,
    state: crate::task::TaskState,
}

impl SleepTask {
    /// Create a new sleep task
    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: Instant::now() + duration,
            metadata: TaskMetadata {
                id: NEXT_TASK_ID.fetch_add(1, AtomicOrdering::SeqCst),
                name: Some("sleep".to_string()),
                priority: TaskPriority::Normal,
                created_at: Instant::now(),
                started_at: None,
                completed_at: None,
                total_runtime: Duration::ZERO,
            },
            state: crate::task::TaskState::Ready,
        }
    }

    /// Create a sleep task that sleeps until a specific deadline
    pub fn until(deadline: Instant) -> Self {
        Self {
            deadline,
            metadata: TaskMetadata {
                id: NEXT_TASK_ID.fetch_add(1, AtomicOrdering::SeqCst),
                name: Some("sleep_until".to_string()),
                priority: TaskPriority::Normal,
                created_at: Instant::now(),
                started_at: None,
                completed_at: None,
                total_runtime: Duration::ZERO,
            },
            state: crate::task::TaskState::Ready,
        }
    }
}

#[async_trait]
impl Task for SleepTask {
    async fn execute(&mut self) -> TaskState {
        if self.state == crate::task::TaskState::Cancelled {
            return TaskState::Cancelled;
        }

        self.state = crate::task::TaskState::Running;

        if self.metadata.started_at.is_none() {
            self.metadata.started_at = Some(Instant::now());
        }

        if Instant::now() >= self.deadline {
            self.state = crate::task::TaskState::Completed;
            self.metadata.completed_at = Some(Instant::now());
            TaskState::Completed
        } else {
            self.state = crate::task::TaskState::Pending;
            TaskState::Pending
        }
    }

    fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }

    fn state(&self) -> crate::task::TaskState {
        self.state
    }

    fn cancel(&mut self) {
        self.state = crate::task::TaskState::Cancelled;
        self.metadata.completed_at = Some(Instant::now());
    }
}

/// Utility functions for timers

/// Sleep for the specified duration
pub async fn sleep(duration: Duration) {
    let mut task = SleepTask::new(duration);
    while task.execute().await == TaskState::Pending {
        // In a real implementation, this would yield to the reactor
        tokio::task::yield_now().await;
    }
}

/// Sleep until the specified deadline
pub async fn sleep_until(deadline: Instant) {
    let mut task = SleepTask::until(deadline);
    while task.execute().await == TaskState::Pending {
        // In a real implementation, this would yield to the reactor
        tokio::task::yield_now().await;
    }
}

/// Create a timer that fires after the specified delay
pub fn timer_after<F>(delay: Duration, callback: F) -> Timer
where
    F: FnOnce() + Send + 'static,
{
    Timer::after(delay, callback)
}

/// Create a timer that fires at the specified deadline
pub fn timer_at<F>(deadline: Instant, callback: F) -> Timer
where
    F: FnOnce() + Send + 'static,
{
    Timer::new(deadline, callback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

    #[test]
    fn test_timer_creation() {
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = fired.clone();
        
        let mut timer = Timer::after(Duration::from_millis(1), move || {
            fired_clone.store(true, Ordering::SeqCst);
        });

        assert_eq!(timer.state(), TimerState::Pending);
        assert!(!fired.load(Ordering::SeqCst));

        // Wait a bit and check if timer is ready
        std::thread::sleep(Duration::from_millis(2));
        assert!(timer.is_ready());

        timer.fire();
        assert_eq!(timer.state(), TimerState::Fired);
        assert!(fired.load(Ordering::SeqCst));
    }

    #[test]
    fn test_timer_wheel() {
        let mut wheel = TimerWheel::new();
        
        let fired = Arc::new(AtomicBool::new(false));
        let fired_clone = fired.clone();
        
        let timer = Timer::after(Duration::from_millis(1), move || {
            fired_clone.store(true, Ordering::SeqCst);
        });
        
        let handle = wheel.add_timer(timer);
        assert_eq!(wheel.active_count(), 1);

        // Timer shouldn't have fired yet
        let fired_timers = wheel.process_expired();
        assert!(fired_timers.is_empty());

        // Wait and process again
        std::thread::sleep(Duration::from_millis(2));
        let fired_timers = wheel.process_expired();
        assert_eq!(fired_timers.len(), 1);
        assert!(fired.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_sleep_task() {
        let start = Instant::now();
        sleep(Duration::from_millis(10)).await;
        let elapsed = start.elapsed();
        
        // Should have slept for at least 10ms (with some tolerance)
        assert!(elapsed >= Duration::from_millis(5));
    }
}