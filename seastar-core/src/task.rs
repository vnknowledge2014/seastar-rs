//! Task system for Seastar-RS
//!
//! Provides abstractions for managing asynchronous tasks within the reactor.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use async_trait::async_trait;


/// Unique identifier for tasks
pub type TaskId = u64;

/// Global task ID counter
pub static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

/// States that a task can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Task is ready to run
    Ready,
    
    /// Task is currently running
    Running,
    
    /// Task has completed execution
    Completed,
    
    /// Task is waiting for more work
    Pending,
    
    /// Task is blocked on I/O or other resources
    Blocked,
    
    /// Task was cancelled
    Cancelled,
}

/// Priority levels for tasks
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Idle = 0,
    Background = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

/// Metadata about a task
#[derive(Debug, Clone)]
pub struct TaskMetadata {
    pub id: TaskId,
    pub name: Option<String>,
    pub priority: TaskPriority,
    pub created_at: Instant,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub total_runtime: Duration,
}

impl TaskMetadata {
    fn new(name: Option<String>, priority: TaskPriority) -> Self {
        Self {
            id: NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst),
            name,
            priority,
            created_at: Instant::now(),
            started_at: None,
            completed_at: None,
            total_runtime: Duration::ZERO,
        }
    }
}

/// A task that can be executed by the reactor
#[async_trait]
pub trait Task: Send {
    /// Execute the task
    async fn execute(&mut self) -> TaskState;
    
    /// Get the task's metadata
    fn metadata(&self) -> &TaskMetadata;
    
    /// Get the task's current state
    fn state(&self) -> TaskState;
    
    /// Cancel the task
    fn cancel(&mut self);
    
    /// Get a handle to this task
    fn handle(&self) -> TaskHandle {
        TaskHandle {
            id: self.metadata().id,
            state: self.state(),
        }
    }
    
    /// Get the task's name
    fn name(&self) -> Option<&str> {
        self.metadata().name.as_deref()
    }
    
    /// Get the task's priority
    fn priority(&self) -> TaskPriority {
        self.metadata().priority
    }
}

/// Handle to a task for monitoring and control
#[derive(Debug, Clone)]
pub struct TaskHandle {
    pub id: TaskId,
    pub state: TaskState,
}

impl TaskHandle {
    /// Check if the task has completed
    pub fn is_completed(&self) -> bool {
        matches!(self.state, TaskState::Completed | TaskState::Cancelled)
    }
    
    /// Check if the task is running
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running)
    }
    
    /// Check if the task was cancelled
    pub fn is_cancelled(&self) -> bool {
        matches!(self.state, TaskState::Cancelled)
    }
}

/// A future-based task implementation
pub struct FutureTask<F> {
    future: Pin<Box<F>>,
    metadata: TaskMetadata,
    state: TaskState,
}

impl<F> FutureTask<F>
where
    F: Future + Send + 'static,
{
    /// Create a new future task
    pub fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
            metadata: TaskMetadata::new(None, TaskPriority::Normal),
            state: TaskState::Ready,
        }
    }
    
    /// Create a new future task with name and priority
    pub fn with_metadata(future: F, name: String, priority: TaskPriority) -> Self {
        Self {
            future: Box::pin(future),
            metadata: TaskMetadata::new(Some(name), priority),
            state: TaskState::Ready,
        }
    }
}

#[async_trait]
impl<F> Task for FutureTask<F>
where
    F: Future + Send,
{
    async fn execute(&mut self) -> TaskState {
        if self.state == TaskState::Cancelled {
            return TaskState::Cancelled;
        }
        
        self.state = TaskState::Running;
        
        if self.metadata.started_at.is_none() {
            self.metadata.started_at = Some(Instant::now());
        }
        
        let start_time = Instant::now();
        
        // Create a no-op waker since we're not using the standard polling mechanism
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        
        // Poll the future
        match self.future.as_mut().poll(&mut context) {
            Poll::Ready(_) => {
                self.state = TaskState::Completed;
                self.metadata.completed_at = Some(Instant::now());
                self.metadata.total_runtime += start_time.elapsed();
                TaskState::Completed
            }
            Poll::Pending => {
                self.state = TaskState::Pending;
                self.metadata.total_runtime += start_time.elapsed();
                TaskState::Pending
            }
        }
    }
    
    fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }
    
    fn state(&self) -> TaskState {
        self.state
    }
    
    fn cancel(&mut self) {
        self.state = TaskState::Cancelled;
        self.metadata.completed_at = Some(Instant::now());
    }
}

/// A simple closure-based task
pub struct ClosureTask<F> {
    closure: Option<F>,
    metadata: TaskMetadata,
    state: TaskState,
}

impl<F> ClosureTask<F>
where
    F: FnOnce() + Send + 'static,
{
    /// Create a new closure task
    pub fn new(closure: F) -> Self {
        Self {
            closure: Some(closure),
            metadata: TaskMetadata::new(None, TaskPriority::Normal),
            state: TaskState::Ready,
        }
    }
    
    /// Create a new closure task with name and priority
    pub fn with_metadata(closure: F, name: String, priority: TaskPriority) -> Self {
        Self {
            closure: Some(closure),
            metadata: TaskMetadata::new(Some(name), priority),
            state: TaskState::Ready,
        }
    }
}

#[async_trait]
impl<F> Task for ClosureTask<F>
where
    F: FnOnce() + Send,
{
    async fn execute(&mut self) -> TaskState {
        if self.state == TaskState::Cancelled {
            return TaskState::Cancelled;
        }
        
        if let Some(closure) = self.closure.take() {
            self.state = TaskState::Running;
            
            if self.metadata.started_at.is_none() {
                self.metadata.started_at = Some(Instant::now());
            }
            
            let start_time = Instant::now();
            closure();
            self.metadata.total_runtime += start_time.elapsed();
            
            self.state = TaskState::Completed;
            self.metadata.completed_at = Some(Instant::now());
        }
        
        TaskState::Completed
    }
    
    fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }
    
    fn state(&self) -> TaskState {
        self.state
    }
    
    fn cancel(&mut self) {
        self.state = TaskState::Cancelled;
        self.metadata.completed_at = Some(Instant::now());
        self.closure.take(); // Drop the closure
    }
}

/// Utility functions for creating tasks

/// Create a task from a future
pub fn make_task<F>(future: F) -> Box<dyn Task>
where
    F: Future + Send + 'static,
{
    Box::new(FutureTask::new(future))
}

/// Create a named task from a future
pub fn make_named_task<F>(future: F, name: String) -> Box<dyn Task>
where
    F: Future + Send + 'static,
{
    Box::new(FutureTask::with_metadata(future, name, TaskPriority::Normal))
}

/// Create a prioritized task from a future
pub fn make_prioritized_task<F>(future: F, name: String, priority: TaskPriority) -> Box<dyn Task>
where
    F: Future + Send + 'static,
{
    Box::new(FutureTask::with_metadata(future, name, priority))
}

/// Create a task from a closure
pub fn make_closure_task<F>(closure: F) -> Box<dyn Task>
where
    F: FnOnce() + Send + 'static,
{
    Box::new(ClosureTask::new(closure))
}

/// Create a named task from a closure
pub fn make_named_closure_task<F>(closure: F, name: String) -> Box<dyn Task>
where
    F: FnOnce() + Send + 'static,
{
    Box::new(ClosureTask::with_metadata(closure, name, TaskPriority::Normal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_future_task() {
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();
        
        let mut task = FutureTask::new(async move {
            executed_clone.store(true, Ordering::SeqCst);
            42
        });
        
        assert_eq!(task.state(), TaskState::Ready);
        
        let result = task.execute().await;
        assert_eq!(result, TaskState::Completed);
        assert!(executed.load(Ordering::SeqCst));
    }
    
    #[tokio::test]
    async fn test_closure_task() {
        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();
        
        let mut task = ClosureTask::new(move || {
            executed_clone.store(true, Ordering::SeqCst);
        });
        
        assert_eq!(task.state(), TaskState::Ready);
        
        let result = task.execute().await;
        assert_eq!(result, TaskState::Completed);
        assert!(executed.load(Ordering::SeqCst));
    }
    
    #[tokio::test]
    async fn test_task_cancellation() {
        let mut task = ClosureTask::new(|| {
            // This should not execute
            panic!("Task should be cancelled");
        });
        
        task.cancel();
        assert_eq!(task.state(), TaskState::Cancelled);
        
        let result = task.execute().await;
        assert_eq!(result, TaskState::Cancelled);
    }
}