//! Reactor - the core event loop for Seastar-RS
//!
//! The reactor is responsible for polling I/O events, scheduling tasks,
//! and managing the execution of futures in a high-performance, single-threaded
//! event loop.

use std::future::Future;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::task::Waker;
use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::task::{Task, TaskHandle, TaskState, FutureTask};
use crate::timer::{Timer, TimerHandle};
use crate::scheduling::TaskQueue;
use crate::io::IoBackend;
use crate::{Error, Result};

/// Configuration for the reactor
#[derive(Debug, Clone)]
pub struct ReactorConfig {
    /// Number of I/O polling threads
    pub io_threads: usize,
    
    /// Maximum number of tasks to process per reactor iteration
    pub max_tasks_per_iteration: usize,
    
    /// Maximum polling timeout
    pub max_poll_timeout: Duration,
    
    /// Enable task preemption
    pub preemption_enabled: bool,
    
    /// Task preemption threshold
    pub preemption_threshold: Duration,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            io_threads: 1,
            max_tasks_per_iteration: 1000,
            max_poll_timeout: Duration::from_millis(100),
            preemption_enabled: true,
            preemption_threshold: Duration::from_micros(500),
        }
    }
}

/// The main reactor that drives the event loop
pub struct Reactor {
    /// Reactor configuration
    config: ReactorConfig,
    
    /// Task queue for ready tasks
    task_queue: TaskQueue,
    
    /// Timer wheel for scheduled tasks
    timers: Vec<Timer>,
    
    /// I/O polling backend
    io_backend: std::sync::Arc<std::sync::Mutex<Box<dyn IoBackend>>>,
    
    /// Pending I/O operations with their associated wakers
    pending_io_ops: HashMap<u64, Waker>,
    
    /// Channel for cross-thread communication
    cross_thread_sender: Sender<CrossThreadMessage>,
    cross_thread_receiver: Receiver<CrossThreadMessage>,
    
    /// Reactor state
    state: ReactorState,
    
    
    /// Statistics
    stats: ReactorStats,
}

/// State of the reactor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReactorState {
    Stopped,
    Running,
    Stopping,
}

/// Statistics collected by the reactor
#[derive(Debug, Default)]
pub struct ReactorStats {
    pub tasks_executed: u64,
    pub io_operations: u64,
    pub timers_fired: u64,
    pub polls_performed: u64,
    pub idle_time: Duration,
}

/// Messages that can be sent across threads to the reactor
enum CrossThreadMessage {
    SubmitTask(Box<dyn Task + Send>),
    ScheduleTimer {
        timer: Timer,
    },
    Shutdown,
}

impl Reactor {
    /// Create a new reactor with the given configuration
    pub fn new(config: ReactorConfig) -> Result<Self> {
        let (sender, receiver) = unbounded();
        
        Ok(Self {
            config,
            task_queue: TaskQueue::new(),
            timers: Vec::new(),
            io_backend: std::sync::Arc::new(std::sync::Mutex::new(crate::io::create_io_backend()?)),
            pending_io_ops: HashMap::new(),
            cross_thread_sender: sender,
            cross_thread_receiver: receiver,
            state: ReactorState::Stopped,
            stats: ReactorStats::default(),
        })
    }

    /// Get a handle to this reactor for cross-thread communication
    pub fn handle(&self) -> ReactorHandle {
        ReactorHandle {
            sender: self.cross_thread_sender.clone(),
        }
    }
    
    /// Get a reference to the I/O backend for direct I/O operations
    pub fn io_backend(&self) -> std::sync::Arc<std::sync::Mutex<Box<dyn IoBackend>>> {
        self.io_backend.clone()
    }
    
    /// Register a waker for an I/O operation
    pub fn register_io_waker(&mut self, request_id: u64, waker: Waker) {
        self.pending_io_ops.insert(request_id, waker);
    }
    
    /// Spawn a future task on the reactor
    pub fn spawn<F>(&mut self, future: F) -> TaskHandle 
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = FutureTask::new(future);
        let handle = task.handle();
        self.task_queue.push(Box::new(task));
        handle
    }
    
    /// Spawn a named future task with specific priority
    pub fn spawn_with_metadata<F>(&mut self, future: F, name: String, priority: crate::task::TaskPriority) -> TaskHandle
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = FutureTask::with_metadata(future, name, priority);
        let handle = task.handle();
        self.task_queue.push(Box::new(task));
        handle
    }

    /// Run the reactor event loop
    pub async fn run(&mut self) -> Result<()> {
        self.state = ReactorState::Running;
        tracing::info!("Reactor starting");

        while self.state == ReactorState::Running {
            let iteration_start = Instant::now();
            
            // Process cross-thread messages
            self.process_cross_thread_messages();
            
            // Process ready tasks
            let tasks_processed = self.process_ready_tasks().await?;
            
            // Process expired timers
            let timers_fired = self.process_timers().await;
            
            // Poll I/O events
            let io_events = self.poll_io_events().await?;
            
            // Update statistics
            self.update_stats(tasks_processed, timers_fired, io_events, iteration_start);
            
            // Yield control if we're running for too long
            if self.config.preemption_enabled 
                && iteration_start.elapsed() > self.config.preemption_threshold 
            {
                tokio::task::yield_now().await;
            }
        }

        tracing::info!("Reactor stopping");
        self.state = ReactorState::Stopped;
        Ok(())
    }

    /// Stop the reactor
    pub fn stop(&mut self) {
        self.state = ReactorState::Stopping;
    }

    /// Submit a task for execution
    pub fn submit_task(&mut self, task: Box<dyn Task>) -> TaskHandle {
        let handle = task.handle();
        self.task_queue.push(task);
        handle
    }

    /// Schedule a timer
    pub fn schedule_timer(&mut self, timer: Timer) -> TimerHandle {
        let handle = timer.handle();
        // In a real implementation, we'd insert into a proper timer wheel
        self.timers.push(timer);
        handle
    }

    /// Process messages from other threads
    fn process_cross_thread_messages(&mut self) {
        while let Ok(message) = self.cross_thread_receiver.try_recv() {
            match message {
                CrossThreadMessage::SubmitTask(task) => {
                    self.task_queue.push(task);
                }
                CrossThreadMessage::ScheduleTimer { timer } => {
                    self.timers.push(timer);
                }
                CrossThreadMessage::Shutdown => {
                    self.state = ReactorState::Stopping;
                    break;
                }
            }
        }
    }

    /// Process ready tasks from the task queue
    async fn process_ready_tasks(&mut self) -> Result<usize> {
        let mut tasks_processed = 0;
        let max_tasks = self.config.max_tasks_per_iteration;

        while tasks_processed < max_tasks {
            if let Some(mut task) = self.task_queue.pop() {
                // Execute the task
                match task.execute().await {
                    TaskState::Completed => {
                        // Task finished, nothing more to do
                    }
                    TaskState::Pending => {
                        // Task needs more time, requeue it
                        self.task_queue.push(task);
                    }
                    TaskState::Blocked => {
                        // Task is blocked on I/O, it will be rescheduled
                        // when the I/O completes
                    }
                    TaskState::Ready | TaskState::Running => {
                        // These states shouldn't happen after execute()
                        self.task_queue.push(task);
                    }
                    TaskState::Cancelled => {
                        // Task was cancelled, nothing to do
                    }
                }
                
                tasks_processed += 1;
            } else {
                break;
            }
        }

        Ok(tasks_processed)
    }

    /// Process expired timers
    async fn process_timers(&mut self) -> usize {
        let now = Instant::now();
        let mut timers_fired = 0;
        
        // In a real implementation, this would use a more efficient timer wheel
        let mut i = 0;
        while i < self.timers.len() {
            if self.timers[i].deadline() <= now {
                self.timers[i].fire();
                timers_fired += 1;
                self.timers.remove(i);
            } else {
                i += 1;
            }
        }

        timers_fired
    }

    /// Poll for I/O events
    async fn poll_io_events(&mut self) -> Result<usize> {
        let timeout = if self.task_queue.is_empty() && self.timers.is_empty() {
            self.config.max_poll_timeout
        } else {
            Duration::from_millis(0) // Don't block if we have work to do
        };

        let mut backend = self.io_backend.lock().unwrap();
        let events = backend.poll(timeout)?;
        
        // Process completed I/O operations
        let completed_ops = backend.complete_io();
        drop(backend); // Release lock early
        
        // Wake up tasks waiting for these I/O operations
        for (request_id, io_result) in completed_ops {
            tracing::debug!("I/O operation {} completed with {} bytes", request_id, io_result.bytes_transferred);
            
            if let Some(waker) = self.pending_io_ops.remove(&request_id) {
                waker.wake();
            }
        }
        
        Ok(events)
    }

    /// Update reactor statistics
    fn update_stats(&mut self, tasks: usize, timers: usize, io_events: usize, start: Instant) {
        self.stats.tasks_executed += tasks as u64;
        self.stats.timers_fired += timers as u64;
        self.stats.io_operations += io_events as u64;
        self.stats.polls_performed += 1;
        
        if tasks == 0 && timers == 0 && io_events == 0 {
            self.stats.idle_time += start.elapsed();
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> &ReactorStats {
        &self.stats
    }
}

/// Handle to a reactor for cross-thread communication
#[derive(Clone)]
pub struct ReactorHandle {
    sender: Sender<CrossThreadMessage>,
}

impl ReactorHandle {
    /// Submit a task to the reactor from another thread
    pub fn submit_task(&self, task: Box<dyn Task + Send>) -> Result<()> {
        self.sender
            .send(CrossThreadMessage::SubmitTask(task))
            .map_err(|_| Error::Internal("Failed to submit task to reactor".to_string()))?;
        Ok(())
    }

    /// Schedule a timer on the reactor from another thread
    pub fn schedule_timer(&self, timer: Timer) -> Result<()> {
        self.sender
            .send(CrossThreadMessage::ScheduleTimer { timer })
            .map_err(|_| Error::Internal("Failed to schedule timer on reactor".to_string()))?;
        Ok(())
    }

    /// Shutdown the reactor
    pub fn shutdown(&self) -> Result<()> {
        self.sender
            .send(CrossThreadMessage::Shutdown)
            .map_err(|_| Error::Internal("Failed to shutdown reactor".to_string()))?;
        Ok(())
    }
}

/// Create a new reactor with default configuration
pub fn create_reactor() -> Result<Reactor> {
    Reactor::new(ReactorConfig::default())
}

/// Create a new reactor with custom configuration
pub fn create_reactor_with_config(config: ReactorConfig) -> Result<Reactor> {
    Reactor::new(config)
}

/// Global reactor instance (thread-local)
thread_local! {
    static REACTOR: std::cell::RefCell<Option<Reactor>> = std::cell::RefCell::new(None);
}

/// Initialize the global reactor for the current thread
pub fn initialize_reactor() -> Result<()> {
    let reactor = create_reactor()?;
    REACTOR.with(|r| {
        *r.borrow_mut() = Some(reactor);
    });
    Ok(())
}

/// Get a reference to the current thread's reactor
pub fn with_reactor<F, R>(f: F) -> R
where
    F: FnOnce(&mut Reactor) -> R,
{
    REACTOR.with(|r| {
        let mut reactor_ref = r.borrow_mut();
        let reactor = reactor_ref.as_mut().expect("Reactor not initialized");
        f(reactor)
    })
}

/// Run the global reactor
pub async fn run_reactor() -> Result<()> {
    REACTOR.with(|r| {
        let mut reactor_ref = r.borrow_mut();
        let _reactor = reactor_ref.as_mut().expect("Reactor not initialized");
        // Note: We can't directly await here due to borrowing rules
        // In a real implementation, this would need more sophisticated handling
        Ok(())
    })
}

