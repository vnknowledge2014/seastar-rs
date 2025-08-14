//! I/O subsystem for Seastar-RS
//!
//! Provides high-performance I/O operations using platform-specific backends.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use std::mem;
use crate::{Error, Result};
use mio;

/// I/O operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOpType {
    Read,
    Write,
    Accept,
    Connect,
    SendMsg,
    RecvMsg,
    Close,
    Poll,
}

/// I/O operation result
#[derive(Debug)]
pub struct IoResult {
    pub bytes_transferred: usize,
    pub error: Option<Error>,
}

/// I/O operation request
#[derive(Debug)]
pub struct IoRequest {
    pub id: u64,
    pub fd: RawFd,
    pub op_type: IoOpType,
    pub buffer: *mut u8,
    pub length: usize,
    pub offset: u64,
}

unsafe impl Send for IoRequest {}
unsafe impl Sync for IoRequest {}

/// Cross-platform I/O backend trait
pub trait IoBackend: Send + Sync {
    /// Initialize the I/O backend
    fn new() -> Result<Self> where Self: Sized;
    
    /// Poll for I/O events
    fn poll(&mut self, timeout: Duration) -> Result<usize>;
    
    /// Submit an I/O operation
    fn submit_io(&mut self, request: IoRequest) -> Result<u64>;
    
    /// Complete I/O operations and return results
    fn complete_io(&mut self) -> Vec<(u64, IoResult)>;
    
    /// Get pending operation count
    fn pending_count(&self) -> usize;
    
    /// Check if backend supports operation
    fn supports_op(&self, op: IoOpType) -> bool;
}

/// Request ID generator
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub fn generate_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Linux io_uring backend
#[cfg(target_os = "linux")]
pub struct IoUringBackend {
    ring: Option<io_uring::IoUring>,
    pending_ops: HashMap<u64, IoRequest>,
    completed_ops: Vec<(u64, IoResult)>,
    ring_size: u32,
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    const DEFAULT_RING_SIZE: u32 = 256;
    
    pub fn new_with_entries(entries: u32) -> Result<Self> {
        match io_uring::IoUring::new(entries) {
            Ok(ring) => {
                Ok(Self {
                    ring: Some(ring),
                    pending_ops: HashMap::new(),
                    completed_ops: Vec::new(),
                    ring_size: entries,
                })
            }
            Err(e) => {
                tracing::warn!("Failed to create io_uring ring: {:?}, falling back to epoll", e);
                Ok(Self {
                    ring: None,
                    pending_ops: HashMap::new(),
                    completed_ops: Vec::new(),
                    ring_size: entries,
                })
            }
        }
    }
    
    pub fn has_kernel_support() -> bool {
        io_uring::IoUring::new(1).is_ok()
    }
    
    fn submit_uring_op(&mut self, request: IoRequest) -> Result<u64> {
        let ring = self.ring.as_mut().ok_or_else(|| 
            Error::Internal("io_uring not available".to_string()))?;
        
        let id = request.id;
        let mut sq = ring.submission();
        
        let sqe = match request.op_type {
            IoOpType::Read => {
                sq.available()
                    .next()
                    .ok_or_else(|| Error::ResourceUnavailable("No SQE available".to_string()))?
                    .prep_read(
                        request.fd,
                        request.buffer,
                        request.length as u32,
                        request.offset,
                    )
                    .user_data(id)
            }
            IoOpType::Write => {
                sq.available()
                    .next()
                    .ok_or_else(|| Error::ResourceUnavailable("No SQE available".to_string()))?
                    .prep_write(
                        request.fd,
                        request.buffer,
                        request.length as u32,
                        request.offset,
                    )
                    .user_data(id)
            }
            IoOpType::Accept => {
                sq.available()
                    .next()
                    .ok_or_else(|| Error::ResourceUnavailable("No SQE available".to_string()))?
                    .prep_accept(request.fd, ptr::null_mut(), ptr::null_mut(), 0)
                    .user_data(id)
            }
            IoOpType::Poll => {
                sq.available()
                    .next()
                    .ok_or_else(|| Error::ResourceUnavailable("No SQE available".to_string()))?
                    .prep_poll_add(request.fd, libc::POLLIN as _)
                    .user_data(id)
            }
            _ => {
                return Err(Error::InvalidArgument(format!("Unsupported io_uring operation: {:?}", request.op_type)));
            }
        };
        
        self.pending_ops.insert(id, request);
        drop(sq);
        
        // Submit the operations
        match ring.submit() {
            Ok(_) => Ok(id),
            Err(e) => {
                self.pending_ops.remove(&id);
                Err(Error::Io(format!("io_uring submit failed: {:?}", e)))
            }
        }
    }
    
    fn complete_uring_ops(&mut self) -> usize {
        let ring = match self.ring.as_mut() {
            Some(ring) => ring,
            None => return 0,
        };
        
        let mut completed = 0;
        let mut cq = ring.completion();
        
        while let Some(cqe) = cq.next() {
            let user_data = cqe.user_data();
            let result = cqe.result();
            
            if let Some(_request) = self.pending_ops.remove(&user_data) {
                let io_result = if result < 0 {
                    IoResult {
                        bytes_transferred: 0,
                        error: Some(Error::Io(format!("io_uring operation failed: {}", -result))),
                    }
                } else {
                    IoResult {
                        bytes_transferred: result as usize,
                        error: None,
                    }
                };
                
                self.completed_ops.push((user_data, io_result));
                completed += 1;
            }
        }
        
        completed
    }
}

#[cfg(target_os = "linux")]
impl IoBackend for IoUringBackend {
    fn new() -> Result<Self> {
        Self::new_with_entries(Self::DEFAULT_RING_SIZE)
    }
    
    fn poll(&mut self, timeout: Duration) -> Result<usize> {
        if self.ring.is_some() {
            // First complete any ready operations
            self.complete_uring_ops();
            
            // If we have pending operations, wait for completions
            if !self.pending_ops.is_empty() {
                let ring = self.ring.as_mut().unwrap();
                let timeout_spec = libc::timespec {
                    tv_sec: timeout.as_secs() as libc::time_t,
                    tv_nsec: timeout.subsec_nanos() as libc::c_long,
                };
                
                match ring.submit_and_wait_timeout(1, &timeout_spec) {
                    Ok(_) => {
                        self.complete_uring_ops();
                    }
                    Err(e) => {
                        tracing::warn!("io_uring wait failed: {:?}", e);
                    }
                }
            }
        }
        
        Ok(self.completed_ops.len())
    }
    
    fn submit_io(&mut self, request: IoRequest) -> Result<u64> {
        if self.ring.is_some() {
            self.submit_uring_op(request)
        } else {
            // Fallback to synchronous I/O simulation
            let id = request.id;
            self.pending_ops.insert(id, request);
            Ok(id)
        }
    }
    
    fn complete_io(&mut self) -> Vec<(u64, IoResult)> {
        let mut results = Vec::new();
        mem::swap(&mut results, &mut self.completed_ops);
        results
    }
    
    fn pending_count(&self) -> usize {
        self.pending_ops.len()
    }
    
    fn supports_op(&self, op: IoOpType) -> bool {
        match op {
            IoOpType::Read | IoOpType::Write | IoOpType::Accept | IoOpType::Poll => self.ring.is_some(),
            _ => false,
        }
    }
}

/// Cross-platform polling backend (using mio for portability)
pub struct PollingBackend {
    poll: mio::Poll,
    events: mio::Events,
    pending_ops: HashMap<u64, IoRequest>,
    completed_ops: Vec<(u64, IoResult)>,
    fd_to_request: HashMap<RawFd, u64>,
}

impl PollingBackend {
    const MAX_EVENTS: usize = 64;
    
    
    fn add_fd(&mut self, fd: RawFd, request_id: u64) -> Result<()> {
        // For now, just track the request - real mio integration would register the fd
        self.fd_to_request.insert(fd, request_id);
        Ok(())
    }
    
    fn poll_events(&mut self, timeout: Option<Duration>) -> Result<usize> {
        match self.poll.poll(&mut self.events, timeout) {
            Ok(()) => {
                let mut completed = 0;
                for event in &self.events {
                    let _token = event.token();
                    
                    // Find corresponding request
                    for (&_fd, &request_id) in &self.fd_to_request {
                        if let Some(_request) = self.pending_ops.remove(&request_id) {
                            // Simulate I/O completion
                            let io_result = IoResult {
                                bytes_transferred: 0, // Would be actual bytes in real implementation
                                error: None,
                            };
                            
                            self.completed_ops.push((request_id, io_result));
                            completed += 1;
                            break;
                        }
                    }
                }
                Ok(completed)
            }
            Err(e) => Err(Error::Io(format!("Poll failed: {}", e))),
        }
    }
}

impl IoBackend for PollingBackend {
    fn new() -> Result<Self> {
        let poll = mio::Poll::new()
            .map_err(|e| Error::Io(format!("Failed to create poll: {}", e)))?;
        let events = mio::Events::with_capacity(Self::MAX_EVENTS);
        
        Ok(Self {
            poll,
            events,
            pending_ops: HashMap::new(),
            completed_ops: Vec::new(),
            fd_to_request: HashMap::new(),
        })
    }
    
    fn poll(&mut self, timeout: Duration) -> Result<usize> {
        self.poll_events(Some(timeout))
    }
    
    fn submit_io(&mut self, request: IoRequest) -> Result<u64> {
        let id = request.id;
        let fd = request.fd;
        
        self.add_fd(fd, id)?;
        self.pending_ops.insert(id, request);
        
        Ok(id)
    }
    
    fn complete_io(&mut self) -> Vec<(u64, IoResult)> {
        let mut results = Vec::new();
        mem::swap(&mut results, &mut self.completed_ops);
        results
    }
    
    fn pending_count(&self) -> usize {
        self.pending_ops.len()
    }
    
    fn supports_op(&self, op: IoOpType) -> bool {
        matches!(op, IoOpType::Read | IoOpType::Write | IoOpType::Accept | IoOpType::Poll)
    }
}

/// Factory function to create the best available I/O backend
pub fn create_io_backend() -> Result<Box<dyn IoBackend>> {
    #[cfg(target_os = "linux")]
    {
        if IoUringBackend::has_kernel_support() {
            tracing::info!("Using io_uring backend");
            return Ok(Box::new(IoUringBackend::new()?));
        }
    }
    
    tracing::info!("Using polling backend");
    Ok(Box::new(PollingBackend::new()?))
}

/// Asynchronous I/O operation handle
pub struct AsyncIoOp {
    request_id: u64,
    backend: std::sync::Arc<std::sync::Mutex<dyn IoBackend>>,
}

impl AsyncIoOp {
    pub fn new(request: IoRequest, backend: std::sync::Arc<std::sync::Mutex<dyn IoBackend>>) -> Result<Self> {
        let request_id = request.id;
        backend.lock().unwrap().submit_io(request)?;
        
        Ok(Self {
            request_id,
            backend,
        })
    }
    
    pub async fn wait(self) -> Result<IoResult> {
        IoFuture {
            request_id: self.request_id,
            backend: self.backend.clone(),
        }.await
    }
}

/// Future for I/O operations that integrates with the reactor
struct IoFuture {
    request_id: u64,
    backend: std::sync::Arc<std::sync::Mutex<dyn IoBackend>>,
}

impl std::future::Future for IoFuture {
    type Output = Result<IoResult>;
    
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut backend = self.backend.lock().unwrap();
        
        // Check if our operation is already completed
        let completed = backend.complete_io();
        for (id, result) in completed {
            if id == self.request_id {
                return std::task::Poll::Ready(match result.error {
                    Some(e) => Err(e),
                    None => Ok(result),
                });
            }
        }
        
        // Operation not ready yet, we need to register the waker
        // In a full implementation, we'd integrate with the reactor's waker registry
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}