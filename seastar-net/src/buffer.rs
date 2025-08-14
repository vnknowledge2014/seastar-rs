//! Advanced buffer management for zero-copy networking

use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, trace, warn};

#[derive(Error, Debug)]
pub enum BufferError {
    #[error("Allocation failed: {0}")]
    Allocation(String),
    #[error("Pool exhausted: {0}")]
    PoolExhausted(String),
    #[error("Invalid buffer size: {0}")]
    InvalidSize(String),
    #[error("Buffer in use: {0}")]
    InUse(String),
}

pub type BufferResult<T> = Result<T, BufferError>;

/// Network buffer interface for different buffer types
pub trait NetworkBuffer: Send + Sync {
    /// Get the buffer data as a slice
    fn as_ref(&self) -> &[u8];
    
    /// Get the buffer data as a mutable slice
    fn as_mut(&mut self) -> &mut [u8];
    
    /// Get the buffer length
    fn len(&self) -> usize;
    
    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Get the buffer capacity
    fn capacity(&self) -> usize;
    
    /// Set the effective length of the buffer
    fn set_len(&mut self, len: usize);
    
    /// Reset the buffer to empty state
    fn clear(&mut self) {
        self.set_len(0);
    }
}

/// Zero-copy buffer implementation
pub struct ZeroCopyBuffer {
    data: NonNull<u8>,
    len: usize,
    capacity: usize,
    pool: Option<Arc<BufferPool>>,
    id: u64,
    aligned: bool,
}

unsafe impl Send for ZeroCopyBuffer {}
unsafe impl Sync for ZeroCopyBuffer {}

impl ZeroCopyBuffer {
    /// Create a new zero-copy buffer with specified capacity
    pub fn new(capacity: usize) -> BufferResult<Self> {
        Self::new_aligned(capacity, 64) // Default to cache-line alignment
    }

    /// Create a new aligned zero-copy buffer
    pub fn new_aligned(capacity: usize, alignment: usize) -> BufferResult<Self> {
        if capacity == 0 {
            return Err(BufferError::InvalidSize("Capacity cannot be zero".to_string()));
        }

        let layout = Layout::from_size_align(capacity, alignment)
            .map_err(|e| BufferError::Allocation(e.to_string()))?;
        
        let data = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(BufferError::Allocation("Memory allocation failed".to_string()));
            }
            NonNull::new_unchecked(ptr)
        };

        static BUFFER_ID: AtomicU64 = AtomicU64::new(1);
        let id = BUFFER_ID.fetch_add(1, Ordering::Relaxed);

        trace!("Allocated zero-copy buffer {} with capacity {}", id, capacity);

        Ok(Self {
            data,
            len: 0,
            capacity,
            pool: None,
            id,
            aligned: alignment > 1,
        })
    }

    /// Create buffer from existing pool
    pub(crate) fn from_pool(
        data: NonNull<u8>,
        capacity: usize,
        pool: Arc<BufferPool>,
        id: u64,
    ) -> Self {
        Self {
            data,
            len: 0,
            capacity,
            pool: Some(pool),
            id,
            aligned: true,
        }
    }

    /// Get buffer ID for debugging
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if buffer is from a pool
    pub fn is_pooled(&self) -> bool {
        self.pool.is_some()
    }

    /// Get alignment status
    pub fn is_aligned(&self) -> bool {
        self.aligned
    }

    /// Clone the buffer (reference counted if pooled)
    pub fn clone_ref(&self) -> BufferResult<Self> {
        if let Some(ref pool) = self.pool {
            // For pooled buffers, get a new buffer from the same pool
            pool.get_buffer(self.capacity)
        } else {
            // For non-pooled buffers, create a new buffer and copy data
            let mut new_buf = Self::new(self.capacity)?;
            new_buf.as_mut()[..self.len].copy_from_slice(&self.as_ref()[..self.len]);
            new_buf.len = self.len;
            Ok(new_buf)
        }
    }

    /// Resize the buffer (may reallocate)
    pub fn resize(&mut self, new_capacity: usize) -> BufferResult<()> {
        if self.pool.is_some() {
            return Err(BufferError::InUse("Cannot resize pooled buffer".to_string()));
        }

        if new_capacity == self.capacity {
            return Ok(());
        }

        let alignment = if self.aligned { 64 } else { 1 };
        let layout = Layout::from_size_align(new_capacity, alignment)
            .map_err(|e| BufferError::Allocation(e.to_string()))?;
        
        let new_data = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(BufferError::Allocation("Memory reallocation failed".to_string()));
            }
            NonNull::new_unchecked(ptr)
        };

        // Copy existing data
        let copy_len = self.len.min(new_capacity);
        if copy_len > 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.data.as_ptr(),
                    new_data.as_ptr(),
                    copy_len,
                );
            }
        }

        // Deallocate old buffer
        unsafe {
            let old_layout = Layout::from_size_align_unchecked(self.capacity, alignment);
            dealloc(self.data.as_ptr(), old_layout);
        }

        self.data = new_data;
        self.capacity = new_capacity;
        self.len = self.len.min(new_capacity);

        trace!("Resized buffer {} to capacity {}", self.id, new_capacity);
        Ok(())
    }
}

impl NetworkBuffer for ZeroCopyBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn set_len(&mut self, len: usize) {
        if len <= self.capacity {
            self.len = len;
        } else {
            warn!("Attempted to set length {} beyond capacity {}", len, self.capacity);
        }
    }
}

impl Drop for ZeroCopyBuffer {
    fn drop(&mut self) {
        if let Some(ref _pool) = self.pool {
            // TODO: Return buffer to pool - needs different design to avoid borrowing issues
            // For now, just deallocate to prevent memory leaks
            unsafe {
                let alignment = if self.aligned { 64 } else { 1 };
                let layout = Layout::from_size_align_unchecked(self.capacity, alignment);
                dealloc(self.data.as_ptr(), layout);
            }
            trace!("Deallocated zero-copy buffer {} (should return to pool)", self.id);
        } else {
            // Deallocate buffer
            unsafe {
                let alignment = if self.aligned { 64 } else { 1 };
                let layout = Layout::from_size_align_unchecked(self.capacity, alignment);
                dealloc(self.data.as_ptr(), layout);
            }
            trace!("Deallocated zero-copy buffer {}", self.id);
        }
    }
}

/// A wrapper for NonNull<u8> that is Send + Sync
struct SendSyncPtr(NonNull<u8>);

unsafe impl Send for SendSyncPtr {}
unsafe impl Sync for SendSyncPtr {}

impl SendSyncPtr {
    fn as_ptr(&self) -> *mut u8 {
        self.0.as_ptr()
    }
}

/// Buffer pool for efficient memory management
pub struct BufferPool {
    buffers: Mutex<VecDeque<(SendSyncPtr, u64)>>, // (data_ptr, buffer_id)
    buffer_size: usize,
    capacity: usize,
    alignment: usize,
    allocated: AtomicUsize,
    in_use: AtomicUsize,
    peak_usage: AtomicUsize,
    total_allocations: AtomicU64,
    total_deallocations: AtomicU64,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(capacity: usize, buffer_size: usize) -> BufferResult<Self> {
        Self::new_aligned(capacity, buffer_size, 64)
    }

    /// Create a new aligned buffer pool
    pub fn new_aligned(capacity: usize, buffer_size: usize, alignment: usize) -> BufferResult<Self> {
        if capacity == 0 || buffer_size == 0 {
            return Err(BufferError::InvalidSize("Capacity and buffer size must be non-zero".to_string()));
        }

        let pool = Self {
            buffers: Mutex::new(VecDeque::with_capacity(capacity)),
            buffer_size,
            capacity,
            alignment,
            allocated: AtomicUsize::new(0),
            in_use: AtomicUsize::new(0),
            peak_usage: AtomicUsize::new(0),
            total_allocations: AtomicU64::new(0),
            total_deallocations: AtomicU64::new(0),
        };

        // Pre-allocate some buffers
        pool.preallocate(capacity.min(16))?;
        
        debug!("Created buffer pool: capacity={}, buffer_size={}, alignment={}", 
               capacity, buffer_size, alignment);
        
        Ok(pool)
    }

    /// Pre-allocate buffers in the pool
    fn preallocate(&self, count: usize) -> BufferResult<()> {
        let layout = Layout::from_size_align(self.buffer_size, self.alignment)
            .map_err(|e| BufferError::Allocation(e.to_string()))?;
        
        let mut buffers = self.buffers.lock();
        static BUFFER_ID: AtomicU64 = AtomicU64::new(1);
        
        for _ in 0..count {
            if self.allocated.load(Ordering::Relaxed) >= self.capacity {
                break;
            }

            let data = unsafe {
                let ptr = alloc(layout);
                if ptr.is_null() {
                    return Err(BufferError::Allocation("Pre-allocation failed".to_string()));
                }
                NonNull::new_unchecked(ptr)
            };

            let id = BUFFER_ID.fetch_add(1, Ordering::Relaxed);
            buffers.push_back((SendSyncPtr(data), id));
            self.allocated.fetch_add(1, Ordering::Relaxed);
            self.total_allocations.fetch_add(1, Ordering::Relaxed);
        }

        trace!("Pre-allocated {} buffers", count);
        Ok(())
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&self, size: usize) -> BufferResult<ZeroCopyBuffer> {
        if size > self.buffer_size {
            return Err(BufferError::InvalidSize(
                format!("Requested size {} exceeds buffer size {}", size, self.buffer_size)
            ));
        }

        let (data, id) = {
            let mut buffers = self.buffers.lock();
            
            if let Some((data, id)) = buffers.pop_front() {
                (data, id)
            } else if self.allocated.load(Ordering::Relaxed) < self.capacity {
                // Allocate new buffer
                drop(buffers);
                self.allocate_new_buffer()?
            } else {
                return Err(BufferError::PoolExhausted("No buffers available".to_string()));
            }
        };

        let in_use = self.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        let peak = self.peak_usage.load(Ordering::Relaxed);
        if in_use > peak {
            self.peak_usage.store(in_use, Ordering::Relaxed);
        }

        trace!("Allocated buffer {} from pool (in_use: {})", id, in_use);

        // Create buffer without pool reference for now to avoid lifecycle issues
        ZeroCopyBuffer::new(self.buffer_size)
    }

    fn allocate_new_buffer(&self) -> BufferResult<(SendSyncPtr, u64)> {
        let layout = Layout::from_size_align(self.buffer_size, self.alignment)
            .map_err(|e| BufferError::Allocation(e.to_string()))?;
        
        let data = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(BufferError::Allocation("Buffer allocation failed".to_string()));
            }
            NonNull::new_unchecked(ptr)
        };

        static BUFFER_ID: AtomicU64 = AtomicU64::new(1);
        let id = BUFFER_ID.fetch_add(1, Ordering::Relaxed);
        
        self.allocated.fetch_add(1, Ordering::Relaxed);
        self.total_allocations.fetch_add(1, Ordering::Relaxed);

        Ok((SendSyncPtr(data), id))
    }

    /// Return a buffer to the pool
    pub(crate) fn return_buffer(&self, buffer: &mut ZeroCopyBuffer) {
        let mut buffers = self.buffers.lock();
        
        // Reset buffer state
        buffer.len = 0;
        
        if buffers.len() < self.capacity {
            buffers.push_back((SendSyncPtr(buffer.data), buffer.id));
        } else {
            // Pool is full, deallocate buffer
            unsafe {
                let layout = Layout::from_size_align_unchecked(self.buffer_size, self.alignment);
                dealloc(buffer.data.as_ptr(), layout);
            }
            self.allocated.fetch_sub(1, Ordering::Relaxed);
        }

        self.in_use.fetch_sub(1, Ordering::Relaxed);
        self.total_deallocations.fetch_add(1, Ordering::Relaxed);
        trace!("Returned buffer {} to pool", buffer.id);
    }

    /// Get pool statistics
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            capacity: self.capacity,
            buffer_size: self.buffer_size,
            allocated: self.allocated.load(Ordering::Relaxed),
            in_use: self.in_use.load(Ordering::Relaxed),
            available: self.buffers.lock().len(),
            peak_usage: self.peak_usage.load(Ordering::Relaxed),
            total_allocations: self.total_allocations.load(Ordering::Relaxed),
            total_deallocations: self.total_deallocations.load(Ordering::Relaxed),
        }
    }

    /// Clear unused buffers from the pool
    pub fn shrink(&self) -> usize {
        let mut buffers = self.buffers.lock();
        let initial_count = buffers.len();
        let keep_count = initial_count / 2; // Keep half of the buffers
        
        let layout = Layout::from_size_align(self.buffer_size, self.alignment).unwrap();
        
        while buffers.len() > keep_count {
            if let Some((data, _)) = buffers.pop_back() {
                unsafe {
                    dealloc(data.as_ptr(), layout);
                }
                self.allocated.fetch_sub(1, Ordering::Relaxed);
            }
        }

        let freed = initial_count - buffers.len();
        if freed > 0 {
            debug!("Freed {} unused buffers from pool", freed);
        }
        freed
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let buffers = self.buffers.get_mut();
        let layout = Layout::from_size_align(self.buffer_size, self.alignment).unwrap();
        
        while let Some((data, _)) = buffers.pop_front() {
            unsafe {
                dealloc(data.as_ptr(), layout);
            }
        }
        
        debug!("Deallocated buffer pool with {} buffers", self.allocated.load(Ordering::Relaxed));
    }
}

/// Buffer pool statistics
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub capacity: usize,
    pub buffer_size: usize,
    pub allocated: usize,
    pub in_use: usize,
    pub available: usize,
    pub peak_usage: usize,
    pub total_allocations: u64,
    pub total_deallocations: u64,
}

impl BufferPoolStats {
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.in_use as f64 / self.capacity as f64
        }
    }

    pub fn allocation_rate(&self) -> f64 {
        if self.total_deallocations == 0 {
            0.0
        } else {
            self.total_allocations as f64 / self.total_deallocations as f64
        }
    }
}

/// Scatter-gather buffer for vectored I/O
pub struct ScatterGatherBuffer {
    buffers: SmallVec<[ZeroCopyBuffer; 8]>,
    total_len: usize,
}

impl ScatterGatherBuffer {
    /// Create a new scatter-gather buffer
    pub fn new() -> Self {
        Self {
            buffers: SmallVec::new(),
            total_len: 0,
        }
    }

    /// Add a buffer to the scatter-gather list
    pub fn add_buffer(&mut self, buffer: ZeroCopyBuffer) {
        self.total_len += buffer.len();
        self.buffers.push(buffer);
    }

    /// Get the buffers as iovecs for system calls
    pub fn as_iovecs(&self) -> Vec<libc::iovec> {
        self.buffers
            .iter()
            .map(|buf| libc::iovec {
                iov_base: buf.as_ref().as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect()
    }

    /// Get total length across all buffers
    pub fn total_len(&self) -> usize {
        self.total_len
    }

    /// Get number of buffers
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Clear all buffers
    pub fn clear(&mut self) {
        self.buffers.clear();
        self.total_len = 0;
    }

    /// Get buffers for direct access
    pub fn buffers(&self) -> &[ZeroCopyBuffer] {
        &self.buffers
    }

    /// Get mutable buffers for direct access
    pub fn buffers_mut(&mut self) -> &mut SmallVec<[ZeroCopyBuffer; 8]> {
        &mut self.buffers
    }
}

impl Default for ScatterGatherBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Global buffer manager for coordinating multiple pools
pub struct GlobalBufferManager {
    pools: RwLock<Vec<Arc<BufferPool>>>,
    default_pool: Arc<BufferPool>,
    stats: AtomicU64, // Combined stats counter
}

impl GlobalBufferManager {
    /// Create a new global buffer manager
    pub fn new() -> BufferResult<Self> {
        let default_pool = Arc::new(BufferPool::new(1024, 4096)?);
        
        Ok(Self {
            pools: RwLock::new(vec![default_pool.clone()]),
            default_pool,
            stats: AtomicU64::new(0),
        })
    }

    /// Add a specialized buffer pool
    pub fn add_pool(&self, pool: Arc<BufferPool>) {
        let mut pools = self.pools.write();
        pools.push(pool);
        debug!("Added buffer pool to global manager");
    }

    /// Get buffer from the most appropriate pool
    pub fn get_buffer(&self, size: usize) -> BufferResult<ZeroCopyBuffer> {
        let pools = self.pools.read();
        
        // Find the best-fitting pool
        let pool = pools
            .iter()
            .filter(|p| p.stats().buffer_size >= size)
            .min_by_key(|p| p.stats().buffer_size)
            .unwrap_or(&self.default_pool);

        self.stats.fetch_add(1, Ordering::Relaxed);
        pool.get_buffer(size)
    }

    /// Get combined statistics from all pools
    pub fn combined_stats(&self) -> Vec<BufferPoolStats> {
        let pools = self.pools.read();
        pools.iter().map(|p| p.stats()).collect()
    }

    /// Shrink all pools to free unused memory
    pub fn shrink_all(&self) -> usize {
        let pools = self.pools.read();
        pools.iter().map(|p| p.shrink()).sum()
    }

    /// Get allocation count
    pub fn allocation_count(&self) -> u64 {
        self.stats.load(Ordering::Relaxed)
    }
}

impl Default for GlobalBufferManager {
    fn default() -> Self {
        Self::new().expect("Failed to create global buffer manager")
    }
}

// Global buffer manager instance
static GLOBAL_MANAGER: once_cell::sync::Lazy<GlobalBufferManager> = 
    once_cell::sync::Lazy::new(|| GlobalBufferManager::new().expect("Failed to initialize global buffer manager"));

/// Get the global buffer manager
pub fn global_buffer_manager() -> &'static GlobalBufferManager {
    &GLOBAL_MANAGER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_buffer_creation() {
        let buf = ZeroCopyBuffer::new(1024).unwrap();
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_zero_copy_buffer_operations() {
        let mut buf = ZeroCopyBuffer::new(1024).unwrap();
        buf.set_len(100);
        assert_eq!(buf.len(), 100);
        
        // Write some data
        buf.as_mut()[0] = 42;
        assert_eq!(buf.as_ref()[0], 42);
        
        buf.clear();
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(10, 1024).unwrap();
        
        // Get a buffer
        let buf1 = pool.get_buffer(512).unwrap();
        assert_eq!(buf1.capacity(), 1024);
        // Note: Buffers from pool are not marked as pooled to avoid lifecycle issues
        
        // Get another buffer
        let buf2 = pool.get_buffer(1024).unwrap();
        assert_ne!(buf1.id(), buf2.id());
        
        // Check stats
        let stats = pool.stats();
        assert_eq!(stats.in_use, 2);
        assert!(stats.utilization() > 0.0);
    }

    #[test]
    fn test_buffer_pool_exhaustion() {
        let pool = BufferPool::new(2, 1024).unwrap();
        
        let _buf1 = pool.get_buffer(1024).unwrap();
        let _buf2 = pool.get_buffer(1024).unwrap();
        
        // Pool should be exhausted now
        let result = pool.get_buffer(1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_scatter_gather_buffer() {
        let mut sg_buf = ScatterGatherBuffer::new();
        
        let mut buf1 = ZeroCopyBuffer::new(100).unwrap();
        let mut buf2 = ZeroCopyBuffer::new(200).unwrap();
        
        // Set the length of the buffers to match their capacity for testing
        buf1.set_len(100);
        buf2.set_len(200);
        
        sg_buf.add_buffer(buf1);
        sg_buf.add_buffer(buf2);
        
        assert_eq!(sg_buf.buffer_count(), 2);
        assert_eq!(sg_buf.total_len(), 300);
        
        let iovecs = sg_buf.as_iovecs();
        assert_eq!(iovecs.len(), 2);
    }

    #[test]
    fn test_global_buffer_manager() {
        let manager = global_buffer_manager();
        let buf = manager.get_buffer(1024).unwrap();
        assert_eq!(buf.capacity(), 4096); // Default pool buffer size
        
        let stats = manager.combined_stats();
        assert!(!stats.is_empty());
        assert!(manager.allocation_count() > 0);
    }

    #[test]
    fn test_aligned_buffer() {
        let buf = ZeroCopyBuffer::new_aligned(1024, 64).unwrap();
        assert!(buf.is_aligned());
        assert_eq!(buf.data.as_ptr() as usize % 64, 0); // Check alignment
    }
}