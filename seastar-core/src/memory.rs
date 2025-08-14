//! Memory management for Seastar-RS
//!
//! Provides efficient memory allocation and buffer management.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics for memory usage
#[derive(Debug, Default)]
pub struct MemoryStats {
    pub allocated_bytes: AtomicUsize,
    pub freed_bytes: AtomicUsize,
    pub active_allocations: AtomicUsize,
    pub peak_usage_bytes: AtomicUsize,
}

impl MemoryStats {
    pub fn allocated(&self) -> usize {
        self.allocated_bytes.load(Ordering::SeqCst)
    }
    
    pub fn freed(&self) -> usize {
        self.freed_bytes.load(Ordering::SeqCst)
    }
    
    pub fn active(&self) -> usize {
        self.active_allocations.load(Ordering::SeqCst)
    }
    
    pub fn peak(&self) -> usize {
        self.peak_usage_bytes.load(Ordering::SeqCst)
    }
    
    pub fn net_allocated(&self) -> isize {
        self.allocated() as isize - self.freed() as isize
    }
}

/// A memory pool for efficient allocation of fixed-size objects
pub struct MemoryPool {
    stats: MemoryStats,
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            stats: MemoryStats::default(),
        }
    }
    
    pub fn stats(&self) -> &MemoryStats {
        &self.stats
    }
}

impl Default for MemoryPool {
    fn default() -> Self {
        Self::new()
    }
}

/// An aligned buffer for DMA operations
pub struct AlignedBuffer {
    data: Vec<u8>,
    alignment: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize, alignment: usize) -> Self {
        let mut data = Vec::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        
        Self {
            data,
            alignment,
        }
    }
    
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    pub fn alignment(&self) -> usize {
        self.alignment
    }
}

/// Global memory statistics
static GLOBAL_MEMORY_STATS: MemoryStats = MemoryStats {
    allocated_bytes: AtomicUsize::new(0),
    freed_bytes: AtomicUsize::new(0),
    active_allocations: AtomicUsize::new(0),
    peak_usage_bytes: AtomicUsize::new(0),
};

/// Get global memory statistics
pub fn global_memory_stats() -> &'static MemoryStats {
    &GLOBAL_MEMORY_STATS
}