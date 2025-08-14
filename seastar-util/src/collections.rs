//! Collection utilities

/// Circular buffer implementation
pub struct CircularBuffer<T> {
    // Stub implementation
    _phantom: std::marker::PhantomData<T>,
}

impl<T> CircularBuffer<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}