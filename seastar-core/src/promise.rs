//! Promise implementation for Seastar-style async programming
//!
//! Promises provide the write-side of the future/promise pair, allowing
//! asynchronous operations to signal completion and provide values.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::mem;

/// States that a promise can be in
#[derive(Debug, Clone)]
pub enum PromiseState<T> {
    /// Promise has not been resolved yet
    Pending,
    /// Promise was resolved with a value
    Ready(T),
    /// Promise was resolved with an error
    Exception(crate::Error),
}

impl<T> PromiseState<T> {
    pub fn is_ready(&self) -> bool {
        !matches!(self, PromiseState::Pending)
    }

    pub fn is_exception(&self) -> bool {
        matches!(self, PromiseState::Exception(_))
    }

    pub fn get_value(self) -> Result<T, crate::Error> {
        match self {
            PromiseState::Ready(value) => Ok(value),
            PromiseState::Exception(error) => Err(error),
            PromiseState::Pending => panic!("Attempted to get value from pending promise"),
        }
    }
}

/// Shared state between a Promise and its corresponding Future
struct SharedState<T> {
    state: PromiseState<T>,
    waker: Option<Waker>,
}

impl<T> SharedState<T> {
    fn new() -> Self {
        Self {
            state: PromiseState::Pending,
            waker: None,
        }
    }
}

/// A promise that can be used to resolve a future
/// 
/// The promise is the write-side of the future/promise pair. It can be used
/// to provide a value or signal an error to the corresponding future.
pub struct Promise<T> {
    shared: Arc<Mutex<SharedState<T>>>,
}

impl<T> Promise<T> {
    /// Create a new promise/future pair
    pub fn new() -> (Promise<T>, PromiseFuture<T>) {
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let promise = Promise {
            shared: shared.clone(),
        };
        let future = PromiseFuture {
            shared: shared.clone(),
        };
        (promise, future)
    }

    /// Set the value of the promise, resolving the corresponding future
    pub fn set_value(self, value: T) {
        self.resolve(PromiseState::Ready(value));
    }

    /// Set an exception for the promise, causing the future to fail
    pub fn set_exception(self, error: crate::Error) {
        self.resolve(PromiseState::Exception(error));
    }

    /// Check if the promise has been resolved
    pub fn is_resolved(&self) -> bool {
        if let Ok(state) = self.shared.lock() {
            state.state.is_ready()
        } else {
            true // If lock is poisoned, consider it resolved
        }
    }

    /// Resolve the promise with the given state
    fn resolve(self, new_state: PromiseState<T>) {
        let waker = {
            if let Ok(mut state) = self.shared.lock() {
                if !state.state.is_ready() {
                    state.state = new_state;
                    // Extract the waker while holding the lock
                    state.waker.take()
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        // Wake up the future after releasing the lock and while still holding the Arc
        if let Some(waker) = waker {
            waker.wake();
        }
        // self.shared is dropped here, ensuring the Arc stays alive until after wake()
    }
}

impl<T> Default for Promise<T> {
    fn default() -> Self {
        Self::new().0
    }
}

/// The future side of a promise
pub struct PromiseFuture<T> {
    shared: Arc<Mutex<SharedState<T>>>,
}

impl<T> Future for PromiseFuture<T> {
    type Output = Result<T, crate::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shared.lock() {
            Ok(mut state) => {
                match &state.state {
                    PromiseState::Pending => {
                        // Store the waker for when the promise is resolved
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                    PromiseState::Ready(_) | PromiseState::Exception(_) => {
                        // Take the state and return it
                        let result_state = mem::replace(&mut state.state, PromiseState::Pending);
                        match result_state {
                            PromiseState::Ready(value) => Poll::Ready(Ok(value)),
                            PromiseState::Exception(error) => Poll::Ready(Err(error)),
                            PromiseState::Pending => unreachable!(),
                        }
                    }
                }
            }
            Err(_) => {
                // Lock was poisoned, treat as error
                Poll::Ready(Err(crate::Error::Internal("Promise lock poisoned".to_string())))
            }
        }
    }
}

// PromiseFuture already implements std::future::Future and gets the
// Future trait implementation automatically via the blanket impl

/// Utility functions for working with promises

/// Create a promise that is already resolved with a value
pub fn make_ready_promise<T>(value: T) -> (Promise<T>, PromiseFuture<T>) {
    let (promise, future) = Promise::new();
    promise.set_value(value);
    (Promise::new().0, future) // Return a new promise since the old one was consumed
}

/// Create a promise that is already resolved with an error
pub fn make_exception_promise<T>(error: crate::Error) -> (Promise<T>, PromiseFuture<T>) {
    let (promise, future) = Promise::new();
    promise.set_exception(error);
    (Promise::new().0, future) // Return a new promise since the old one was consumed
}

/// Create multiple promises for collecting results
pub fn make_promise_vector<T>(count: usize) -> (Vec<Promise<T>>, Vec<PromiseFuture<T>>) {
    let mut promises = Vec::with_capacity(count);
    let mut futures = Vec::with_capacity(count);
    
    for _ in 0..count {
        let (promise, future) = Promise::new();
        promises.push(promise);
        futures.push(future);
    }
    
    (promises, futures)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_promise_basic_usage() {
        let (promise, future) = Promise::new();
        
        // Resolve the promise in a separate task
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            promise.set_value(42);
        });
        
        // Await the future
        let result = future.await;
        match result {
            Ok(value) => assert_eq!(value, 42),
            Err(e) => panic!("Expected success but got error: {:?}", e),
        }
    }
    
    #[tokio::test]
    async fn test_promise_exception() {
        let (promise, future) = Promise::<i32>::new();
        
        // Resolve with an exception
        promise.set_exception(crate::Error::InvalidArgument("test error".to_string()));
        
        // Future should complete with error
        let result = future.await;
        assert!(result.is_err());
    }
}