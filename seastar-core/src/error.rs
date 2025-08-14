//! Error types for Seastar-RS
//!
//! Defines the standard error types used throughout the Seastar-RS framework.


/// Standard error type for Seastar-RS operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// I/O operation failed
    #[error("I/O error: {0}")]
    Io(String),
    
    /// Network operation failed
    #[error("Network error: {0}")]
    Network(String),
    
    /// Invalid argument provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    
    /// Operation was cancelled
    #[error("Operation cancelled")]
    Cancelled,
    
    /// Timeout occurred
    #[error("Timeout")]
    Timeout,
    
    /// Resource not available
    #[error("Resource unavailable: {0}")]
    ResourceUnavailable(String),
    
    /// Memory allocation failed
    #[error("Memory allocation failed")]
    OutOfMemory,
    
    /// Internal framework error
    #[error("Internal error: {0}")]
    Internal(String),
    
    /// Permission denied
    #[error("Permission denied")]
    PermissionDenied,
    
    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),
    
    /// Resource already exists
    #[error("Already exists: {0}")]  
    AlreadyExists(String),
    
    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),
    
    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(error.to_string()),
            std::io::ErrorKind::PermissionDenied => Error::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused => Error::Connection(error.to_string()),
            std::io::ErrorKind::ConnectionAborted => Error::Connection(error.to_string()),
            std::io::ErrorKind::TimedOut => Error::Timeout,
            std::io::ErrorKind::AlreadyExists => Error::AlreadyExists(error.to_string()),
            _ => Error::Io(error.to_string()),
        }
    }
}

/// Result type for Seastar-RS operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error codes that can be used for categorizing errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Success = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    PermissionDenied = 4,
    ResourceUnavailable = 5,
    Timeout = 6,
    Cancelled = 7,
    Internal = 8,
    Io = 9,
    Network = 10,
    Connection = 11,
    Protocol = 12,
    Serialization = 13,
    OutOfMemory = 14,
}

impl Error {
    /// Get the error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Error::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Error::NotFound(_) => ErrorCode::NotFound,
            Error::AlreadyExists(_) => ErrorCode::AlreadyExists,
            Error::PermissionDenied => ErrorCode::PermissionDenied,
            Error::ResourceUnavailable(_) => ErrorCode::ResourceUnavailable,
            Error::Timeout => ErrorCode::Timeout,
            Error::Cancelled => ErrorCode::Cancelled,
            Error::Internal(_) => ErrorCode::Internal,
            Error::Io(_) => ErrorCode::Io,
            Error::Network(_) => ErrorCode::Network,
            Error::Connection(_) => ErrorCode::Connection,
            Error::Protocol(_) => ErrorCode::Protocol,
            Error::Serialization(_) => ErrorCode::Serialization,
            Error::OutOfMemory => ErrorCode::OutOfMemory,
        }
    }
    
    /// Check if this is a retryable error
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Timeout
                | Error::ResourceUnavailable(_)
                | Error::Network(_)
                | Error::Connection(_)
        )
    }
    
    /// Check if this error indicates a temporary condition
    pub fn is_temporary(&self) -> bool {
        matches!(
            self,
            Error::Timeout
                | Error::ResourceUnavailable(_)
                | Error::OutOfMemory
        )
    }
}