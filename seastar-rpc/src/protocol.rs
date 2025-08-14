//! RPC protocol definitions

use seastar_core::{Result, Error};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for RPC requests
pub type RequestId = u64;

/// RPC method name
pub type MethodName = String;

/// RPC service name
pub type ServiceName = String;

/// Global request ID counter
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique request ID
pub fn generate_request_id() -> RequestId {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// RPC message type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessageType {
    /// Request message
    Request {
        id: RequestId,
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>,
    },
    
    /// Response message - success
    Response {
        id: RequestId,
        payload: Vec<u8>,
    },
    
    /// Response message - error
    Error {
        id: RequestId,
        code: RpcErrorCode,
        message: String,
    },
    
    /// Notification (no response expected)
    Notification {
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>,
    },
    
    /// Heartbeat for connection monitoring
    Heartbeat {
        timestamp: u64,
    },
    
    /// Cancel a request
    Cancel {
        id: RequestId,
    },
}

/// RPC error codes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RpcErrorCode {
    /// Method not found
    MethodNotFound,
    /// Invalid parameters
    InvalidParams,
    /// Internal error
    InternalError,
    /// Service unavailable
    ServiceUnavailable,
    /// Request timeout
    Timeout,
    /// Request cancelled
    Cancelled,
    /// Serialization error
    SerializationError,
    /// Network error
    NetworkError,
    /// Custom error with code
    Custom(i32),
}

impl RpcErrorCode {
    pub fn code(&self) -> i32 {
        match self {
            RpcErrorCode::MethodNotFound => -32601,
            RpcErrorCode::InvalidParams => -32602,
            RpcErrorCode::InternalError => -32603,
            RpcErrorCode::ServiceUnavailable => -32000,
            RpcErrorCode::Timeout => -32001,
            RpcErrorCode::Cancelled => -32002,
            RpcErrorCode::SerializationError => -32003,
            RpcErrorCode::NetworkError => -32004,
            RpcErrorCode::Custom(code) => *code,
        }
    }
    
    pub fn message(&self) -> &str {
        match self {
            RpcErrorCode::MethodNotFound => "Method not found",
            RpcErrorCode::InvalidParams => "Invalid parameters",
            RpcErrorCode::InternalError => "Internal error",
            RpcErrorCode::ServiceUnavailable => "Service unavailable",
            RpcErrorCode::Timeout => "Request timeout",
            RpcErrorCode::Cancelled => "Request cancelled",
            RpcErrorCode::SerializationError => "Serialization error",
            RpcErrorCode::NetworkError => "Network error",
            RpcErrorCode::Custom(_) => "Custom error",
        }
    }
}

impl std::fmt::Display for RpcErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message(), self.code())
    }
}

/// RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: RpcErrorCode,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new(code: RpcErrorCode, message: String) -> Self {
        Self {
            code,
            message,
            data: None,
        }
    }
    
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }
    
    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            RpcErrorCode::MethodNotFound,
            format!("Method '{}' not found", method)
        )
    }
    
    pub fn invalid_params(message: &str) -> Self {
        Self::new(RpcErrorCode::InvalidParams, message.to_string())
    }
    
    pub fn internal_error(message: &str) -> Self {
        Self::new(RpcErrorCode::InternalError, message.to_string())
    }
    
    pub fn service_unavailable(service: &str) -> Self {
        Self::new(
            RpcErrorCode::ServiceUnavailable,
            format!("Service '{}' unavailable", service)
        )
    }
    
    pub fn timeout(id: RequestId) -> Self {
        Self::new(
            RpcErrorCode::Timeout,
            format!("Request {} timed out", id)
        )
    }
    
    pub fn cancelled(id: RequestId) -> Self {
        Self::new(
            RpcErrorCode::Cancelled,
            format!("Request {} cancelled", id)
        )
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

impl From<RpcError> for Error {
    fn from(err: RpcError) -> Self {
        Error::Internal(format!("RPC Error: {}", err))
    }
}

/// RPC protocol configuration
#[derive(Debug, Clone)]
pub struct RpcProtocolConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Request timeout in milliseconds
    pub request_timeout: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval: u64,
    /// Enable message compression
    pub compression: bool,
    /// Protocol version
    pub version: String,
}

impl Default for RpcProtocolConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16MB
            request_timeout: 30_000, // 30 seconds
            heartbeat_interval: 30_000, // 30 seconds
            compression: false,
            version: "1.0".to_string(),
        }
    }
}

/// RPC protocol handler
#[derive(Clone)]
pub struct RpcProtocol {
    config: RpcProtocolConfig,
}

impl RpcProtocol {
    /// Create a new RPC protocol with default configuration
    pub fn new() -> Self {
        Self {
            config: RpcProtocolConfig::default(),
        }
    }
    
    /// Create a new RPC protocol with custom configuration
    pub fn with_config(config: RpcProtocolConfig) -> Self {
        Self { config }
    }
    
    /// Get protocol configuration
    pub fn config(&self) -> &RpcProtocolConfig {
        &self.config
    }
    
    /// Create a request message
    pub fn create_request(
        &self,
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>
    ) -> RpcMessageType {
        RpcMessageType::Request {
            id: generate_request_id(),
            service,
            method,
            payload,
        }
    }
    
    /// Create a response message
    pub fn create_response(&self, id: RequestId, payload: Vec<u8>) -> RpcMessageType {
        RpcMessageType::Response { id, payload }
    }
    
    /// Create an error response message
    pub fn create_error(&self, id: RequestId, error: RpcError) -> RpcMessageType {
        RpcMessageType::Error {
            id,
            code: error.code,
            message: error.message,
        }
    }
    
    /// Create a notification message
    pub fn create_notification(
        &self,
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>
    ) -> RpcMessageType {
        RpcMessageType::Notification {
            service,
            method,
            payload,
        }
    }
    
    /// Create a heartbeat message
    pub fn create_heartbeat(&self) -> RpcMessageType {
        RpcMessageType::Heartbeat {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
    
    /// Create a cancel message
    pub fn create_cancel(&self, id: RequestId) -> RpcMessageType {
        RpcMessageType::Cancel { id }
    }
}

impl Default for RpcProtocol {
    fn default() -> Self {
        Self::new()
    }
}

/// RPC method metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMethodInfo {
    pub name: MethodName,
    pub description: String,
    pub input_type: String,
    pub output_type: String,
}

/// RPC service metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcServiceInfo {
    pub name: ServiceName,
    pub description: String,
    pub version: String,
    pub methods: Vec<RpcMethodInfo>,
}

/// RPC service registry for reflection
#[derive(Debug, Default)]
pub struct RpcServiceRegistry {
    services: HashMap<ServiceName, RpcServiceInfo>,
}

impl RpcServiceRegistry {
    /// Create a new service registry
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a service
    pub fn register_service(&mut self, service: RpcServiceInfo) {
        self.services.insert(service.name.clone(), service);
    }
    
    /// Get service information
    pub fn get_service(&self, name: &str) -> Option<&RpcServiceInfo> {
        self.services.get(name)
    }
    
    /// List all services
    pub fn list_services(&self) -> Vec<&RpcServiceInfo> {
        self.services.values().collect()
    }
    
    /// Get method information
    pub fn get_method(&self, service: &str, method: &str) -> Option<&RpcMethodInfo> {
        self.services
            .get(service)?
            .methods
            .iter()
            .find(|m| m.name == method)
    }
}