//! RPC client implementation

use crate::protocol::{
    RpcMessageType, RpcError, RpcErrorCode, RequestId, ServiceName, MethodName,
    RpcProtocol, RpcProtocolConfig, generate_request_id
};
use crate::codec::{RpcCodec, WireProtocol, SerializationFormat};
use seastar_core::{Result, Error};
use seastar_net::{Socket, ConnectedSocket, SocketAddress};
use socket2::{Domain, Type, Protocol};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn, debug};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::timeout;

/// Pending RPC request
struct PendingRequest {
    /// Request ID
    id: RequestId,
    /// Response sender
    response_sender: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
    /// Request timestamp
    timestamp: std::time::Instant,
}

/// RPC client configuration
#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    /// Server address to connect to
    pub server_address: SocketAddress,
    /// Request timeout in milliseconds
    pub request_timeout: u64,
    /// Connection timeout in milliseconds
    pub connection_timeout: u64,
    /// Protocol configuration
    pub protocol_config: RpcProtocolConfig,
    /// Enable connection pooling
    pub connection_pooling: bool,
    /// Maximum number of connections in pool
    pub max_connections: usize,
    /// Retry attempts for failed requests
    pub retry_attempts: u32,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        Self {
            server_address: SocketAddress::Inet(
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::new(127, 0, 0, 1),
                    9090
                ))
            ),
            request_timeout: 30_000, // 30 seconds
            connection_timeout: 5_000, // 5 seconds
            protocol_config: RpcProtocolConfig::default(),
            connection_pooling: false,
            max_connections: 10,
            retry_attempts: 3,
        }
    }
}

/// RPC client
pub struct RpcClient {
    config: RpcClientConfig,
    protocol: RpcProtocol,
    wire_protocol: WireProtocol,
    codec: RpcCodec,
    connection: Arc<Mutex<Option<ConnectedSocket>>>,
    pending_requests: Arc<Mutex<HashMap<RequestId, PendingRequest>>>,
}

impl RpcClient {
    /// Create a new RPC client with default configuration
    pub fn new() -> Self {
        let config = RpcClientConfig::default();
        Self::with_config(config)
    }
    
    /// Create a new RPC client with custom configuration
    pub fn with_config(config: RpcClientConfig) -> Self {
        let protocol = RpcProtocol::with_config(config.protocol_config.clone());
        let codec = RpcCodec::from_config(&config.protocol_config);
        let wire_protocol = WireProtocol::new(codec.clone());
        
        Self {
            config,
            protocol,
            wire_protocol,
            codec,
            connection: Arc::new(Mutex::new(None)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Connect to the RPC server
    pub async fn connect(&self) -> Result<()> {
        info!("Connecting to RPC server at {:?}", self.config.server_address);
        
        // Create connection with timeout
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;
        
        let connect_future = socket.connect(&self.config.server_address);
        timeout(
            Duration::from_millis(self.config.connection_timeout),
            connect_future
        ).await
        .map_err(|_| Error::Network("Connection timeout".to_string()))??;
        
        let connected_socket = ConnectedSocket::from_socket(socket);
        
        // Store connection
        {
            let mut connection = self.connection.lock().unwrap();
            *connection = Some(connected_socket);
        }
        
        info!("Connected to RPC server");
        Ok(())
    }
    
    /// Disconnect from the RPC server
    pub async fn disconnect(&self) -> Result<()> {
        info!("Disconnecting from RPC server");
        
        let mut connection = self.connection.lock().unwrap();
        if let Some(conn) = connection.take() {
            // Close the connection
            conn.close().await?;
        }
        
        // Clear pending requests
        {
            let mut pending = self.pending_requests.lock().unwrap();
            for (_, request) in pending.drain() {
                let _ = request.response_sender.send(Err(Error::Internal("Connection closed".to_string())));
            }
        }
        
        info!("Disconnected from RPC server");
        Ok(())
    }
    
    /// Check if connected to server
    pub fn is_connected(&self) -> bool {
        let connection = self.connection.lock().unwrap();
        connection.is_some()
    }
    
    /// Call an RPC method
    pub async fn call(
        &self,
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>
    ) -> Result<Vec<u8>> {
        if !self.is_connected() {
            self.connect().await?;
        }
        
        let request_id = generate_request_id();
        let message = self.protocol.create_request(service.clone(), method.clone(), payload);
        
        // Create response channel
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        
        // Store pending request
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(request_id, PendingRequest {
                id: request_id,
                response_sender,
                timestamp: std::time::Instant::now(),
            });
        }
        
        // Send request
        let wire_data = self.wire_protocol.encode_for_wire(&message)?;
        {
            let mut connection = self.connection.lock().unwrap();
            if let Some(ref mut conn) = connection.as_mut() {
                conn.write(&wire_data).await?;
            } else {
                return Err(Error::Internal("No connection available".to_string()));
            }
        }
        
        debug!("Sent RPC request {}: {}.{}", request_id, service, method);
        
        // Wait for response with timeout
        let response = timeout(
            Duration::from_millis(self.config.request_timeout),
            response_receiver
        ).await;
        
        match response {
            Ok(recv_result) => recv_result.map_err(|_| Error::Internal("Channel closed".to_string()))?,
            Err(_) => {
                // Remove timed out request
                let mut pending = self.pending_requests.lock().unwrap();
                pending.remove(&request_id);
                Err(Error::Internal(format!("Request {} timed out", request_id)))
            }
        }
    }
    
    /// Call an RPC method with typed parameters
    pub async fn call_typed<Req, Resp>(
        &self,
        service: ServiceName,
        method: MethodName,
        request: Req
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        // Serialize request
        let payload = self.codec.serialize(&request)?;
        
        // Make call
        let response_payload = self.call(service, method, payload).await?;
        
        // Deserialize response
        self.codec.deserialize(&response_payload)
    }
    
    /// Send a notification (no response expected)
    pub async fn notify(
        &self,
        service: ServiceName,
        method: MethodName,
        payload: Vec<u8>
    ) -> Result<()> {
        if !self.is_connected() {
            self.connect().await?;
        }
        
        let message = self.protocol.create_notification(service.clone(), method.clone(), payload);
        
        // Send notification
        let wire_data = self.wire_protocol.encode_for_wire(&message)?;
        {
            let mut connection = self.connection.lock().unwrap();
            if let Some(ref mut conn) = connection.as_mut() {
                conn.write(&wire_data).await?;
            } else {
                return Err(Error::Internal("No connection available".to_string()));
            }
        }
        
        debug!("Sent RPC notification: {}.{}", service, method);
        Ok(())
    }
    
    /// Send a typed notification
    pub async fn notify_typed<Req>(
        &self,
        service: ServiceName,
        method: MethodName,
        request: Req
    ) -> Result<()>
    where
        Req: Serialize,
    {
        let payload = self.codec.serialize(&request)?;
        self.notify(service, method, payload).await
    }
    
    /// Send a heartbeat
    pub async fn heartbeat(&self) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Internal("Not connected".to_string()));
        }
        
        let message = self.protocol.create_heartbeat();
        let wire_data = self.wire_protocol.encode_for_wire(&message)?;
        
        {
            let mut connection = self.connection.lock().unwrap();
            if let Some(ref mut conn) = connection.as_mut() {
                conn.write(&wire_data).await?;
            } else {
                return Err(Error::Internal("No connection available".to_string()));
            }
        }
        
        debug!("Sent heartbeat");
        Ok(())
    }
    
    /// Cancel a pending request
    pub async fn cancel_request(&self, request_id: RequestId) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Internal("Not connected".to_string()));
        }
        
        let message = self.protocol.create_cancel(request_id);
        let wire_data = self.wire_protocol.encode_for_wire(&message)?;
        
        {
            let mut connection = self.connection.lock().unwrap();
            if let Some(ref mut conn) = connection.as_mut() {
                conn.write(&wire_data).await?;
            } else {
                return Err(Error::Internal("No connection available".to_string()));
            }
        }
        
        // Remove from pending requests
        {
            let mut pending = self.pending_requests.lock().unwrap();
            if let Some(request) = pending.remove(&request_id) {
                let _ = request.response_sender.send(Err(Error::Internal("Request cancelled".to_string())));
            }
        }
        
        debug!("Cancelled request {}", request_id);
        Ok(())
    }
    
    /// Get client configuration
    pub fn config(&self) -> &RpcClientConfig {
        &self.config
    }
    
    /// Get pending request count
    pub fn pending_request_count(&self) -> usize {
        let pending = self.pending_requests.lock().unwrap();
        pending.len()
    }
    
    /// Start response handling loop (for persistent connections)
    pub async fn start_response_handler(&self) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Internal("Not connected".to_string()));
        }
        
        info!("Starting RPC response handler");
        
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
        
        loop {
            let bytes_read = {
                let mut connection = self.connection.lock().unwrap();
                match connection.as_mut() {
                    Some(conn) => conn.read(&mut buffer).await?,
                    None => {
                        info!("Connection lost, stopping response handler");
                        break;
                    }
                }
            };
            
            if bytes_read == 0 {
                info!("Server closed connection");
                break;
            }
            
            // Decode message
            match self.wire_protocol.decode_from_wire(&buffer[..bytes_read]) {
                Ok(message) => {
                    self.handle_response_message(message).await;
                }
                Err(e) => {
                    warn!("Failed to decode response message: {}", e);
                }
            }
        }
        
        // Mark connection as closed and fail pending requests
        {
            let mut connection = self.connection.lock().unwrap();
            *connection = None;
        }
        
        {
            let mut pending = self.pending_requests.lock().unwrap();
            for (_, request) in pending.drain() {
                let _ = request.response_sender.send(Err(Error::Internal("Connection lost".to_string())));
            }
        }
        
        info!("Response handler finished");
        Ok(())
    }
    
    /// Handle a response message
    async fn handle_response_message(&self, message: RpcMessageType) {
        match message {
            RpcMessageType::Response { id, payload } => {
                debug!("Received response for request {}", id);
                
                let mut pending = self.pending_requests.lock().unwrap();
                if let Some(request) = pending.remove(&id) {
                    let _ = request.response_sender.send(Ok(payload));
                } else {
                    warn!("Received response for unknown request: {}", id);
                }
            }
            
            RpcMessageType::Error { id, code, message } => {
                debug!("Received error response for request {}: {:?} - {}", id, code, message);
                
                let mut pending = self.pending_requests.lock().unwrap();
                if let Some(request) = pending.remove(&id) {
                    let error = RpcError::new(code, message);
                    let _ = request.response_sender.send(Err(error.into()));
                } else {
                    warn!("Received error for unknown request: {}", id);
                }
            }
            
            RpcMessageType::Heartbeat { timestamp: _ } => {
                debug!("Received heartbeat from server");
            }
            
            _ => {
                warn!("Unexpected message type received by client");
            }
        }
    }
}

impl Default for RpcClient {
    fn default() -> Self {
        Self::new()
    }
}

/// RPC client builder for easier configuration
pub struct RpcClientBuilder {
    config: RpcClientConfig,
}

impl RpcClientBuilder {
    /// Create a new client builder
    pub fn new() -> Self {
        Self {
            config: RpcClientConfig::default(),
        }
    }
    
    /// Set server address
    pub fn server_address(mut self, address: SocketAddress) -> Self {
        self.config.server_address = address;
        self
    }
    
    /// Set request timeout
    pub fn request_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.request_timeout = timeout_ms;
        self
    }
    
    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.connection_timeout = timeout_ms;
        self
    }
    
    /// Enable connection pooling
    pub fn connection_pooling(mut self, enabled: bool) -> Self {
        self.config.connection_pooling = enabled;
        self
    }
    
    /// Set maximum connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }
    
    /// Set retry attempts
    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.config.retry_attempts = attempts;
        self
    }
    
    /// Set serialization format
    pub fn serialization_format(mut self, format: SerializationFormat) -> Self {
        // This would need to be implemented in the protocol config
        self
    }
    
    /// Build the RPC client
    pub fn build(self) -> RpcClient {
        RpcClient::with_config(self.config)
    }
}

impl Default for RpcClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_client_creation() {
        let client = RpcClient::new();
        assert!(!client.is_connected());
        assert_eq!(client.pending_request_count(), 0);
    }
    
    #[test]
    fn test_client_builder() {
        let client = RpcClientBuilder::new()
            .request_timeout(5000)
            .connection_timeout(2000)
            .retry_attempts(5)
            .build();
        
        assert_eq!(client.config().request_timeout, 5000);
        assert_eq!(client.config().connection_timeout, 2000);
        assert_eq!(client.config().retry_attempts, 5);
    }
}