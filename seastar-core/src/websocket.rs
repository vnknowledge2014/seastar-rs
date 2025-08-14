//! WebSocket protocol support for Seastar-RS
//!
//! Provides WebSocket server and client functionality compatible with RFC 6455

use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::{
    WebSocketStream, accept_async, client_async,
    tungstenite::{Message, protocol::CloseFrame}
};
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, Deserialize};
use crate::{Result, Error};

/// WebSocket connection ID
pub type ConnectionId = u64;

/// WebSocket message types
#[derive(Debug, Clone, PartialEq)]
pub enum WebSocketMessage {
    /// Text message
    Text(String),
    /// Binary message
    Binary(Vec<u8>),
    /// Ping frame
    Ping(Vec<u8>),
    /// Pong frame
    Pong(Vec<u8>),
    /// Close frame
    Close(Option<CloseFrame<'static>>),
}

impl From<Message> for WebSocketMessage {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Text(text) => WebSocketMessage::Text(text),
            Message::Binary(data) => WebSocketMessage::Binary(data),
            Message::Ping(data) => WebSocketMessage::Ping(data),
            Message::Pong(data) => WebSocketMessage::Pong(data),
            Message::Close(frame) => WebSocketMessage::Close(frame.map(|f| CloseFrame {
                code: f.code,
                reason: f.reason.into_owned().into(),
            })),
            Message::Frame(_) => WebSocketMessage::Binary(vec![]), // Raw frames not supported
        }
    }
}

impl Into<Message> for WebSocketMessage {
    fn into(self) -> Message {
        match self {
            WebSocketMessage::Text(text) => Message::Text(text),
            WebSocketMessage::Binary(data) => Message::Binary(data),
            WebSocketMessage::Ping(data) => Message::Ping(data),
            WebSocketMessage::Pong(data) => Message::Pong(data),
            WebSocketMessage::Close(frame) => Message::Close(frame),
        }
    }
}

/// WebSocket connection handler trait
#[async_trait::async_trait]
pub trait WebSocketHandler: Send + Sync + 'static {
    /// Called when a new connection is established
    async fn on_connect(&self, conn_id: ConnectionId, request_uri: &str) -> Result<()>;
    
    /// Called when a message is received
    async fn on_message(&self, conn_id: ConnectionId, message: WebSocketMessage) -> Result<()>;
    
    /// Called when a connection is closed
    async fn on_disconnect(&self, conn_id: ConnectionId, reason: Option<String>) -> Result<()>;
    
    /// Called when an error occurs
    async fn on_error(&self, conn_id: ConnectionId, error: Error) -> Result<()>;
}

/// WebSocket connection wrapper
pub struct WebSocketConnection {
    id: ConnectionId,
    stream: WebSocketStream<TcpStream>,
    remote_addr: std::net::SocketAddr,
    request_uri: String,
}

impl WebSocketConnection {
    /// Create new WebSocket connection
    pub fn new(
        id: ConnectionId,
        stream: WebSocketStream<TcpStream>,
        remote_addr: std::net::SocketAddr,
        request_uri: String,
    ) -> Self {
        Self {
            id,
            stream,
            remote_addr,
            request_uri,
        }
    }
    
    /// Get connection ID
    pub fn id(&self) -> ConnectionId {
        self.id
    }
    
    /// Get remote address
    pub fn remote_addr(&self) -> std::net::SocketAddr {
        self.remote_addr
    }
    
    /// Get request URI
    pub fn request_uri(&self) -> &str {
        &self.request_uri
    }
    
    /// Send a message
    pub async fn send(&mut self, message: WebSocketMessage) -> Result<()> {
        self.stream.send(message.into()).await
            .map_err(|e| Error::Network(format!("Failed to send WebSocket message: {}", e)))
    }
    
    /// Receive a message
    pub async fn receive(&mut self) -> Result<Option<WebSocketMessage>> {
        match self.stream.next().await {
            Some(Ok(msg)) => Ok(Some(msg.into())),
            Some(Err(e)) => Err(Error::Network(format!("WebSocket receive error: {}", e))),
            None => Ok(None),
        }
    }
    
    /// Close the connection
    pub async fn close(&mut self, reason: Option<CloseFrame<'static>>) -> Result<()> {
        self.stream.send(Message::Close(reason)).await
            .map_err(|e| Error::Network(format!("Failed to close WebSocket: {}", e)))
    }
}

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WebSocketServerConfig {
    /// Maximum message size (default: 16MB)
    pub max_message_size: usize,
    /// Maximum frame size (default: 16MB) 
    pub max_frame_size: usize,
    /// Accept unmasked frames (default: false for security)
    pub accept_unmasked_frames: bool,
    /// Enable compression (default: true)
    pub enable_compression: bool,
    /// Connection timeout (default: 30s)
    pub connection_timeout: std::time::Duration,
}

impl Default for WebSocketServerConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16MB
            max_frame_size: 16 * 1024 * 1024,   // 16MB
            accept_unmasked_frames: false,
            enable_compression: true,
            connection_timeout: std::time::Duration::from_secs(30),
        }
    }
}

/// WebSocket server
pub struct WebSocketServer {
    listener: TcpListener,
    config: WebSocketServerConfig,
    connections: Arc<RwLock<HashMap<ConnectionId, Arc<tokio::sync::Mutex<WebSocketConnection>>>>>,
    next_conn_id: Arc<AtomicU64>,
    handler: Arc<dyn WebSocketHandler>,
}

impl WebSocketServer {
    /// Create a new WebSocket server
    pub async fn bind<A: tokio::net::ToSocketAddrs>(
        addr: A,
        handler: Arc<dyn WebSocketHandler>,
    ) -> Result<Self> {
        Self::bind_with_config(addr, handler, WebSocketServerConfig::default()).await
    }
    
    /// Create a new WebSocket server with custom configuration
    pub async fn bind_with_config<A: tokio::net::ToSocketAddrs>(
        addr: A,
        handler: Arc<dyn WebSocketHandler>,
        config: WebSocketServerConfig,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await
            .map_err(|e| Error::Network(format!("Failed to bind WebSocket server: {}", e)))?;
        
        Ok(Self {
            listener,
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            next_conn_id: Arc::new(AtomicU64::new(1)),
            handler,
        })
    }
    
    /// Get the local address the server is bound to
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr()
            .map_err(|e| Error::Network(format!("Failed to get local address: {}", e)))
    }
    
    /// Run the WebSocket server
    pub async fn run(&self) -> Result<()> {
        tracing::info!("WebSocket server listening on {}", self.local_addr()?);
        
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
                    let connections = self.connections.clone();
                    let handler = self.handler.clone();
                    let config = self.config.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            conn_id,
                            stream,
                            addr,
                            connections,
                            handler,
                            config,
                        ).await {
                            tracing::error!("WebSocket connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to accept WebSocket connection: {}", e);
                }
            }
        }
    }
    
    /// Handle a single WebSocket connection
    async fn handle_connection(
        conn_id: ConnectionId,
        stream: TcpStream,
        addr: std::net::SocketAddr,
        connections: Arc<RwLock<HashMap<ConnectionId, Arc<tokio::sync::Mutex<WebSocketConnection>>>>>,
        handler: Arc<dyn WebSocketHandler>,
        config: WebSocketServerConfig,
    ) -> Result<()> {
        tracing::debug!("New WebSocket connection from {}", addr);
        
        // Perform WebSocket handshake
        let ws_stream = match tokio::time::timeout(
            config.connection_timeout,
            accept_async(stream)
        ).await {
            Ok(Ok(ws)) => ws,
            Ok(Err(e)) => {
                tracing::error!("WebSocket handshake failed: {}", e);
                return Err(Error::Network(format!("WebSocket handshake failed: {}", e)));
            }
            Err(_) => {
                tracing::error!("WebSocket handshake timeout");
                return Err(Error::Timeout);
            }
        };
        
        // Create connection wrapper
        let connection = WebSocketConnection::new(
            conn_id,
            ws_stream,
            addr,
            "/".to_string(), // TODO: Extract actual request URI from handshake
        );
        
        // Register connection
        let connection_arc = Arc::new(tokio::sync::Mutex::new(connection));
        connections.write().await.insert(conn_id, connection_arc.clone());
        
        // Notify handler of new connection
        if let Err(e) = handler.on_connect(conn_id, "/").await {
            tracing::error!("Handler on_connect error: {}", e);
        }
        
        // Message loop
        let mut connection_guard = connection_arc.lock().await;
        loop {
            match connection_guard.receive().await {
                Ok(Some(message)) => {
                    // Handle different message types
                    match &message {
                        WebSocketMessage::Close(frame) => {
                            let reason = frame.as_ref()
                                .map(|f| f.reason.to_string())
                                .unwrap_or_else(|| "Connection closed".to_string());
                            
                            if let Err(e) = handler.on_disconnect(conn_id, Some(reason)).await {
                                tracing::error!("Handler on_disconnect error: {}", e);
                            }
                            break;
                        }
                        WebSocketMessage::Ping(data) => {
                            // Respond with pong
                            let pong = WebSocketMessage::Pong(data.clone());
                            if let Err(e) = connection_guard.send(pong).await {
                                tracing::error!("Failed to send pong: {}", e);
                                break;
                            }
                        }
                        _ => {
                            // Forward message to handler
                            if let Err(e) = handler.on_message(conn_id, message).await {
                                tracing::error!("Handler on_message error: {}", e);
                                if let Err(e) = handler.on_error(conn_id, e).await {
                                    tracing::error!("Handler on_error failed: {}", e);
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Connection closed
                    if let Err(e) = handler.on_disconnect(conn_id, None).await {
                        tracing::error!("Handler on_disconnect error: {}", e);
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!("WebSocket receive error: {}", e);
                    if let Err(e) = handler.on_error(conn_id, e).await {
                        tracing::error!("Handler on_error failed: {}", e);
                    }
                    break;
                }
            }
        }
        
        // Cleanup connection
        drop(connection_guard);
        connections.write().await.remove(&conn_id);
        tracing::debug!("WebSocket connection {} closed", conn_id);
        
        Ok(())
    }
    
    /// Send a message to a specific connection
    pub async fn send_to(&self, conn_id: ConnectionId, message: WebSocketMessage) -> Result<()> {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(&conn_id) {
            let mut conn = connection.lock().await;
            conn.send(message).await
        } else {
            Err(Error::InvalidArgument(format!("Connection {} not found", conn_id)))
        }
    }
    
    /// Broadcast a message to all connected clients
    pub async fn broadcast(&self, message: WebSocketMessage) -> Result<()> {
        let connections = self.connections.read().await;
        let mut errors = Vec::new();
        
        for (conn_id, connection) in connections.iter() {
            let mut conn = connection.lock().await;
            if let Err(e) = conn.send(message.clone()).await {
                errors.push((*conn_id, e));
            }
        }
        
        if !errors.is_empty() {
            tracing::warn!("Broadcast errors: {:?}", errors);
        }
        
        Ok(())
    }
    
    /// Get the number of active connections
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
    
    /// Close a specific connection
    pub async fn close_connection(&self, conn_id: ConnectionId, reason: Option<String>) -> Result<()> {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(&conn_id) {
            let mut conn = connection.lock().await;
            let close_frame = reason.map(|r| CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: r.into(),
            });
            conn.close(close_frame).await
        } else {
            Err(Error::InvalidArgument(format!("Connection {} not found", conn_id)))
        }
    }
}

/// WebSocket client
pub struct WebSocketClient {
    stream: WebSocketStream<TcpStream>,
    remote_addr: String,
}

impl WebSocketClient {
    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self> {
        // Parse URL to extract host and port
        let uri: http::Uri = url.parse()
            .map_err(|e| Error::InvalidArgument(format!("Invalid URL: {}", e)))?;
        
        let host = uri.host().ok_or_else(|| Error::InvalidArgument("No host in URL".to_string()))?;
        let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("wss") { 443 } else { 80 });
        
        let tcp_stream = TcpStream::connect(format!("{}:{}", host, port)).await
            .map_err(|e| Error::Network(format!("Failed to connect: {}", e)))?;
        
        let (ws_stream, _response) = client_async(url, tcp_stream)
            .await
            .map_err(|e| Error::Network(format!("WebSocket connection failed: {}", e)))?;
        
        Ok(Self {
            stream: ws_stream,
            remote_addr: url.to_string(),
        })
    }
    
    /// Send a message
    pub async fn send(&mut self, message: WebSocketMessage) -> Result<()> {
        self.stream.send(message.into()).await
            .map_err(|e| Error::Network(format!("Failed to send message: {}", e)))
    }
    
    /// Receive a message
    pub async fn receive(&mut self) -> Result<Option<WebSocketMessage>> {
        match self.stream.next().await {
            Some(Ok(msg)) => Ok(Some(msg.into())),
            Some(Err(e)) => Err(Error::Network(format!("Receive error: {}", e))),
            None => Ok(None),
        }
    }
    
    /// Close the connection
    pub async fn close(&mut self) -> Result<()> {
        self.stream.send(Message::Close(None)).await
            .map_err(|e| Error::Network(format!("Failed to close connection: {}", e)))
    }
    
    /// Get remote address
    pub fn remote_addr(&self) -> &str {
        &self.remote_addr
    }
}

/// JSON WebSocket message for typed communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMessage<T> {
    pub message_type: String,
    pub data: T,
    pub timestamp: u64,
}

impl<T: Serialize + for<'de> Deserialize<'de>> JsonMessage<T> {
    /// Create a new JSON message
    pub fn new(message_type: String, data: T) -> Self {
        Self {
            message_type,
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Convert to WebSocket text message
    pub fn to_websocket_message(&self) -> Result<WebSocketMessage> {
        let json = serde_json::to_string(self)
            .map_err(|e| Error::Internal(format!("JSON serialization error: {}", e)))?;
        Ok(WebSocketMessage::Text(json))
    }
    
    /// Create from WebSocket text message
    pub fn from_websocket_message(message: WebSocketMessage) -> Result<Self> {
        match message {
            WebSocketMessage::Text(text) => {
                serde_json::from_str(&text)
                    .map_err(|e| Error::Internal(format!("JSON deserialization error: {}", e)))
            }
            _ => Err(Error::InvalidArgument("Expected text message".to_string())),
        }
    }
}

/// Example WebSocket handler implementation
pub struct EchoHandler;

#[async_trait::async_trait]
impl WebSocketHandler for EchoHandler {
    async fn on_connect(&self, conn_id: ConnectionId, request_uri: &str) -> Result<()> {
        tracing::info!("Client {} connected to {}", conn_id, request_uri);
        Ok(())
    }
    
    async fn on_message(&self, conn_id: ConnectionId, message: WebSocketMessage) -> Result<()> {
        tracing::debug!("Received message from {}: {:?}", conn_id, message);
        
        // Echo the message back (this would be handled by the server)
        // In a real implementation, you would get a reference to the server
        // and call send_to(conn_id, message)
        
        Ok(())
    }
    
    async fn on_disconnect(&self, conn_id: ConnectionId, reason: Option<String>) -> Result<()> {
        tracing::info!(
            "Client {} disconnected: {}",
            conn_id,
            reason.unwrap_or_else(|| "Unknown".to_string())
        );
        Ok(())
    }
    
    async fn on_error(&self, conn_id: ConnectionId, error: Error) -> Result<()> {
        tracing::error!("Error on connection {}: {}", conn_id, error);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};
    
    #[tokio::test]
    async fn test_websocket_message_conversion() {
        let msg = WebSocketMessage::Text("Hello, World!".to_string());
        let tungstenite_msg: Message = msg.clone().into();
        let converted_back: WebSocketMessage = tungstenite_msg.into();
        
        assert_eq!(msg, converted_back);
    }
    
    #[tokio::test]
    async fn test_json_message() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct TestData {
            value: i32,
            text: String,
        }
        
        let data = TestData {
            value: 42,
            text: "test".to_string(),
        };
        
        let json_msg = JsonMessage::new("test_message".to_string(), data.clone());
        let ws_msg = json_msg.to_websocket_message().unwrap();
        let recovered = JsonMessage::<TestData>::from_websocket_message(ws_msg).unwrap();
        
        assert_eq!(json_msg.message_type, recovered.message_type);
        assert_eq!(json_msg.data, recovered.data);
    }
    
    #[test]
    fn test_websocket_server_config() {
        let config = WebSocketServerConfig::default();
        assert_eq!(config.max_message_size, 16 * 1024 * 1024);
        assert!(!config.accept_unmasked_frames);
        assert!(config.enable_compression);
    }
    
    #[test]
    fn test_echo_handler() {
        let handler = EchoHandler;
        // Test that handler implements the trait correctly
        // Actual async testing would require a running server
    }
}