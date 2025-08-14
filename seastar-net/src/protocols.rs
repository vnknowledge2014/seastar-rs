//! Protocol definitions and handlers

use crate::buffer::{NetworkBuffer, ZeroCopyBuffer};
use crate::packet::{Packet, PacketHeader, Protocol as PacketProtocol};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, trace, warn};

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Protocol not supported: {0}")]
    NotSupported(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Handler error: {0}")]
    Handler(String),
    #[error("Buffer error: {0}")]
    Buffer(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Protocol types supported by the stack
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    Tcp,
    Udp,
    Http,
    Http2,
    WebSocket,
    Custom(u16),
}

impl From<PacketProtocol> for Protocol {
    fn from(proto: PacketProtocol) -> Self {
        match proto {
            PacketProtocol::Tcp => Protocol::Tcp,
            PacketProtocol::Udp => Protocol::Udp,
            PacketProtocol::Icmp => Protocol::Custom(1), // ICMP
        }
    }
}

impl Protocol {
    pub fn default_port(&self) -> Option<u16> {
        match self {
            Protocol::Http => Some(80),
            Protocol::Http2 => Some(80),
            Protocol::WebSocket => Some(80),
            _ => None,
        }
    }

    pub fn is_connection_oriented(&self) -> bool {
        matches!(self, Protocol::Tcp | Protocol::Http | Protocol::Http2 | Protocol::WebSocket)
    }

    pub fn requires_handshake(&self) -> bool {
        matches!(self, Protocol::Http2 | Protocol::WebSocket)
    }
}

/// Protocol handler trait for processing packets
#[async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Handle an incoming packet
    async fn handle_packet(
        &mut self,
        packet: Packet,
        context: &mut ProtocolContext,
    ) -> ProtocolResult<Vec<Packet>>;

    /// Initialize the protocol handler
    async fn initialize(&mut self, _config: &ProtocolConfig) -> ProtocolResult<()> {
        Ok(())
    }

    /// Cleanup resources
    async fn cleanup(&mut self) -> ProtocolResult<()> {
        Ok(())
    }

    /// Get protocol statistics
    fn stats(&self) -> ProtocolStats {
        ProtocolStats::default()
    }
}

/// Protocol context for handlers
pub struct ProtocolContext {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub protocol: Protocol,
    pub connection_id: Option<u64>,
    pub session_data: HashMap<String, Bytes>,
}

impl ProtocolContext {
    pub fn new(local_addr: SocketAddr, peer_addr: SocketAddr, protocol: Protocol) -> Self {
        Self {
            local_addr,
            peer_addr,
            protocol,
            connection_id: None,
            session_data: HashMap::new(),
        }
    }

    pub fn set_connection_id(&mut self, id: u64) {
        self.connection_id = Some(id);
    }

    pub fn set_session_data(&mut self, key: String, data: Bytes) {
        self.session_data.insert(key, data);
    }

    pub fn get_session_data(&self, key: &str) -> Option<&Bytes> {
        self.session_data.get(key)
    }
}

/// Protocol configuration
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub max_packet_size: usize,
    pub timeout: std::time::Duration,
    pub buffer_size: usize,
    pub custom_params: HashMap<String, String>,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            max_packet_size: 64 * 1024,
            timeout: std::time::Duration::from_secs(30),
            buffer_size: 8 * 1024,
            custom_params: HashMap::new(),
        }
    }
}

/// Protocol statistics
#[derive(Debug, Default, Clone)]
pub struct ProtocolStats {
    pub packets_handled: u64,
    pub bytes_processed: u64,
    pub errors: u64,
    pub active_connections: u64,
}

/// HTTP protocol handler
pub struct HttpHandler {
    config: ProtocolConfig,
    stats: ProtocolStats,
}

impl HttpHandler {
    pub fn new() -> Self {
        Self {
            config: ProtocolConfig::default(),
            stats: ProtocolStats::default(),
        }
    }

    fn parse_http_request(&self, data: &[u8]) -> ProtocolResult<HttpRequest> {
        let request_str = std::str::from_utf8(data)
            .map_err(|e| ProtocolError::Parse(format!("Invalid UTF-8: {}", e)))?;

        let mut lines = request_str.lines();
        let first_line = lines.next()
            .ok_or_else(|| ProtocolError::Parse("Empty request".to_string()))?;

        let mut parts = first_line.split_whitespace();
        let method = parts.next()
            .ok_or_else(|| ProtocolError::Parse("Missing method".to_string()))?;
        let path = parts.next()
            .ok_or_else(|| ProtocolError::Parse("Missing path".to_string()))?;
        let version = parts.next()
            .ok_or_else(|| ProtocolError::Parse("Missing version".to_string()))?;

        let mut headers = HashMap::new();
        for line in lines {
            if line.is_empty() {
                break;
            }
            
            if let Some((key, value)) = line.split_once(':') {
                headers.insert(
                    key.trim().to_lowercase(),
                    value.trim().to_string(),
                );
            }
        }

        Ok(HttpRequest {
            method: method.to_string(),
            path: path.to_string(),
            version: version.to_string(),
            headers,
            body: Vec::new(),
        })
    }

    fn create_http_response(&self, status: u16, body: &str) -> Vec<u8> {
        let status_text = match status {
            200 => "OK",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "Unknown",
        };

        format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\n\r\n{}",
            status, status_text, body.len(), body
        ).into_bytes()
    }
}

#[async_trait]
impl ProtocolHandler for HttpHandler {
    async fn handle_packet(
        &mut self,
        packet: Packet,
        context: &mut ProtocolContext,
    ) -> ProtocolResult<Vec<Packet>> {
        trace!("Handling HTTP packet from {}", context.peer_addr);
        
        // Parse HTTP request
        let request = self.parse_http_request(&packet.payload)?;
        debug!("HTTP {} {} from {}", request.method, request.path, context.peer_addr);

        // Simple response based on path
        let (status, body) = match request.path.as_str() {
            "/" => (200, "Hello, World!"),
            "/health" => (200, "OK"),
            _ => (404, "Not Found"),
        };

        let response_data = self.create_http_response(status, body);
        
        // Create response packet
        let response_packet = Packet::new(
            PacketHeader {
                src_addr: context.local_addr,
                dst_addr: context.peer_addr,
                protocol: PacketProtocol::Tcp,
                flags: Default::default(),
                sequence: None,
                acknowledgment: None,
                window_size: None,
            },
            Bytes::from(response_data),
        );

        self.stats.packets_handled += 1;
        self.stats.bytes_processed += packet.payload.len() as u64;

        Ok(vec![response_packet])
    }

    async fn initialize(&mut self, config: &ProtocolConfig) -> ProtocolResult<()> {
        self.config = config.clone();
        debug!("HTTP handler initialized");
        Ok(())
    }

    fn stats(&self) -> ProtocolStats {
        self.stats.clone()
    }
}

impl Default for HttpHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    version: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// WebSocket protocol handler
pub struct WebSocketHandler {
    config: ProtocolConfig,
    stats: ProtocolStats,
    handshake_complete: bool,
}

impl WebSocketHandler {
    pub fn new() -> Self {
        Self {
            config: ProtocolConfig::default(),
            stats: ProtocolStats::default(),
            handshake_complete: false,
        }
    }

    fn handle_websocket_handshake(&mut self, data: &[u8]) -> ProtocolResult<Vec<u8>> {
        // Simplified WebSocket handshake
        let request = std::str::from_utf8(data)
            .map_err(|e| ProtocolError::Parse(format!("Invalid UTF-8: {}", e)))?;

        if request.contains("Upgrade: websocket") {
            self.handshake_complete = true;
            
            let response = "HTTP/1.1 101 Switching Protocols\r\n\
                           Upgrade: websocket\r\n\
                           Connection: Upgrade\r\n\
                           Sec-WebSocket-Accept: dummy-accept-key\r\n\r\n";
            
            Ok(response.as_bytes().to_vec())
        } else {
            Err(ProtocolError::Parse("Invalid WebSocket handshake".to_string()))
        }
    }

    fn handle_websocket_frame(&self, data: &[u8]) -> ProtocolResult<Vec<u8>> {
        // Simplified frame handling - just echo back
        if data.len() >= 2 {
            let mut response = Vec::new();
            response.push(0x81); // Text frame, FIN bit set
            response.push(data.len() as u8); // Payload length (simplified)
            response.extend_from_slice(data);
            Ok(response)
        } else {
            Err(ProtocolError::Parse("Invalid WebSocket frame".to_string()))
        }
    }
}

#[async_trait]
impl ProtocolHandler for WebSocketHandler {
    async fn handle_packet(
        &mut self,
        packet: Packet,
        context: &mut ProtocolContext,
    ) -> ProtocolResult<Vec<Packet>> {
        trace!("Handling WebSocket packet from {}", context.peer_addr);

        let response_data = if !self.handshake_complete {
            // Handle handshake
            self.handle_websocket_handshake(&packet.payload)?
        } else {
            // Handle frame
            self.handle_websocket_frame(&packet.payload)?
        };

        let response_packet = Packet::new(
            PacketHeader {
                src_addr: context.local_addr,
                dst_addr: context.peer_addr,
                protocol: PacketProtocol::Tcp,
                flags: Default::default(),
                sequence: None,
                acknowledgment: None,
                window_size: None,
            },
            Bytes::from(response_data),
        );

        self.stats.packets_handled += 1;
        self.stats.bytes_processed += packet.payload.len() as u64;

        Ok(vec![response_packet])
    }

    async fn initialize(&mut self, config: &ProtocolConfig) -> ProtocolResult<()> {
        self.config = config.clone();
        debug!("WebSocket handler initialized");
        Ok(())
    }

    fn stats(&self) -> ProtocolStats {
        self.stats.clone()
    }
}

impl Default for WebSocketHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Protocol registry for managing handlers
pub struct ProtocolRegistry {
    handlers: Arc<Mutex<HashMap<Protocol, Box<dyn ProtocolHandler>>>>,
    default_config: ProtocolConfig,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(HashMap::new())),
            default_config: ProtocolConfig::default(),
        }
    }

    pub async fn register_handler(
        &self,
        protocol: Protocol,
        mut handler: Box<dyn ProtocolHandler>,
    ) -> ProtocolResult<()> {
        handler.initialize(&self.default_config).await?;
        self.handlers.lock().await.insert(protocol, handler);
        debug!("Registered handler for protocol {:?}", protocol);
        Ok(())
    }

    pub async fn handle_packet(
        &self,
        protocol: Protocol,
        packet: Packet,
        context: &mut ProtocolContext,
    ) -> ProtocolResult<Vec<Packet>> {
        let mut handlers = self.handlers.lock().await;
        
        if let Some(handler) = handlers.get_mut(&protocol) {
            handler.handle_packet(packet, context).await
        } else {
            warn!("No handler registered for protocol {:?}", protocol);
            Err(ProtocolError::NotSupported(format!("{:?}", protocol)))
        }
    }

    pub async fn get_stats(&self, protocol: Protocol) -> Option<ProtocolStats> {
        let handlers = self.handlers.lock().await;
        handlers.get(&protocol).map(|handler| handler.stats())
    }

    pub async fn shutdown(&self) -> ProtocolResult<()> {
        let mut handlers = self.handlers.lock().await;
        
        for (protocol, handler) in handlers.iter_mut() {
            if let Err(e) = handler.cleanup().await {
                warn!("Error cleaning up handler for {:?}: {}", protocol, e);
            }
        }
        
        handlers.clear();
        debug!("Protocol registry shutdown completed");
        Ok(())
    }
}

impl Default for ProtocolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a default protocol registry with standard handlers
pub async fn create_default_registry() -> ProtocolResult<ProtocolRegistry> {
    let registry = ProtocolRegistry::new();
    
    // Register HTTP handler
    registry.register_handler(
        Protocol::Http,
        Box::new(HttpHandler::new()),
    ).await?;
    
    // Register WebSocket handler
    registry.register_handler(
        Protocol::WebSocket,
        Box::new(WebSocketHandler::new()),
    ).await?;
    
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[tokio::test]
    async fn test_protocol_registry() {
        let registry = ProtocolRegistry::new();
        
        let handler = Box::new(HttpHandler::new());
        let result = registry.register_handler(Protocol::Http, handler).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_http_handler() {
        let mut handler = HttpHandler::new();
        let config = ProtocolConfig::default();
        
        handler.initialize(&config).await.unwrap();
        
        let request_data = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let packet = Packet::new(
            PacketHeader {
                src_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
                dst_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
                protocol: PacketProtocol::Tcp,
                flags: Default::default(),
                sequence: None,
                acknowledgment: None,
                window_size: None,
            },
            Bytes::from(&request_data[..]),
        );
        
        let mut context = ProtocolContext::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            Protocol::Http,
        );
        
        let result = handler.handle_packet(packet, &mut context).await;
        assert!(result.is_ok());
        
        let responses = result.unwrap();
        assert_eq!(responses.len(), 1);
        
        let response = &responses[0];
        let response_str = std::str::from_utf8(&response.payload).unwrap();
        assert!(response_str.contains("200 OK"));
        assert!(response_str.contains("Hello, World!"));
    }

    #[tokio::test]
    async fn test_websocket_handler() {
        let mut handler = WebSocketHandler::new();
        let config = ProtocolConfig::default();
        
        handler.initialize(&config).await.unwrap();
        
        // Test handshake
        let handshake_data = b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n";
        let packet = Packet::new(
            PacketHeader {
                src_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
                dst_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
                protocol: PacketProtocol::Tcp,
                flags: Default::default(),
                sequence: None,
                acknowledgment: None,
                window_size: None,
            },
            Bytes::from(&handshake_data[..]),
        );
        
        let mut context = ProtocolContext::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80)),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            Protocol::WebSocket,
        );
        
        let result = handler.handle_packet(packet, &mut context).await;
        assert!(result.is_ok());
        
        let responses = result.unwrap();
        assert_eq!(responses.len(), 1);
        
        let response = &responses[0];
        let response_str = std::str::from_utf8(&response.payload).unwrap();
        assert!(response_str.contains("101 Switching Protocols"));
    }

    #[test]
    fn test_protocol_properties() {
        assert_eq!(Protocol::Http.default_port(), Some(80));
        assert!(Protocol::Tcp.is_connection_oriented());
        assert!(!Protocol::Udp.is_connection_oriented());
        assert!(Protocol::WebSocket.requires_handshake());
        assert!(!Protocol::Tcp.requires_handshake());
    }

    #[tokio::test]
    async fn test_default_registry() {
        let registry = create_default_registry().await.unwrap();
        
        let http_stats = registry.get_stats(Protocol::Http).await;
        assert!(http_stats.is_some());
        
        let websocket_stats = registry.get_stats(Protocol::WebSocket).await;
        assert!(websocket_stats.is_some());
    }
}