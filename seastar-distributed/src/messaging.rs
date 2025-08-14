//! Distributed messaging system

use crate::node::{NodeId, NodeInfo};
use crate::{DistributedError, DistributedResult};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Message types in the distributed system
#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    /// Heartbeat/keepalive message
    Heartbeat,
    /// Node join request
    JoinRequest,
    /// Node join response
    JoinResponse,
    /// Node leave notification
    LeaveNotification,
    /// Gossip protocol message
    Gossip,
    /// Consensus protocol message
    Consensus,
    /// Application-specific message
    Application,
    /// Health check message
    HealthCheck,
    /// Leader election message
    Election,
}

/// Distributed message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedMessage {
    /// Unique message identifier
    pub id: Uuid,
    /// Source node ID
    pub sender: NodeId,
    /// Destination node ID (None for broadcast)
    pub recipient: Option<NodeId>,
    /// Message type
    pub message_type: MessageType,
    /// Message payload
    pub payload: Vec<u8>,
    /// Timestamp when message was created
    pub timestamp: SystemTime,
    /// Time-to-live for the message
    pub ttl: Duration,
}

impl DistributedMessage {
    /// Create a new message
    pub fn new(
        sender: NodeId,
        recipient: Option<NodeId>,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            sender,
            recipient,
            message_type,
            payload,
            timestamp: SystemTime::now(),
            ttl: Duration::from_secs(30), // Default TTL
        }
    }

    /// Create a heartbeat message
    pub fn heartbeat(sender: NodeId) -> Self {
        Self::new(sender, None, MessageType::Heartbeat, Vec::new())
    }

    /// Create a join request message
    pub fn join_request(sender: NodeId, node_info: NodeInfo) -> Self {
        let payload = bincode::serialize(&node_info).unwrap_or_default();
        Self::new(sender, None, MessageType::JoinRequest, payload)
    }

    /// Create a join response message
    pub fn join_response(sender: NodeId, recipient: NodeId, accepted: bool) -> Self {
        let payload = bincode::serialize(&accepted).unwrap_or_default();
        Self::new(sender, Some(recipient), MessageType::JoinResponse, payload)
    }

    /// Create a leave notification message
    pub fn leave_notification(sender: NodeId) -> Self {
        Self::new(sender, None, MessageType::LeaveNotification, Vec::new())
    }

    /// Create a gossip message
    pub fn gossip(sender: NodeId, gossip_data: Vec<u8>) -> Self {
        Self::new(sender, None, MessageType::Gossip, gossip_data)
    }

    /// Create an application message
    pub fn application(sender: NodeId, recipient: Option<NodeId>, data: Vec<u8>) -> Self {
        Self::new(sender, recipient, MessageType::Application, data)
    }

    /// Check if the message has expired
    pub fn is_expired(&self) -> bool {
        if let Ok(age) = self.timestamp.elapsed() {
            age > self.ttl
        } else {
            true
        }
    }

    /// Set TTL for the message
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Serialize the message for transmission
    pub fn serialize(&self) -> DistributedResult<Vec<u8>> {
        bincode::serialize(self).map_err(DistributedError::from)
    }

    /// Deserialize a message from bytes
    pub fn deserialize(data: &[u8]) -> DistributedResult<Self> {
        bincode::deserialize(data).map_err(DistributedError::from)
    }
}

/// Handler for processing distributed messages
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Handle an incoming message
    async fn handle_message(&self, message: DistributedMessage) -> DistributedResult<Option<DistributedMessage>>;
    
    /// Get the message types this handler is interested in
    fn message_types(&self) -> Vec<MessageType>;
}

/// Message bus for handling distributed communication
pub struct MessageBus {
    local_addr: SocketAddr,
    tcp_listener: Option<Arc<TcpListener>>,
    udp_socket: Option<Arc<UdpSocket>>,
    handlers: Arc<RwLock<HashMap<MessageType, Vec<Arc<dyn MessageHandler>>>>>,
    outgoing_queue: Arc<Mutex<mpsc::UnboundedSender<(SocketAddr, DistributedMessage)>>>,
    connection_pool: Arc<RwLock<HashMap<SocketAddr, Arc<Mutex<TcpStream>>>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl MessageBus {
    /// Create a new message bus
    pub async fn new(local_addr: SocketAddr) -> DistributedResult<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        let bus = Self {
            local_addr,
            tcp_listener: None,
            udp_socket: None,
            handlers: Arc::new(RwLock::new(HashMap::new())),
            outgoing_queue: Arc::new(Mutex::new(sender)),
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // Start the outgoing message processor
        bus.start_outgoing_processor(receiver).await;

        Ok(bus)
    }

    /// Start the message bus
    pub async fn start(&self) -> DistributedResult<()> {
        info!("Starting message bus on {}", self.local_addr);
        
        self.running.store(true, std::sync::atomic::Ordering::SeqCst);

        // Start TCP listener
        self.start_tcp_listener().await?;
        
        // Start UDP socket
        self.start_udp_socket().await?;

        info!("Message bus started successfully");
        Ok(())
    }

    /// Stop the message bus
    pub async fn stop(&self) -> DistributedResult<()> {
        info!("Stopping message bus");
        
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
        
        // Close connections
        let mut pool = self.connection_pool.write().await;
        pool.clear();

        info!("Message bus stopped");
        Ok(())
    }

    /// Register a message handler
    pub async fn register_handler(&self, handler: Arc<dyn MessageHandler>) {
        let mut handlers = self.handlers.write().await;
        
        for msg_type in handler.message_types() {
            handlers.entry(msg_type).or_insert_with(Vec::new).push(handler.clone());
        }
        
        debug!("Registered message handler for types: {:?}", handler.message_types());
    }

    /// Send a message to a specific address
    pub async fn send_to(&self, addr: SocketAddr, message: DistributedMessage) -> DistributedResult<()> {
        if message.is_expired() {
            return Err(DistributedError::Network("Message expired".to_string()));
        }

        let sender = self.outgoing_queue.lock().await;
        sender.send((addr, message))
            .map_err(|_| DistributedError::Network("Failed to queue message".to_string()))?;
        
        Ok(())
    }

    /// Broadcast a message (placeholder implementation)
    pub async fn broadcast(&self, message: DistributedMessage) -> DistributedResult<()> {
        // In a real implementation, this would send to all known nodes
        debug!("Broadcasting message: {:?}", message.message_type);
        Ok(())
    }

    // Private methods

    async fn start_tcp_listener(&self) -> DistributedResult<()> {
        let listener = TcpListener::bind(self.local_addr).await
            .map_err(|e| DistributedError::Network(format!("Failed to bind TCP listener: {}", e)))?;
        
        let listener = Arc::new(listener);
        
        // Clone for the listening task
        let listener_clone = listener.clone();
        let handlers = self.handlers.clone();
        let running = self.running.clone();
        
        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                match listener_clone.accept().await {
                    Ok((stream, addr)) => {
                        let handlers = handlers.clone();
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_tcp_connection(stream, handlers).await {
                                warn!("Error handling TCP connection from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept TCP connection: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn start_udp_socket(&self) -> DistributedResult<()> {
        let socket = UdpSocket::bind(self.local_addr).await
            .map_err(|e| DistributedError::Network(format!("Failed to bind UDP socket: {}", e)))?;
        
        let socket = Arc::new(socket);
        
        // Clone for the receiving task
        let socket_clone = socket.clone();
        let handlers = self.handlers.clone();
        let running = self.running.clone();
        
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 65536]; // 64KB buffer
            
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                match socket_clone.recv_from(&mut buffer).await {
                    Ok((len, addr)) => {
                        if let Ok(message) = DistributedMessage::deserialize(&buffer[..len]) {
                            if !message.is_expired() {
                                let handlers = handlers.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_message(message, handlers).await {
                                        warn!("Error handling UDP message from {}: {}", addr, e);
                                    }
                                });
                            }
                        }
                    }
                    Err(e) => {
                        warn!("UDP receive error: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn start_outgoing_processor(&self, mut receiver: mpsc::UnboundedReceiver<(SocketAddr, DistributedMessage)>) {
        let connection_pool = self.connection_pool.clone();
        
        tokio::spawn(async move {
            while let Some((addr, message)) = receiver.recv().await {
                if let Err(e) = Self::send_message_to_addr(addr, message, connection_pool.clone()).await {
                    warn!("Failed to send message to {}: {}", addr, e);
                }
            }
        });
    }

    async fn send_message_to_addr(
        addr: SocketAddr,
        message: DistributedMessage,
        connection_pool: Arc<RwLock<HashMap<SocketAddr, Arc<Mutex<TcpStream>>>>>,
    ) -> DistributedResult<()> {
        // Try to get existing connection
        let connection = {
            let pool = connection_pool.read().await;
            pool.get(&addr).cloned()
        };

        let stream = if let Some(conn) = connection {
            conn
        } else {
            // Create new connection
            let stream = timeout(Duration::from_secs(5), TcpStream::connect(addr)).await
                .map_err(|_| DistributedError::Network("Connection timeout".to_string()))?
                .map_err(|e| DistributedError::Network(format!("Connection failed: {}", e)))?;
            
            let stream = Arc::new(Mutex::new(stream));
            
            // Store in pool
            {
                let mut pool = connection_pool.write().await;
                pool.insert(addr, stream.clone());
            }
            
            stream
        };

        // Send message
        let data = message.serialize()?;
        use tokio::io::AsyncWriteExt;
        
        let mut stream_guard = stream.lock().await;
        stream_guard.write_all(&(data.len() as u32).to_be_bytes()).await
            .map_err(|e| DistributedError::Network(format!("Write failed: {}", e)))?;
        stream_guard.write_all(&data).await
            .map_err(|e| DistributedError::Network(format!("Write failed: {}", e)))?;
        stream_guard.flush().await
            .map_err(|e| DistributedError::Network(format!("Flush failed: {}", e)))?;

        Ok(())
    }

    async fn handle_tcp_connection(
        mut stream: TcpStream,
        handlers: Arc<RwLock<HashMap<MessageType, Vec<Arc<dyn MessageHandler>>>>>,
    ) -> DistributedResult<()> {
        use tokio::io::AsyncReadExt;
        
        loop {
            // Read message length
            let mut len_bytes = [0u8; 4];
            if stream.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            
            let msg_len = u32::from_be_bytes(len_bytes) as usize;
            if msg_len > 1024 * 1024 { // 1MB max message size
                break;
            }
            
            // Read message data
            let mut data = vec![0u8; msg_len];
            if stream.read_exact(&mut data).await.is_err() {
                break;
            }
            
            // Deserialize and handle message
            if let Ok(message) = DistributedMessage::deserialize(&data) {
                if !message.is_expired() {
                    if let Err(e) = Self::handle_message(message, handlers.clone()).await {
                        warn!("Error handling message: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_message(
        message: DistributedMessage,
        handlers: Arc<RwLock<HashMap<MessageType, Vec<Arc<dyn MessageHandler>>>>>,
    ) -> DistributedResult<()> {
        let handlers_guard = handlers.read().await;
        
        if let Some(message_handlers) = handlers_guard.get(&message.message_type) {
            for handler in message_handlers {
                if let Err(e) = handler.handle_message(message.clone()).await {
                    warn!("Handler error: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// Default heartbeat handler
pub struct HeartbeatHandler {
    local_node_id: NodeId,
}

impl HeartbeatHandler {
    pub fn new(local_node_id: NodeId) -> Self {
        Self { local_node_id }
    }
}

#[async_trait]
impl MessageHandler for HeartbeatHandler {
    async fn handle_message(&self, message: DistributedMessage) -> DistributedResult<Option<DistributedMessage>> {
        debug!("Received heartbeat from {}", message.sender);
        
        // Respond with our own heartbeat
        let response = DistributedMessage::heartbeat(self.local_node_id.clone());
        Ok(Some(response))
    }

    fn message_types(&self) -> Vec<MessageType> {
        vec![MessageType::Heartbeat]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_message_creation() {
        let sender = NodeId::new();
        let recipient = NodeId::new();
        let payload = b"test payload".to_vec();
        
        let message = DistributedMessage::new(
            sender.clone(),
            Some(recipient.clone()),
            MessageType::Application,
            payload.clone(),
        );

        assert_eq!(message.sender, sender);
        assert_eq!(message.recipient, Some(recipient));
        assert_eq!(message.payload, payload);
        assert!(!message.is_expired());
    }

    #[test]
    fn test_message_serialization() {
        let message = DistributedMessage::heartbeat(NodeId::new());
        
        let serialized = message.serialize().unwrap();
        let deserialized = DistributedMessage::deserialize(&serialized).unwrap();
        
        assert_eq!(message.id, deserialized.id);
        assert_eq!(message.sender, deserialized.sender);
        assert_eq!(message.message_type as u8, deserialized.message_type as u8);
    }

    #[tokio::test]
    async fn test_message_bus_creation() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let bus = MessageBus::new(addr).await.unwrap();
        
        // Test that the bus was created successfully
        assert_eq!(bus.local_addr.ip(), addr.ip());
    }
}