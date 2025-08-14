//! RPC server implementation

use crate::protocol::{
    RpcMessageType, RpcError, RpcErrorCode, RequestId, ServiceName, MethodName,
    RpcProtocol, RpcProtocolConfig, RpcServiceRegistry, RpcServiceInfo
};
use crate::codec::{RpcCodec, WireProtocol};
use seastar_core::{Result, Error};
use seastar_net::{Socket, ConnectedSocket, SocketAddress};
use socket2::{Domain, Type, Protocol};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use tracing::{info, warn, error, debug};
use serde::{Serialize, de::DeserializeOwned};

/// RPC method handler trait
pub trait RpcMethodHandler: Send + Sync {
    /// Handle an RPC method call
    fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>>;
    
    /// Get method name
    fn method_name(&self) -> &str;
    
    /// Get method description
    fn description(&self) -> &str {
        ""
    }
}

/// Closure-based RPC method handler
pub struct ClosureHandler<F>
where
    F: Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync,
{
    name: String,
    description: String,
    handler: F,
}

impl<F> ClosureHandler<F>
where
    F: Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync,
{
    pub fn new(name: String, handler: F) -> Self {
        Self {
            name,
            description: String::new(),
            handler,
        }
    }
    
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }
}

impl<F> RpcMethodHandler for ClosureHandler<F>
where
    F: Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync,
{
    fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        (self.handler)(payload)
    }
    
    fn method_name(&self) -> &str {
        &self.name
    }
    
    fn description(&self) -> &str {
        &self.description
    }
}

/// Typed RPC method handler
pub struct TypedHandler<Req, Resp, F>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
    F: Fn(Req) -> Result<Resp> + Send + Sync,
{
    name: String,
    description: String,
    handler: F,
    codec: RpcCodec,
    _phantom: std::marker::PhantomData<(Req, Resp)>,
}

impl<Req, Resp, F> TypedHandler<Req, Resp, F>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
    F: Fn(Req) -> Result<Resp> + Send + Sync,
{
    pub fn new(name: String, handler: F, codec: RpcCodec) -> Self {
        Self {
            name,
            description: String::new(),
            handler,
            codec,
            _phantom: std::marker::PhantomData,
        }
    }
    
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }
}

impl<Req, Resp, F> RpcMethodHandler for TypedHandler<Req, Resp, F>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
    F: Fn(Req) -> Result<Resp> + Send + Sync,
{
    fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        // Deserialize request
        let request: Req = self.codec.deserialize(&payload)?;
        
        // Call handler
        let response = (self.handler)(request)?;
        
        // Serialize response
        self.codec.serialize(&response)
    }
    
    fn method_name(&self) -> &str {
        &self.name
    }
    
    fn description(&self) -> &str {
        &self.description
    }
}

/// RPC service
pub struct RpcService {
    name: ServiceName,
    description: String,
    version: String,
    methods: HashMap<MethodName, Arc<dyn RpcMethodHandler>>,
}

impl RpcService {
    /// Create a new RPC service
    pub fn new(name: ServiceName) -> Self {
        Self {
            name,
            description: String::new(),
            version: "1.0".to_string(),
            methods: HashMap::new(),
        }
    }
    
    /// Set service description
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }
    
    /// Set service version
    pub fn with_version(mut self, version: String) -> Self {
        self.version = version;
        self
    }
    
    /// Add a method handler
    pub fn add_method<H: RpcMethodHandler + 'static>(&mut self, handler: H) {
        let method_name = handler.method_name().to_string();
        self.methods.insert(method_name, Arc::new(handler));
    }
    
    /// Add a typed method handler
    pub fn add_typed_method<Req, Resp, F>(
        &mut self,
        name: String,
        handler: F,
        codec: RpcCodec
    ) where
        Req: DeserializeOwned + Send + Sync + 'static,
        Resp: Serialize + Send + Sync + 'static,
        F: Fn(Req) -> Result<Resp> + Send + Sync + 'static,
    {
        let typed_handler = TypedHandler::new(name, handler, codec);
        self.add_method(typed_handler);
    }
    
    /// Add a closure method handler
    pub fn add_closure_method<F>(&mut self, name: String, handler: F)
    where
        F: Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        let closure_handler = ClosureHandler::new(name, handler);
        self.add_method(closure_handler);
    }
    
    /// Handle a method call
    pub fn handle_method(&self, method: &str, payload: Vec<u8>) -> Result<Vec<u8>> {
        match self.methods.get(method) {
            Some(handler) => handler.handle(payload),
            None => Err(RpcError::method_not_found(method).into()),
        }
    }
    
    /// Get service name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get service description
    pub fn description(&self) -> &str {
        &self.description
    }
    
    /// Get service version
    pub fn version(&self) -> &str {
        &self.version
    }
    
    /// List method names
    pub fn method_names(&self) -> Vec<&str> {
        self.methods.keys().map(|s| s.as_str()).collect()
    }
    
    /// Get method handler
    pub fn get_method(&self, name: &str) -> Option<&Arc<dyn RpcMethodHandler>> {
        self.methods.get(name)
    }
    
    /// Get service info for registry
    pub fn service_info(&self) -> RpcServiceInfo {
        let methods = self.methods
            .iter()
            .map(|(name, handler)| crate::protocol::RpcMethodInfo {
                name: name.clone(),
                description: handler.description().to_string(),
                input_type: "Vec<u8>".to_string(),
                output_type: "Vec<u8>".to_string(),
            })
            .collect();
            
        RpcServiceInfo {
            name: self.name.clone(),
            description: self.description.clone(),
            version: self.version.clone(),
            methods,
        }
    }
}

/// RPC server configuration
#[derive(Debug, Clone)]
pub struct RpcServerConfig {
    /// Address to bind to
    pub bind_address: SocketAddress,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Protocol configuration
    pub protocol_config: RpcProtocolConfig,
    /// Enable service discovery
    pub enable_discovery: bool,
    /// Enable reflection API
    pub enable_reflection: bool,
}

impl Default for RpcServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddress::Inet(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                9090
            ))),
            max_connections: 1000,
            protocol_config: RpcProtocolConfig::default(),
            enable_discovery: true,
            enable_reflection: true,
        }
    }
}

/// RPC server
pub struct RpcServer {
    config: RpcServerConfig,
    protocol: RpcProtocol,
    codec: RpcCodec,
    wire_protocol: WireProtocol,
    services: Arc<Mutex<HashMap<ServiceName, Arc<RpcService>>>>,
    registry: Arc<Mutex<RpcServiceRegistry>>,
}

impl RpcServer {
    /// Create a new RPC server with default configuration
    pub fn new() -> Self {
        let config = RpcServerConfig::default();
        let protocol = RpcProtocol::with_config(config.protocol_config.clone());
        let codec = RpcCodec::from_config(&config.protocol_config);
        let wire_protocol = WireProtocol::new(codec.clone());
        
        let mut server = Self {
            config,
            protocol,
            codec,
            wire_protocol,
            services: Arc::new(Mutex::new(HashMap::new())),
            registry: Arc::new(Mutex::new(RpcServiceRegistry::new())),
        };
        
        // Add built-in services if enabled
        server.add_builtin_services();
        server
    }
    
    /// Create a new RPC server with custom configuration
    pub fn with_config(config: RpcServerConfig) -> Self {
        let protocol = RpcProtocol::with_config(config.protocol_config.clone());
        let codec = RpcCodec::from_config(&config.protocol_config);
        let wire_protocol = WireProtocol::new(codec.clone());
        
        let mut server = Self {
            config,
            protocol,
            codec,
            wire_protocol,
            services: Arc::new(Mutex::new(HashMap::new())),
            registry: Arc::new(Mutex::new(RpcServiceRegistry::new())),
        };
        
        server.add_builtin_services();
        server
    }
    
    /// Add an RPC service
    pub fn add_service(&mut self, service: RpcService) -> Result<()> {
        let service_name = service.name().to_string();
        let service_info = service.service_info();
        
        // Add to services
        {
            let mut services = self.services.lock().unwrap();
            services.insert(service_name.clone(), Arc::new(service));
        }
        
        // Add to registry
        if self.config.enable_discovery {
            let mut registry = self.registry.lock().unwrap();
            registry.register_service(service_info);
        }
        
        info!("Registered RPC service: {}", service_name);
        Ok(())
    }
    
    /// Start the RPC server
    pub async fn start(&self) -> Result<()> {
        info!("Starting RPC server on {:?}", self.config.bind_address);
        
        // Create listening socket
        let listen_socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        listen_socket.set_nonblocking(true)?;
        listen_socket.bind(&self.config.bind_address)?;
        listen_socket.listen(128)?;
        
        info!("RPC server listening on {:?}", self.config.bind_address);
        
        // Accept connections loop
        loop {
            match self.accept_connection(&listen_socket).await {
                Ok(connection) => {
                    let services = self.services.clone();
                    let wire_protocol = self.wire_protocol.clone();
                    let protocol = self.protocol.clone();
                    
                    // Spawn connection handler
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            connection,
                            services,
                            wire_protocol,
                            protocol
                        ).await {
                            warn!("RPC connection handling error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    warn!("Failed to accept RPC connection: {}", e);
                    // Small delay to prevent busy loop
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
    
    /// Accept a single connection (simplified implementation)
    async fn accept_connection(&self, _listen_socket: &Socket) -> Result<ConnectedSocket> {
        // In a real implementation, this would use the actual socket accept() functionality
        // For now, return an error to prevent infinite loop
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Err(Error::Internal("Socket accept not fully implemented".to_string()))
    }
    
    /// Handle a single RPC connection
    async fn handle_connection(
        mut connection: ConnectedSocket,
        services: Arc<Mutex<HashMap<ServiceName, Arc<RpcService>>>>,
        wire_protocol: WireProtocol,
        protocol: RpcProtocol,
    ) -> Result<()> {
        info!("New RPC connection established");
        
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
        
        loop {
            // Read message from wire
            let bytes_read = connection.read(&mut buffer).await?;
            if bytes_read == 0 {
                info!("RPC connection closed by client");
                break;
            }
            
            // Decode message
            let message = match wire_protocol.decode_from_wire(&buffer[..bytes_read]) {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Failed to decode RPC message: {}", e);
                    continue;
                }
            };
            
            // Handle message
            let response = Self::handle_message(message, &services, &protocol).await;
            
            // Send response
            if let Ok(response_msg) = response {
                match wire_protocol.encode_for_wire(&response_msg) {
                    Ok(response_data) => {
                        if let Err(e) = connection.write(&response_data).await {
                            warn!("Failed to send RPC response: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to encode RPC response: {}", e);
                    }
                }
            }
        }
        
        info!("RPC connection handler finished");
        Ok(())
    }
    
    /// Handle a single RPC message
    async fn handle_message(
        message: RpcMessageType,
        services: &Arc<Mutex<HashMap<ServiceName, Arc<RpcService>>>>,
        protocol: &RpcProtocol,
    ) -> Result<RpcMessageType> {
        match message {
            RpcMessageType::Request { id, service, method, payload } => {
                debug!("Handling RPC request {}: {}.{}", id, service, method);
                
                // Find service
                let service_arc = {
                    let services_guard = services.lock().unwrap();
                    services_guard.get(&service).cloned()
                };
                
                let response = match service_arc {
                    Some(svc) => {
                        // Handle method call
                        match svc.handle_method(&method, payload) {
                            Ok(response_payload) => {
                                protocol.create_response(id, response_payload)
                            }
                            Err(e) => {
                                let rpc_error = RpcError::internal_error(&e.to_string());
                                protocol.create_error(id, rpc_error)
                            }
                        }
                    }
                    None => {
                        let rpc_error = RpcError::service_unavailable(&service);
                        protocol.create_error(id, rpc_error)
                    }
                };
                
                Ok(response)
            }
            
            RpcMessageType::Notification { service, method, payload } => {
                debug!("Handling RPC notification: {}.{}", service, method);
                
                // Handle notification (no response)
                let services_guard = services.lock().unwrap();
                if let Some(svc) = services_guard.get(&service) {
                    if let Err(e) = svc.handle_method(&method, payload) {
                        warn!("Error handling notification {}.{}: {}", service, method, e);
                    }
                }
                
                // No response for notifications
                Err(Error::Internal("No response for notifications".to_string()))
            }
            
            RpcMessageType::Heartbeat { timestamp: _ } => {
                // Respond with heartbeat
                Ok(protocol.create_heartbeat())
            }
            
            RpcMessageType::Cancel { id } => {
                // Handle request cancellation
                warn!("Request cancellation not implemented for request {}", id);
                Err(Error::Internal("Cancel not implemented".to_string()))
            }
            
            _ => {
                warn!("Unexpected message type received by server");
                Err(Error::Internal("Unexpected message type".to_string()))
            }
        }
    }
    
    /// Add built-in services
    fn add_builtin_services(&mut self) {
        if self.config.enable_reflection {
            self.add_reflection_service();
        }
        
        if self.config.enable_discovery {
            self.add_discovery_service();
        }
    }
    
    /// Add reflection service
    fn add_reflection_service(&mut self) {
        let mut reflection_service = RpcService::new("reflection".to_string())
            .with_description("Service reflection API".to_string());
        
        let registry = self.registry.clone();
        
        // List services method
        reflection_service.add_closure_method(
            "list_services".to_string(),
            move |_payload| {
                let registry = registry.lock().unwrap();
                let services: Vec<String> = registry
                    .list_services()
                    .iter()
                    .map(|s| s.name.clone())
                    .collect();
                
                serde_json::to_vec(&services)
                    .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))
            }
        );
        
        let registry = self.registry.clone();
        
        // Get service info method
        reflection_service.add_closure_method(
            "get_service".to_string(),
            move |payload| {
                let service_name: String = serde_json::from_slice(&payload)
                    .map_err(|e| Error::Internal(format!("Deserialization failed: {}", e)))?;
                
                let registry = registry.lock().unwrap();
                match registry.get_service(&service_name) {
                    Some(service_info) => {
                        serde_json::to_vec(service_info)
                            .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))
                    }
                    None => Err(Error::InvalidArgument(format!("Service not found: {}", service_name)))
                }
            }
        );
        
        if let Err(e) = self.add_service(reflection_service) {
            warn!("Failed to add reflection service: {}", e);
        }
    }
    
    /// Add service discovery service
    fn add_discovery_service(&mut self) {
        let mut discovery_service = RpcService::new("discovery".to_string())
            .with_description("Service discovery API".to_string());
        
        // Health check method
        discovery_service.add_closure_method(
            "health_check".to_string(),
            |_payload| {
                let health_status = serde_json::json!({
                    "status": "healthy",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                });
                
                serde_json::to_vec(&health_status)
                    .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))
            }
        );
        
        if let Err(e) = self.add_service(discovery_service) {
            warn!("Failed to add discovery service: {}", e);
        }
    }
    
    /// Get server configuration
    pub fn config(&self) -> &RpcServerConfig {
        &self.config
    }
    
    /// Get service registry
    pub fn registry(&self) -> Arc<Mutex<RpcServiceRegistry>> {
        self.registry.clone()
    }
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}