//! HTTP server implementation

use crate::{HttpRequest, HttpResponse, Router};
use crate::handler::Middleware;
use seastar_core::{Result, Error};
use seastar_net::{Socket, ConnectedSocket, SocketAddress};
use socket2::{Domain, Type, Protocol};
use std::sync::Arc;
use std::net::{SocketAddr, Ipv4Addr, SocketAddrV4};
use tracing::{info, warn, error};

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    /// Address to bind to
    pub bind_address: SocketAddress,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Request timeout in milliseconds
    pub request_timeout: u64,
    /// Maximum request size in bytes
    pub max_request_size: usize,
    /// Keep-alive timeout in milliseconds
    pub keep_alive_timeout: u64,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddress::Inet(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                8080
            ))),
            max_connections: 1000,
            request_timeout: 30_000, // 30 seconds
            max_request_size: 4 * 1024 * 1024, // 4MB
            keep_alive_timeout: 60_000, // 60 seconds
        }
    }
}

/// HTTP server
pub struct HttpServer {
    config: HttpServerConfig,
    router: Arc<Router>,
    middleware: Vec<Arc<dyn Middleware>>,
}

impl HttpServer {
    /// Create a new HTTP server with default configuration
    pub fn new() -> Self {
        Self {
            config: HttpServerConfig::default(),
            router: Arc::new(Router::new()),
            middleware: Vec::new(),
        }
    }
    
    /// Create a new HTTP server with custom configuration
    pub fn with_config(config: HttpServerConfig) -> Self {
        Self {
            config,
            router: Arc::new(Router::new()),
            middleware: Vec::new(),
        }
    }
    
    /// Set the router for this server
    pub fn with_router(mut self, router: Router) -> Self {
        self.router = Arc::new(router);
        self
    }
    
    /// Add middleware to the server
    pub fn add_middleware<M: Middleware + 'static>(&mut self, middleware: M) {
        self.middleware.push(Arc::new(middleware));
    }
    
    /// Start the HTTP server
    pub async fn start(&self) -> Result<()> {
        info!("Starting HTTP server on {:?}", self.config.bind_address);
        
        // Create listening socket
        let mut listen_socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        listen_socket.set_nonblocking(true)?;
        listen_socket.bind(&self.config.bind_address)?;
        listen_socket.listen(128)?;
        
        info!("HTTP server listening on {:?}", self.config.bind_address);
        
        // Accept connections loop
        loop {
            match self.accept_connection(&listen_socket).await {
                Ok(connection) => {
                    let router = self.router.clone();
                    let middleware = self.middleware.clone();
                    let config = self.config.clone();
                    
                    // Spawn connection handler
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(connection, router, middleware, config).await {
                            warn!("Connection handling error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    warn!("Failed to accept connection: {}", e);
                    // Small delay to prevent busy loop on persistent errors
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
    
    /// Handle a single connection
    async fn handle_connection(
        mut connection: ConnectedSocket,
        router: Arc<Router>,
        middleware: Vec<Arc<dyn Middleware>>,
        config: HttpServerConfig,
    ) -> Result<()> {
        info!("New connection established");
        
        let mut buffer = vec![0u8; config.max_request_size];
        
        loop {
            // Read request data with timeout
            let bytes_read = match tokio::time::timeout(
                std::time::Duration::from_millis(config.request_timeout),
                connection.read(&mut buffer)
            ).await {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(e)) => {
                    warn!("Failed to read from connection: {}", e);
                    break;
                }
                Err(_) => {
                    warn!("Request timeout");
                    break;
                }
            };
            
            if bytes_read == 0 {
                info!("Connection closed by client");
                break;
            }
            
            // Parse HTTP request
            let mut request = match HttpRequest::parse(&buffer[..bytes_read]) {
                Ok(req) => req,
                Err(e) => {
                    warn!("Failed to parse HTTP request: {}", e);
                    let error_response = HttpResponse::new(crate::HttpStatus::BadRequest);
                    let _ = Self::send_response(&mut connection, error_response).await;
                    continue;
                }
            };
            
            // Apply middleware (before request)
            for mw in &middleware {
                if let Err(e) = mw.before_request(&mut request) {
                    warn!("Middleware error: {}", e);
                    let error_response = HttpResponse::new(crate::HttpStatus::InternalServerError);
                    let _ = Self::send_response(&mut connection, error_response).await;
                    continue;
                }
            }
            
            // Route the request
            let mut response = match router.route(request.clone()) {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Router error: {}", e);
                    HttpResponse::error(format!("Internal server error: {}", e))
                }
            };
            
            // Apply middleware (after response)
            for mw in &middleware {
                if let Err(e) = mw.after_response(&request, &mut response) {
                    warn!("Middleware response error: {}", e);
                }
            }
            
            // Send response
            if let Err(e) = Self::send_response(&mut connection, response).await {
                warn!("Failed to send response: {}", e);
                break;
            }
            
            // Check for connection: close header
            if let Some(connection_header) = request.get_header("connection") {
                if connection_header.to_lowercase() == "close" {
                    break;
                }
            }
            
            // For HTTP/1.0, close connection by default
            if request.version == crate::HttpVersion::Http10 {
                break;
            }
        }
        
        info!("Connection handler finished");
        Ok(())
    }
    
    /// Send HTTP response
    async fn send_response(connection: &mut ConnectedSocket, response: HttpResponse) -> Result<()> {
        let response_bytes = response.to_bytes();
        connection.write(&response_bytes).await?;
        Ok(())
    }
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP server builder for easier configuration
pub struct HttpServerBuilder {
    config: HttpServerConfig,
    router: Option<Router>,
    middleware: Vec<Arc<dyn Middleware>>,
}

impl HttpServerBuilder {
    /// Create a new server builder
    pub fn new() -> Self {
        Self {
            config: HttpServerConfig::default(),
            router: None,
            middleware: Vec::new(),
        }
    }
    
    /// Set the bind address
    pub fn bind(mut self, address: SocketAddress) -> Self {
        self.config.bind_address = address;
        self
    }
    
    /// Set maximum connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }
    
    /// Set request timeout
    pub fn request_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.request_timeout = timeout_ms;
        self
    }
    
    /// Set maximum request size
    pub fn max_request_size(mut self, size: usize) -> Self {
        self.config.max_request_size = size;
        self
    }
    
    /// Set the router
    pub fn router(mut self, router: Router) -> Self {
        self.router = Some(router);
        self
    }
    
    /// Add middleware
    pub fn middleware<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.middleware.push(Arc::new(middleware));
        self
    }
    
    /// Build the HTTP server
    pub fn build(self) -> HttpServer {
        let mut server = HttpServer {
            config: self.config,
            router: Arc::new(self.router.unwrap_or_else(Router::new)),
            middleware: self.middleware,
        };
        
        server
    }
}

impl Default for HttpServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}