//! HTTP client implementation

use crate::{HttpRequest, HttpResponse, HttpMethod, HttpVersion};
use seastar_core::{Result, Error};
use seastar_net::{Socket, ConnectedSocket, SocketAddress};
use socket2::{Domain, Type, Protocol};
use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashMap;
use tracing::{info, warn};

/// HTTP client configuration
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Request timeout in milliseconds
    pub timeout: u64,
    /// Maximum redirects to follow
    pub max_redirects: u32,
    /// Default headers to include in all requests
    pub default_headers: HashMap<String, String>,
    /// User agent string
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        let mut default_headers = HashMap::new();
        default_headers.insert("User-Agent".to_string(), "Seastar-RS HTTP Client/0.1.0".to_string());
        default_headers.insert("Accept".to_string(), "*/*".to_string());
        default_headers.insert("Connection".to_string(), "close".to_string());
        
        Self {
            timeout: 30_000, // 30 seconds
            max_redirects: 5,
            default_headers,
            user_agent: "Seastar-RS HTTP Client/0.1.0".to_string(),
        }
    }
}

/// HTTP client
pub struct HttpClient {
    config: HttpClientConfig,
}

impl HttpClient {
    /// Create a new HTTP client with default configuration
    pub fn new() -> Self {
        Self {
            config: HttpClientConfig::default(),
        }
    }
    
    /// Create a new HTTP client with custom configuration
    pub fn with_config(config: HttpClientConfig) -> Self {
        Self { config }
    }
    
    /// Perform a GET request
    pub async fn get(&self, url: &str) -> Result<HttpResponse> {
        let request = HttpRequest::new(HttpMethod::GET, url.to_string())
            .header("Host".to_string(), self.extract_host(url)?)
            .header("User-Agent".to_string(), self.config.user_agent.clone());
        
        self.send_request(url, request).await
    }
    
    /// Perform a POST request
    pub async fn post(&self, url: &str, body: Vec<u8>) -> Result<HttpResponse> {
        let content_length = body.len();
        let request = HttpRequest::new(HttpMethod::POST, url.to_string())
            .header("Host".to_string(), self.extract_host(url)?)
            .header("User-Agent".to_string(), self.config.user_agent.clone())
            .header("Content-Length".to_string(), content_length.to_string())
            .body(body);
        
        self.send_request(url, request).await
    }
    
    /// Perform a POST request with JSON body
    pub async fn post_json(&self, url: &str, json: String) -> Result<HttpResponse> {
        let body = json.into_bytes();
        let content_length = body.len();
        let request = HttpRequest::new(HttpMethod::POST, url.to_string())
            .header("Host".to_string(), self.extract_host(url)?)
            .header("User-Agent".to_string(), self.config.user_agent.clone())
            .header("Content-Type".to_string(), "application/json".to_string())
            .header("Content-Length".to_string(), content_length.to_string())
            .body(body);
        
        self.send_request(url, request).await
    }
    
    /// Perform a PUT request
    pub async fn put(&self, url: &str, body: Vec<u8>) -> Result<HttpResponse> {
        let content_length = body.len();
        let request = HttpRequest::new(HttpMethod::PUT, url.to_string())
            .header("Host".to_string(), self.extract_host(url)?)
            .header("User-Agent".to_string(), self.config.user_agent.clone())
            .header("Content-Length".to_string(), content_length.to_string())
            .body(body);
        
        self.send_request(url, request).await
    }
    
    /// Perform a DELETE request
    pub async fn delete(&self, url: &str) -> Result<HttpResponse> {
        let request = HttpRequest::new(HttpMethod::DELETE, url.to_string())
            .header("Host".to_string(), self.extract_host(url)?)
            .header("User-Agent".to_string(), self.config.user_agent.clone());
        
        self.send_request(url, request).await
    }
    
    /// Send a custom HTTP request
    pub async fn send(&self, url: &str, mut request: HttpRequest) -> Result<HttpResponse> {
        // Add default headers
        for (key, value) in &self.config.default_headers {
            if !request.headers.contains_key(key) {
                request.headers.insert(key.clone(), value.clone());
            }
        }
        
        self.send_request(url, request).await
    }
    
    /// Internal method to send request
    async fn send_request(&self, url: &str, mut request: HttpRequest) -> Result<HttpResponse> {
        let (host, port) = self.parse_url(url)?;
        
        // Update request URI to path only
        request.uri = self.extract_path(url);
        
        // Ensure host header is set
        if !request.headers.contains_key("host") {
            request.headers.insert("host".to_string(), format!("{}:{}", host, port));
        }
        
        info!("Sending HTTP {} {} to {}:{}", request.method, request.uri, host, port);
        
        // Create connection
        let socket_addr = format!("{}:{}", host, port)
            .to_socket_addrs()
            .map_err(|e| Error::Network(format!("Failed to resolve address: {}", e)))?
            .next()
            .ok_or_else(|| Error::Network("No address found for host".to_string()))?;
        
        let mut socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;
        
        let socket_address = SocketAddress::Inet(socket_addr);
        socket.connect(&socket_address).await?;
        
        let mut connected_socket = ConnectedSocket::from_socket(socket);
        
        // Send request
        let request_bytes = request.to_bytes();
        connected_socket.write(&request_bytes).await?;
        
        // Read response with timeout
        let mut response_buffer = vec![0u8; 64 * 1024]; // 64KB buffer
        let bytes_read = tokio::time::timeout(
            std::time::Duration::from_millis(self.config.timeout),
            connected_socket.read(&mut response_buffer)
        ).await
        .map_err(|_| Error::Network("Request timeout".to_string()))?
        .map_err(|e| Error::Network(format!("Failed to read response: {}", e)))?;
        
        // Parse response
        self.parse_response(&response_buffer[..bytes_read]).await
    }
    
    /// Parse HTTP response from raw bytes
    async fn parse_response(&self, data: &[u8]) -> Result<HttpResponse> {
        let response_str = std::str::from_utf8(data)
            .map_err(|_| Error::InvalidArgument("Invalid UTF-8 in HTTP response".to_string()))?;
        
        let mut lines = response_str.lines();
        
        // Parse status line
        let status_line = lines.next()
            .ok_or_else(|| Error::InvalidArgument("Empty HTTP response".to_string()))?;
        
        let mut status_parts = status_line.split_whitespace();
        let _version = status_parts.next() // HTTP/1.1
            .ok_or_else(|| Error::InvalidArgument("Missing HTTP version".to_string()))?;
        
        let status_code: u16 = status_parts.next()
            .ok_or_else(|| Error::InvalidArgument("Missing status code".to_string()))?
            .parse()
            .map_err(|_| Error::InvalidArgument("Invalid status code".to_string()))?;
        
        let reason_phrase = status_parts.collect::<Vec<_>>().join(" ");
        
        // Convert status code to HttpStatus
        let status = match status_code {
            200 => crate::HttpStatus::Ok,
            201 => crate::HttpStatus::Created,
            204 => crate::HttpStatus::NoContent,
            400 => crate::HttpStatus::BadRequest,
            401 => crate::HttpStatus::Unauthorized,
            403 => crate::HttpStatus::Forbidden,
            404 => crate::HttpStatus::NotFound,
            500 => crate::HttpStatus::InternalServerError,
            _ => crate::HttpStatus::Custom(status_code, reason_phrase),
        };
        
        // Parse headers
        let mut headers = HashMap::new();
        let mut body_start = None;
        
        for (i, line) in lines.enumerate() {
            if line.is_empty() {
                body_start = Some(i + 1);
                break;
            }
            
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(
                    name.trim().to_lowercase(),
                    value.trim().to_string()
                );
            }
        }
        
        // Parse body (simplified)
        let body = if let Some(_start) = body_start {
            // In a real implementation, we'd handle content-length, chunked encoding, etc.
            // For now, assume the rest is body
            let body_start_pos = response_str.find("\r\n\r\n")
                .or_else(|| response_str.find("\n\n"))
                .map(|pos| pos + 4)
                .unwrap_or(response_str.len());
            
            if body_start_pos < data.len() {
                data[body_start_pos..].to_vec()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        
        Ok(HttpResponse {
            version: HttpVersion::Http11,
            status,
            headers,
            body,
        })
    }
    
    /// Extract host from URL
    fn extract_host(&self, url: &str) -> Result<String> {
        let (host, port) = self.parse_url(url)?;
        Ok(if port == 80 { host } else { format!("{}:{}", host, port) })
    }
    
    /// Extract path from URL
    fn extract_path(&self, url: &str) -> String {
        if let Some(path_start) = url.find("://").and_then(|i| url[i+3..].find('/')) {
            let full_start = url.find("://").unwrap() + 3 + path_start;
            url[full_start..].to_string()
        } else {
            "/".to_string()
        }
    }
    
    /// Parse URL to extract host and port
    fn parse_url(&self, url: &str) -> Result<(String, u16)> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(Error::InvalidArgument("URL must start with http:// or https://".to_string()));
        }
        
        let is_https = url.starts_with("https://");
        let default_port = if is_https { 443 } else { 80 };
        
        let without_scheme = if is_https {
            &url[8..] // Remove "https://"
        } else {
            &url[7..] // Remove "http://"
        };
        
        // Find the end of host:port part
        let host_part = if let Some(slash_pos) = without_scheme.find('/') {
            &without_scheme[..slash_pos]
        } else {
            without_scheme
        };
        
        // Parse host and port
        if let Some(colon_pos) = host_part.find(':') {
            let host = host_part[..colon_pos].to_string();
            let port = host_part[colon_pos+1..].parse()
                .map_err(|_| Error::InvalidArgument("Invalid port number".to_string()))?;
            Ok((host, port))
        } else {
            Ok((host_part.to_string(), default_port))
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP client builder for easier configuration
pub struct HttpClientBuilder {
    config: HttpClientConfig,
}

impl HttpClientBuilder {
    /// Create a new client builder
    pub fn new() -> Self {
        Self {
            config: HttpClientConfig::default(),
        }
    }
    
    /// Set request timeout
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.config.timeout = timeout_ms;
        self
    }
    
    /// Set maximum redirects
    pub fn max_redirects(mut self, max: u32) -> Self {
        self.config.max_redirects = max;
        self
    }
    
    /// Set user agent
    pub fn user_agent(mut self, ua: String) -> Self {
        self.config.user_agent = ua.clone();
        self.config.default_headers.insert("User-Agent".to_string(), ua);
        self
    }
    
    /// Add a default header
    pub fn header(mut self, name: String, value: String) -> Self {
        self.config.default_headers.insert(name, value);
        self
    }
    
    /// Build the HTTP client
    pub fn build(self) -> HttpClient {
        HttpClient::with_config(self.config)
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}