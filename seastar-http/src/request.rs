//! HTTP request types

use std::collections::HashMap;
use seastar_core::{Result, Error};

/// HTTP methods
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,
    OPTIONS,
    PATCH,
    TRACE,
    CONNECT,
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpMethod::GET => write!(f, "GET"),
            HttpMethod::POST => write!(f, "POST"),
            HttpMethod::PUT => write!(f, "PUT"),
            HttpMethod::DELETE => write!(f, "DELETE"),
            HttpMethod::HEAD => write!(f, "HEAD"),
            HttpMethod::OPTIONS => write!(f, "OPTIONS"),
            HttpMethod::PATCH => write!(f, "PATCH"),
            HttpMethod::TRACE => write!(f, "TRACE"),
            HttpMethod::CONNECT => write!(f, "CONNECT"),
        }
    }
}

impl std::str::FromStr for HttpMethod {
    type Err = Error;
    
    fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Ok(HttpMethod::GET),
            "POST" => Ok(HttpMethod::POST),
            "PUT" => Ok(HttpMethod::PUT),
            "DELETE" => Ok(HttpMethod::DELETE),
            "HEAD" => Ok(HttpMethod::HEAD),
            "OPTIONS" => Ok(HttpMethod::OPTIONS),
            "PATCH" => Ok(HttpMethod::PATCH),
            "TRACE" => Ok(HttpMethod::TRACE),
            "CONNECT" => Ok(HttpMethod::CONNECT),
            _ => Err(Error::InvalidArgument(format!("Invalid HTTP method: {}", s))),
        }
    }
}

/// HTTP version
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpVersion {
    Http10,
    Http11,
    Http2,
}

impl std::fmt::Display for HttpVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpVersion::Http10 => write!(f, "HTTP/1.0"),
            HttpVersion::Http11 => write!(f, "HTTP/1.1"),
            HttpVersion::Http2 => write!(f, "HTTP/2"),
        }
    }
}

/// HTTP request
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub uri: String,
    pub version: HttpVersion,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    /// Create a new HTTP request
    pub fn new(method: HttpMethod, uri: String) -> Self {
        Self {
            method,
            uri,
            version: HttpVersion::Http11,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
    
    /// Add a header to the request
    pub fn header(mut self, name: String, value: String) -> Self {
        self.headers.insert(name, value);
        self
    }
    
    /// Set the request body
    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
    
    /// Get a header value
    pub fn get_header(&self, name: &str) -> Option<&String> {
        self.headers.get(name)
    }
    
    /// Parse HTTP request from raw bytes
    pub fn parse(data: &[u8]) -> Result<Self> {
        let request_str = std::str::from_utf8(data)
            .map_err(|_| Error::InvalidArgument("Invalid UTF-8 in HTTP request".to_string()))?;
        
        let mut lines = request_str.lines();
        
        // Parse request line
        let request_line = lines.next()
            .ok_or_else(|| Error::InvalidArgument("Empty HTTP request".to_string()))?;
        
        let mut parts = request_line.split_whitespace();
        let method: HttpMethod = parts.next()
            .ok_or_else(|| Error::InvalidArgument("Missing HTTP method".to_string()))?
            .parse()?;
        
        let uri = parts.next()
            .ok_or_else(|| Error::InvalidArgument("Missing URI".to_string()))?
            .to_string();
        
        let version = match parts.next().unwrap_or("HTTP/1.1") {
            "HTTP/1.0" => HttpVersion::Http10,
            "HTTP/1.1" => HttpVersion::Http11,
            "HTTP/2" => HttpVersion::Http2,
            v => return Err(Error::InvalidArgument(format!("Unsupported HTTP version: {}", v))),
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
            // In a real implementation, we'd properly handle content-length, chunked encoding, etc.
            Vec::new()
        } else {
            Vec::new()
        };
        
        Ok(HttpRequest {
            method,
            uri,
            version,
            headers,
            body,
        })
    }
    
    /// Convert request to raw bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        
        // Request line
        result.extend_from_slice(format!("{} {} {}\r\n", self.method, self.uri, self.version).as_bytes());
        
        // Headers
        for (name, value) in &self.headers {
            result.extend_from_slice(format!("{}: {}\r\n", name, value).as_bytes());
        }
        
        // Empty line
        result.extend_from_slice(b"\r\n");
        
        // Body
        result.extend_from_slice(&self.body);
        
        result
    }
}