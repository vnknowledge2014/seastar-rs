//! HTTP response types

use std::collections::HashMap;
use seastar_core::{Result, Error};
use crate::request::HttpVersion;

/// HTTP status codes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpStatus {
    // 1xx Informational
    Continue,
    SwitchingProtocols,
    
    // 2xx Success
    Ok,
    Created,
    Accepted,
    NoContent,
    
    // 3xx Redirection
    MovedPermanently,
    Found,
    SeeOther,
    NotModified,
    TemporaryRedirect,
    
    // 4xx Client Error
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    MethodNotAllowed,
    Conflict,
    
    // 5xx Server Error
    InternalServerError,
    NotImplemented,
    BadGateway,
    ServiceUnavailable,
    
    // Custom status
    Custom(u16, String),
}

impl HttpStatus {
    pub fn code(&self) -> u16 {
        match self {
            HttpStatus::Continue => 100,
            HttpStatus::SwitchingProtocols => 101,
            HttpStatus::Ok => 200,
            HttpStatus::Created => 201,
            HttpStatus::Accepted => 202,
            HttpStatus::NoContent => 204,
            HttpStatus::MovedPermanently => 301,
            HttpStatus::Found => 302,
            HttpStatus::SeeOther => 303,
            HttpStatus::NotModified => 304,
            HttpStatus::TemporaryRedirect => 307,
            HttpStatus::BadRequest => 400,
            HttpStatus::Unauthorized => 401,
            HttpStatus::Forbidden => 403,
            HttpStatus::NotFound => 404,
            HttpStatus::MethodNotAllowed => 405,
            HttpStatus::Conflict => 409,
            HttpStatus::InternalServerError => 500,
            HttpStatus::NotImplemented => 501,
            HttpStatus::BadGateway => 502,
            HttpStatus::ServiceUnavailable => 503,
            HttpStatus::Custom(code, _) => *code,
        }
    }
    
    pub fn reason_phrase(&self) -> &str {
        match self {
            HttpStatus::Continue => "Continue",
            HttpStatus::SwitchingProtocols => "Switching Protocols",
            HttpStatus::Ok => "OK",
            HttpStatus::Created => "Created",
            HttpStatus::Accepted => "Accepted",
            HttpStatus::NoContent => "No Content",
            HttpStatus::MovedPermanently => "Moved Permanently",
            HttpStatus::Found => "Found",
            HttpStatus::SeeOther => "See Other",
            HttpStatus::NotModified => "Not Modified",
            HttpStatus::TemporaryRedirect => "Temporary Redirect",
            HttpStatus::BadRequest => "Bad Request",
            HttpStatus::Unauthorized => "Unauthorized",
            HttpStatus::Forbidden => "Forbidden",
            HttpStatus::NotFound => "Not Found",
            HttpStatus::MethodNotAllowed => "Method Not Allowed",
            HttpStatus::Conflict => "Conflict",
            HttpStatus::InternalServerError => "Internal Server Error",
            HttpStatus::NotImplemented => "Not Implemented",
            HttpStatus::BadGateway => "Bad Gateway",
            HttpStatus::ServiceUnavailable => "Service Unavailable",
            HttpStatus::Custom(_, reason) => reason,
        }
    }
}

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.code(), self.reason_phrase())
    }
}

/// HTTP response
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub version: HttpVersion,
    pub status: HttpStatus,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Create a new HTTP response
    pub fn new(status: HttpStatus) -> Self {
        Self {
            version: HttpVersion::Http11,
            status,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
    
    /// Create a successful response with body
    pub fn ok(body: Vec<u8>) -> Self {
        let mut response = Self::new(HttpStatus::Ok);
        let content_length = body.len();
        response.body = body;
        response.headers.insert("Content-Length".to_string(), content_length.to_string());
        response
    }
    
    /// Create a text response
    pub fn text(text: String) -> Self {
        let body = text.into_bytes();
        let mut response = Self::ok(body);
        response.headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
        response
    }
    
    /// Create an HTML response
    pub fn html(html: String) -> Self {
        let body = html.into_bytes();
        let mut response = Self::ok(body);
        response.headers.insert("Content-Type".to_string(), "text/html; charset=utf-8".to_string());
        response
    }
    
    /// Create a JSON response
    pub fn json(json: String) -> Self {
        let body = json.into_bytes();
        let mut response = Self::ok(body);
        response.headers.insert("Content-Type".to_string(), "application/json; charset=utf-8".to_string());
        response
    }
    
    /// Create a not found response
    pub fn not_found() -> Self {
        Self::text("Not Found".to_string()).with_status(HttpStatus::NotFound)
    }
    
    /// Create an error response
    pub fn error(message: String) -> Self {
        Self::text(message).with_status(HttpStatus::InternalServerError)
    }
    
    /// Set the status
    pub fn with_status(mut self, status: HttpStatus) -> Self {
        self.status = status;
        self
    }
    
    /// Add a header to the response
    pub fn header(mut self, name: String, value: String) -> Self {
        self.headers.insert(name, value);
        self
    }
    
    /// Get a header value
    pub fn get_header(&self, name: &str) -> Option<&String> {
        self.headers.get(name)
    }
    
    /// Convert response to raw bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        
        // Status line
        result.extend_from_slice(format!("{} {}\r\n", self.version, self.status).as_bytes());
        
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