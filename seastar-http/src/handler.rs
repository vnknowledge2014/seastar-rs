//! HTTP request handlers

use crate::{HttpRequest, HttpResponse};
use seastar_core::{Result, Error};
use std::collections::HashMap;
use std::sync::Arc;

/// HTTP handler trait
pub trait HttpHandler: Send + Sync {
    /// Handle an HTTP request
    fn handle(&self, request: HttpRequest) -> Result<HttpResponse>;
}

/// Boxed HTTP handler
pub type BoxHandler = Box<dyn HttpHandler>;

/// Closure-based HTTP handler
pub struct ClosureHandler<F>
where
    F: Fn(HttpRequest) -> Result<HttpResponse> + Send + Sync,
{
    handler: F,
}

impl<F> ClosureHandler<F>
where
    F: Fn(HttpRequest) -> Result<HttpResponse> + Send + Sync,
{
    pub fn new(handler: F) -> Self {
        Self { handler }
    }
}

impl<F> HttpHandler for ClosureHandler<F>
where
    F: Fn(HttpRequest) -> Result<HttpResponse> + Send + Sync,
{
    fn handle(&self, request: HttpRequest) -> Result<HttpResponse> {
        (self.handler)(request)
    }
}

/// Static file handler
pub struct StaticHandler {
    content: Vec<u8>,
    content_type: String,
}

impl StaticHandler {
    pub fn new(content: Vec<u8>, content_type: String) -> Self {
        Self { content, content_type }
    }
    
    pub fn text(text: String) -> Self {
        Self {
            content: text.into_bytes(),
            content_type: "text/plain; charset=utf-8".to_string(),
        }
    }
    
    pub fn html(html: String) -> Self {
        Self {
            content: html.into_bytes(),
            content_type: "text/html; charset=utf-8".to_string(),
        }
    }
    
    pub fn json(json: String) -> Self {
        Self {
            content: json.into_bytes(),
            content_type: "application/json; charset=utf-8".to_string(),
        }
    }
}

impl HttpHandler for StaticHandler {
    fn handle(&self, _request: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse::ok(self.content.clone())
            .header("Content-Type".to_string(), self.content_type.clone()))
    }
}

/// Route definition
#[derive(Clone)]
pub struct Route {
    pub path: String,
    pub method: crate::request::HttpMethod,
    pub handler: Arc<dyn HttpHandler>,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Route")
            .field("path", &self.path)
            .field("method", &self.method)
            .field("handler", &"<handler>")
            .finish()
    }
}

impl Route {
    pub fn new<H>(method: crate::request::HttpMethod, path: String, handler: H) -> Self
    where
        H: HttpHandler + 'static,
    {
        Self {
            method,
            path,
            handler: Arc::new(handler),
        }
    }
    
    /// Check if this route matches the given request
    pub fn matches(&self, method: &crate::request::HttpMethod, path: &str) -> bool {
        self.method == *method && self.path_matches(path)
    }
    
    /// Check if the path matches (supports basic wildcard matching)
    fn path_matches(&self, path: &str) -> bool {
        if self.path == path {
            return true;
        }
        
        // Simple wildcard matching for routes ending with /*
        if self.path.ends_with("/*") {
            let prefix = &self.path[..self.path.len() - 2];
            return path.starts_with(prefix);
        }
        
        // Path parameter matching for routes with :param
        if self.path.contains(':') {
            return self.match_path_params(path);
        }
        
        false
    }
    
    /// Match path parameters like /users/:id
    fn match_path_params(&self, path: &str) -> bool {
        let route_parts: Vec<&str> = self.path.split('/').collect();
        let path_parts: Vec<&str> = path.split('/').collect();
        
        if route_parts.len() != path_parts.len() {
            return false;
        }
        
        for (route_part, path_part) in route_parts.iter().zip(path_parts.iter()) {
            if route_part.starts_with(':') {
                // This is a parameter, it matches any value
                continue;
            } else if route_part != path_part {
                return false;
            }
        }
        
        true
    }
    
    /// Extract path parameters from the request path
    pub fn extract_params(&self, path: &str) -> HashMap<String, String> {
        let mut params = HashMap::new();
        
        if !self.path.contains(':') {
            return params;
        }
        
        let route_parts: Vec<&str> = self.path.split('/').collect();
        let path_parts: Vec<&str> = path.split('/').collect();
        
        if route_parts.len() == path_parts.len() {
            for (route_part, path_part) in route_parts.iter().zip(path_parts.iter()) {
                if route_part.starts_with(':') {
                    let param_name = &route_part[1..]; // Remove the ':'
                    params.insert(param_name.to_string(), path_part.to_string());
                }
            }
        }
        
        params
    }
}

/// HTTP router
#[derive(Default)]
pub struct Router {
    routes: Vec<Route>,
}

impl Router {
    /// Create a new router
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
        }
    }
    
    /// Add a route to the router
    pub fn add_route(&mut self, route: Route) {
        self.routes.push(route);
    }
    
    /// Add a GET route
    pub fn get<H>(&mut self, path: String, handler: H)
    where
        H: HttpHandler + 'static,
    {
        self.add_route(Route::new(crate::request::HttpMethod::GET, path, handler));
    }
    
    /// Add a POST route
    pub fn post<H>(&mut self, path: String, handler: H)
    where
        H: HttpHandler + 'static,
    {
        self.add_route(Route::new(crate::request::HttpMethod::POST, path, handler));
    }
    
    /// Add a PUT route
    pub fn put<H>(&mut self, path: String, handler: H)
    where
        H: HttpHandler + 'static,
    {
        self.add_route(Route::new(crate::request::HttpMethod::PUT, path, handler));
    }
    
    /// Add a DELETE route
    pub fn delete<H>(&mut self, path: String, handler: H)
    where
        H: HttpHandler + 'static,
    {
        self.add_route(Route::new(crate::request::HttpMethod::DELETE, path, handler));
    }
    
    /// Route a request to the appropriate handler
    pub fn route(&self, mut request: HttpRequest) -> Result<HttpResponse> {
        // Find matching route
        for route in &self.routes {
            if route.matches(&request.method, &request.uri) {
                // Extract path parameters and add them to headers for now
                let params = route.extract_params(&request.uri);
                for (key, value) in params {
                    request.headers.insert(format!("param-{}", key), value);
                }
                
                return route.handler.handle(request);
            }
        }
        
        // No route matched
        Ok(HttpResponse::not_found())
    }
}

/// Middleware trait
pub trait Middleware: Send + Sync {
    /// Process request before it reaches the handler
    fn before_request(&self, request: &mut HttpRequest) -> Result<()>;
    
    /// Process response after the handler
    fn after_response(&self, request: &HttpRequest, response: &mut HttpResponse) -> Result<()>;
}

/// CORS middleware
pub struct CorsMiddleware {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
}

impl CorsMiddleware {
    pub fn new() -> Self {
        Self {
            allowed_origins: vec!["*".to_string()],
            allowed_methods: vec!["GET".to_string(), "POST".to_string(), "PUT".to_string(), "DELETE".to_string()],
            allowed_headers: vec!["Content-Type".to_string(), "Authorization".to_string()],
        }
    }
}

impl Middleware for CorsMiddleware {
    fn before_request(&self, _request: &mut HttpRequest) -> Result<()> {
        // CORS preflight handling would go here
        Ok(())
    }
    
    fn after_response(&self, _request: &HttpRequest, response: &mut HttpResponse) -> Result<()> {
        response.headers.insert("Access-Control-Allow-Origin".to_string(), self.allowed_origins.join(", "));
        response.headers.insert("Access-Control-Allow-Methods".to_string(), self.allowed_methods.join(", "));
        response.headers.insert("Access-Control-Allow-Headers".to_string(), self.allowed_headers.join(", "));
        Ok(())
    }
}

/// Logging middleware
pub struct LoggingMiddleware;

impl LoggingMiddleware {
    pub fn new() -> Self {
        Self
    }
}

impl Middleware for LoggingMiddleware {
    fn before_request(&self, request: &mut HttpRequest) -> Result<()> {
        tracing::info!("HTTP {} {} from {}", 
            request.method, 
            request.uri, 
            request.headers.get("host").unwrap_or(&"unknown".to_string())
        );
        Ok(())
    }
    
    fn after_response(&self, request: &HttpRequest, response: &mut HttpResponse) -> Result<()> {
        tracing::info!("HTTP {} {} -> {}", 
            request.method, 
            request.uri, 
            response.status.code()
        );
        Ok(())
    }
}