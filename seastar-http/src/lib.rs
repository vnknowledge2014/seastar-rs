//! # Seastar-RS HTTP
//! 
//! HTTP client and server implementation for Seastar-RS.

pub mod server;
pub mod client;
pub mod request;
pub mod response;
pub mod handler;

pub use server::{HttpServer, HttpServerConfig, HttpServerBuilder};
pub use client::{HttpClient, HttpClientConfig, HttpClientBuilder};
pub use request::{HttpRequest, HttpMethod, HttpVersion};
pub use response::{HttpResponse, HttpStatus};
pub use handler::{
    HttpHandler, Router, Route, StaticHandler, ClosureHandler,
    Middleware, CorsMiddleware, LoggingMiddleware
};