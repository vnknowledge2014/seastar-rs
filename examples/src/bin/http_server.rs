//! HTTP Server example for Seastar-RS

use seastar_core::prelude::*;
use seastar_http::HttpServer;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    info!("Starting HTTP server example");
    
    let server = HttpServer::new();
    
    match server.start().await {
        Ok(()) => {
            info!("HTTP server started successfully");
        }
        Err(e) => {
            error!("Failed to start HTTP server: {}", e);
        }
    }
    
    Ok(())
}