//! Hello World example for Seastar-RS

use seastar_core::prelude::*;
use seastar_core::future::make_ready_future;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    info!("Hello, Seastar-RS!");
    
    // Create a simple future and await it
    let result = make_ready_future(42).await;
    info!("Future completed with result: {}", result);
    
    // Demonstrate promise/future usage
    let (promise, future) = Promise::new();
    
    // In a real application, this would be done asynchronously
    promise.set_value("Hello from promise!".to_string());
    
    match future.await {
        Ok(value) => info!("Promise resolved with: {}", value),
        Err(e) => eprintln!("Promise failed: {}", e),
    }
    
    Ok(())
}