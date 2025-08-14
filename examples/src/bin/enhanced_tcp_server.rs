//! Enhanced TCP echo server demonstrating the reactor system with real I/O integration

use seastar_core::reactor::{initialize_reactor, with_reactor};
use seastar_core::task::TaskPriority;
use seastar_core::{Error, Result};
use seastar_net::tcp::{TcpListener, TcpConnection, TcpConfig};
use seastar_net::SocketAddress;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tracing::{info, error};

/// Handle a client connection
async fn handle_client(mut stream: TcpConnection, client_addr: SocketAddr) -> Result<()> {
    info!("Accepted connection from {:?}", client_addr);
    
    let mut buffer = vec![0u8; 1024];
    
    loop {
        // Read data from client
        match stream.recv_data(&mut buffer).await {
            Ok(0) => {
                info!("Client {:?} disconnected", client_addr);
                break;
            }
            Ok(bytes_read) => {
                info!("Received {} bytes from {:?}", bytes_read, client_addr);
                
                // Echo the data back to client
                if let Err(e) = stream.send_data(&buffer[..bytes_read]).await {
                    error!("Failed to write to client {:?}: {}", client_addr, e);
                    return Err(Error::Network(e.to_string()));
                }
                
                info!("Echoed {} bytes back to {:?}", bytes_read, client_addr);
            }
            Err(e) => {
                error!("Failed to read from client {:?}: {}", client_addr, e);
                return Err(Error::Network(e.to_string()));
            }
        }
    }
    
    info!("Connection handler for {:?} finished", client_addr);
    Ok(())
}

/// Main server loop
async fn run_server() -> Result<()> {
    let addr = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 0, 0, 1),
        8080
    ));
    
    info!("Starting enhanced TCP echo server on {:?}", addr);
    
    // Create TCP listener with default config
    let config = TcpConfig::default();
    let mut listener = TcpListener::bind(addr.clone(), config).await.map_err(|e| Error::Network(e.to_string()))?;
    
    // Listener integrates with reactor automatically
    
    info!("Server listening on {:?}", addr);
    
    // Accept connections in a loop
    loop {
        match listener.accept().await {
            Ok(connection) => {
                let client_addr = connection.info().peer_addr;
                info!("New connection from {:?}", client_addr);
                
                // Use the TcpConnection directly
                let stream = connection;
                
                // Spawn task with reactor
                with_reactor(|reactor| {
                    
                    // Spawn a task to handle this client
                    let task_name = format!("client-handler-{:?}", client_addr);
                    reactor.spawn_with_metadata(
                        handle_client(stream, client_addr),
                        task_name,
                        TaskPriority::Normal
                    );
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
                // Continue accepting other connections
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting enhanced Seastar TCP echo server");
    
    // Initialize the reactor for this thread
    initialize_reactor()?;
    
    // Spawn the server task
    let server_handle = with_reactor(|reactor| {
        reactor.spawn_with_metadata(
            run_server(),
            "tcp-echo-server".to_string(),
            TaskPriority::High
        )
    });
    
    // Run the reactor event loop
    let reactor_task = with_reactor(|reactor| {
        reactor.spawn_with_metadata(
            reactor_event_loop(),
            "reactor-loop".to_string(),
            TaskPriority::Critical
        )
    });
    
    info!("Reactor and server initialized");
    
    // In a real implementation, we'd have proper shutdown handling
    // For now, just sleep to keep the program running
    tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    
    info!("Server shutting down");
    Ok(())
}

/// The main reactor event loop
async fn reactor_event_loop() -> Result<()> {
    loop {
        // Process reactor iteration
        let events_processed = with_reactor(|reactor| -> Result<usize> {
            // In a real implementation, we'd integrate this properly with tokio/async runtime
            // For now, just return 0 to indicate no events processed
            Ok(0)
        })?;
        
        // Yield control to allow other tasks to run
        tokio::task::yield_now().await;
        
        // Small delay to prevent busy loop
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }
}