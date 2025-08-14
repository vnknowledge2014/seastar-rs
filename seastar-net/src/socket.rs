//! Socket abstractions for Seastar-RS networking

use seastar_core::{Result, Error};
use seastar_core::io::{IoBackend, IoRequest, IoOpType, generate_request_id, AsyncIoOp};
use crate::SocketAddress;
use std::os::unix::io::{RawFd, AsRawFd, FromRawFd};
use std::sync::{Arc, Mutex};
use socket2::{Socket as Socket2, Domain, Type, Protocol};

/// A generic socket wrapper
pub struct Socket {
    fd: RawFd,
    io_backend: Option<Arc<Mutex<dyn IoBackend>>>,
}

impl Socket {
    /// Create a new socket
    pub fn new(domain: Domain, socket_type: Type, protocol: Option<Protocol>) -> Result<Self> {
        let socket = Socket2::new(domain, socket_type, Some(protocol.unwrap_or(Protocol::TCP)))
            .map_err(|e| Error::Network(format!("Failed to create socket: {}", e)))?;
        
        Ok(Self {
            fd: socket.as_raw_fd(),
            io_backend: None,
        })
    }
    
    /// Create from existing raw file descriptor
    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            fd,
            io_backend: None,
        }
    }
    
    /// Set the I/O backend for this socket
    pub fn set_io_backend(&mut self, backend: Arc<Mutex<dyn IoBackend>>) {
        self.io_backend = Some(backend);
    }
    
    /// Get the raw file descriptor
    pub fn raw_fd(&self) -> RawFd {
        self.fd
    }
    
    /// Make the socket non-blocking
    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        let socket = unsafe { Socket2::from_raw_fd(self.fd) };
        socket.set_nonblocking(nonblocking)
            .map_err(|e| Error::Network(format!("Failed to set nonblocking: {}", e)))?;
        std::mem::forget(socket); // Don't drop the socket
        Ok(())
    }
    
    /// Bind the socket to an address
    pub fn bind(&self, addr: &SocketAddress) -> Result<()> {
        let socket = unsafe { Socket2::from_raw_fd(self.fd) };
        let result = match addr {
            SocketAddress::Inet(addr) => {
                socket.bind(&(*addr).into())
            }
            SocketAddress::Unix(_path) => {
                return Err(Error::InvalidArgument("Unix sockets not yet supported".to_string()));
            }
        };
        
        std::mem::forget(socket); // Don't drop the socket
        result.map_err(|e| Error::Network(format!("Failed to bind socket: {}", e)))
    }
    
    /// Start listening for connections
    pub fn listen(&self, backlog: i32) -> Result<()> {
        let socket = unsafe { Socket2::from_raw_fd(self.fd) };
        let result = socket.listen(backlog);
        std::mem::forget(socket); // Don't drop the socket
        result.map_err(|e| Error::Network(format!("Failed to listen: {}", e)))
    }
    
    /// Connect to a remote address
    pub async fn connect(&self, addr: &SocketAddress) -> Result<()> {
        let socket = unsafe { Socket2::from_raw_fd(self.fd) };
        let result = match addr {
            SocketAddress::Inet(addr) => {
                socket.connect(&(*addr).into())
            }
            SocketAddress::Unix(_path) => {
                return Err(Error::InvalidArgument("Unix sockets not yet supported".to_string()));
            }
        };
        
        std::mem::forget(socket); // Don't drop the socket
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    // Connection in progress, would need to wait for completion
                    // For now, return success
                    Ok(())
                } else {
                    Err(Error::Network(format!("Failed to connect: {}", e)))
                }
            }
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}

/// A connected socket for stream-oriented communication
pub struct ConnectedSocket {
    socket: Socket,
}

impl ConnectedSocket {
    /// Create a connected socket from a raw file descriptor
    pub fn from_raw_fd(fd: RawFd) -> Self {
        Self {
            socket: Socket::from_raw_fd(fd),
        }
    }
    
    /// Create a connected socket from a Socket
    pub fn from_socket(socket: Socket) -> Self {
        Self { socket }
    }
    
    /// Set the I/O backend for this socket
    pub fn set_io_backend(&mut self, backend: Arc<Mutex<dyn IoBackend>>) {
        self.socket.set_io_backend(backend);
    }
    
    /// Read data from the socket asynchronously
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if let Some(backend) = &self.socket.io_backend {
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.fd,
                op_type: IoOpType::Read,
                buffer: buffer.as_mut_ptr(),
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            Ok(result.bytes_transferred)
        } else {
            // Fallback to blocking I/O
            self.read_blocking(buffer)
        }
    }
    
    /// Write data to the socket asynchronously
    pub async fn write(&mut self, buffer: &[u8]) -> Result<usize> {
        if let Some(backend) = &self.socket.io_backend {
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.fd,
                op_type: IoOpType::Write,
                buffer: buffer.as_ptr() as *mut u8,
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            Ok(result.bytes_transferred)
        } else {
            // Fallback to blocking I/O
            self.write_blocking(buffer)
        }
    }
    
    /// Read data using blocking I/O as fallback
    pub fn read_blocking(&self, buffer: &mut [u8]) -> Result<usize> {
        let result = unsafe {
            libc::recv(
                self.socket.fd,
                buffer.as_mut_ptr() as *mut libc::c_void,
                buffer.len(),
                0,
            )
        };
        
        if result < 0 {
            Err(Error::Io("recv failed".to_string()))
        } else {
            Ok(result as usize)
        }
    }
    
    /// Write data using blocking I/O as fallback  
    pub fn write_blocking(&self, buffer: &[u8]) -> Result<usize> {
        let result = unsafe {
            libc::send(
                self.socket.fd,
                buffer.as_ptr() as *const libc::c_void,
                buffer.len(),
                0,
            )
        };
        
        if result < 0 {
            Err(Error::Io("send failed".to_string()))
        } else {
            Ok(result as usize)
        }
    }
    
    /// Get local socket address
    pub fn local_addr(&self) -> Result<SocketAddress> {
        let socket = unsafe { Socket2::from_raw_fd(self.socket.fd) };
        let addr = socket.local_addr()
            .map_err(|e| Error::Network(format!("Failed to get local addr: {}", e)))?;
        std::mem::forget(socket); // Don't drop the socket
        
        if let Some(addr) = addr.as_socket() {
            Ok(SocketAddress::Inet(addr))
        } else {
            Err(Error::Network("Unsupported address type".to_string()))
        }
    }
    
    /// Get remote socket address
    pub fn peer_addr(&self) -> Result<SocketAddress> {
        let socket = unsafe { Socket2::from_raw_fd(self.socket.fd) };
        let addr = socket.peer_addr()
            .map_err(|e| Error::Network(format!("Failed to get peer addr: {}", e)))?;
        std::mem::forget(socket); // Don't drop the socket
        
        if let Some(addr) = addr.as_socket() {
            Ok(SocketAddress::Inet(addr))
        } else {
            Err(Error::Network("Unsupported address type".to_string()))
        }
    }
    
    /// Shutdown the socket for reading, writing, or both
    pub fn shutdown(&self, how: std::net::Shutdown) -> Result<()> {
        let socket = unsafe { Socket2::from_raw_fd(self.socket.fd) };
        let result = socket.shutdown(how);
        std::mem::forget(socket); // Don't drop the socket
        result.map_err(|e| Error::Network(format!("Failed to shutdown: {}", e)))
    }
    
    /// Close the socket
    pub async fn close(self) -> Result<()> {
        // Socket will be closed when dropped
        Ok(())
    }
}

/// Create a socket pair (for testing or IPC)
pub fn socket_pair() -> Result<(ConnectedSocket, ConnectedSocket)> {
    let mut fds: [libc::c_int; 2] = [0, 0];
    let result = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM,
            0,
            fds.as_mut_ptr(),
        )
    };
    
    if result < 0 {
        return Err(Error::Network("Failed to create socket pair".to_string()));
    }
    
    Ok((
        ConnectedSocket::from_raw_fd(fds[0]),
        ConnectedSocket::from_raw_fd(fds[1]),
    ))
}