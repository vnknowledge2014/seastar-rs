//! UDP networking implementation

use seastar_core::{Future, Result, Error};
use seastar_core::io::{IoBackend, IoRequest, IoOpType, generate_request_id, AsyncIoOp};
use crate::{SocketAddress, Socket};
use socket2::{Socket as Socket2, Domain, Type, Protocol};
use std::os::unix::io::{RawFd, AsRawFd, FromRawFd};
use std::sync::{Arc, Mutex};
use bytes::{Bytes, BytesMut};

/// UDP socket for datagram communication
pub struct UdpSocket {
    socket: Socket,
    io_backend: Option<Arc<Mutex<dyn IoBackend>>>,
}

impl UdpSocket {
    /// Bind to a socket address
    pub async fn bind(addr: SocketAddress) -> Result<Self> {
        // Create UDP socket
        let mut socket = Socket::new(
            Domain::IPV4, // TODO: detect from address
            Type::DGRAM,
            Some(Protocol::UDP)
        )?;
        
        // Set socket options
        socket.set_nonblocking(true)?;
        
        // Bind to the address
        socket.bind(&addr)?;
        
        Ok(Self {
            socket,
            io_backend: None,
        })
    }
    
    /// Create an unbound UDP socket
    pub fn new() -> Result<Self> {
        let socket = Socket::new(
            Domain::IPV4,
            Type::DGRAM,
            Some(Protocol::UDP)
        )?;
        
        Ok(Self {
            socket,
            io_backend: None,
        })
    }
    
    /// Set the I/O backend for this socket
    pub fn set_io_backend(&mut self, backend: Arc<Mutex<dyn IoBackend>>) {
        self.socket.set_io_backend(backend.clone());
        self.io_backend = Some(backend);
    }
    
    /// Send data to a remote address
    pub async fn send_to(&mut self, buffer: &[u8], addr: &SocketAddress) -> Result<usize> {
        if let Some(backend) = &self.io_backend {
            // Use async I/O backend (sendmsg)
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.raw_fd(),
                op_type: IoOpType::SendMsg,
                buffer: buffer.as_ptr() as *mut u8,
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            Ok(result.bytes_transferred)
        } else {
            // Fallback to blocking sendto
            self.send_to_blocking(buffer, addr)
        }
    }
    
    /// Receive data from any address
    pub async fn recv_from(&mut self, buffer: &mut [u8]) -> Result<(usize, SocketAddress)> {
        if let Some(backend) = &self.io_backend {
            // Use async I/O backend (recvmsg)
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.raw_fd(),
                op_type: IoOpType::RecvMsg,
                buffer: buffer.as_mut_ptr(),
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            
            // For now, return a placeholder address - in real implementation
            // this would extract the source address from the received message
            let placeholder_addr = SocketAddress::Inet(
                std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::new(0, 0, 0, 0), 0
                ))
            );
            
            Ok((result.bytes_transferred, placeholder_addr))
        } else {
            // Fallback to blocking recvfrom
            self.recv_from_blocking(buffer)
        }
    }
    
    /// Blocking send implementation using socket2
    fn send_to_blocking(&self, buffer: &[u8], addr: &SocketAddress) -> Result<usize> {
        let socket = unsafe { Socket2::from_raw_fd(self.socket.raw_fd()) };
        let result = match addr {
            SocketAddress::Inet(socket_addr) => {
                socket.send_to(buffer, &(*socket_addr).into())
            }
            SocketAddress::Unix(_) => {
                return Err(Error::InvalidArgument("Unix sockets not yet supported".to_string()));
            }
        };
        std::mem::forget(socket); // Don't close the socket
        
        result.map_err(|e| Error::Network(format!("sendto failed: {}", e)))
    }
    
    /// Blocking receive implementation using direct syscalls
    fn recv_from_blocking(&self, buffer: &mut [u8]) -> Result<(usize, SocketAddress)> {
        let fd = self.socket.raw_fd();
        let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        
        let result = unsafe {
            libc::recvfrom(
                fd,
                buffer.as_mut_ptr() as *mut libc::c_void,
                buffer.len(),
                0,
                &mut addr as *mut _ as *mut libc::sockaddr,
                &mut addr_len,
            )
        };
        
        if result < 0 {
            return Err(Error::Network("recvfrom failed".to_string()));
        }
        
        // Convert sockaddr to SocketAddress (simplified for cross-platform compatibility)
        let src_addr = SocketAddress::Inet(
            std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::new(127, 0, 0, 1), // placeholder
                8080
            ))
        );
        
        Ok((result as usize, src_addr))
    }
    
    /// Get the local address this socket is bound to
    pub fn local_addr(&self) -> Result<SocketAddress> {
        let socket = unsafe { Socket2::from_raw_fd(self.socket.raw_fd()) };
        let addr = socket.local_addr()
            .map_err(|e| Error::Network(format!("Failed to get local addr: {}", e)))?;
        std::mem::forget(socket); // Don't drop the socket
        
        if let Some(addr) = addr.as_socket() {
            Ok(SocketAddress::Inet(addr))
        } else {
            Err(Error::Network("Unsupported address type".to_string()))
        }
    }
    
    /// Connect this socket to a remote address
    pub async fn connect(&mut self, addr: &SocketAddress) -> Result<()> {
        self.socket.connect(addr).await
    }
    
    /// Send data to the connected address (requires connect() first)
    pub async fn send(&mut self, buffer: &[u8]) -> Result<usize> {
        if let Some(backend) = &self.io_backend {
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.raw_fd(),
                op_type: IoOpType::Write,
                buffer: buffer.as_ptr() as *mut u8,
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            Ok(result.bytes_transferred)
        } else {
            self.send_blocking(buffer)
        }
    }
    
    /// Receive data from the connected address (requires connect() first)
    pub async fn recv(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if let Some(backend) = &self.io_backend {
            let request = IoRequest {
                id: generate_request_id(),
                fd: self.socket.raw_fd(),
                op_type: IoOpType::Read,
                buffer: buffer.as_mut_ptr(),
                length: buffer.len(),
                offset: 0,
            };
            
            let io_op = AsyncIoOp::new(request, backend.clone())?;
            let result = io_op.wait().await?;
            Ok(result.bytes_transferred)
        } else {
            self.recv_blocking(buffer)
        }
    }
    
    /// Blocking send for connected socket
    fn send_blocking(&self, buffer: &[u8]) -> Result<usize> {
        let result = unsafe {
            libc::send(
                self.socket.raw_fd(),
                buffer.as_ptr() as *const libc::c_void,
                buffer.len(),
                0,
            )
        };
        
        if result < 0 {
            Err(Error::Network("send failed".to_string()))
        } else {
            Ok(result as usize)
        }
    }
    
    /// Blocking receive for connected socket
    fn recv_blocking(&self, buffer: &mut [u8]) -> Result<usize> {
        let result = unsafe {
            libc::recv(
                self.socket.raw_fd(),
                buffer.as_mut_ptr() as *mut libc::c_void,
                buffer.len(),
                0,
            )
        };
        
        if result < 0 {
            Err(Error::Network("recv failed".to_string()))
        } else {
            Ok(result as usize)
        }
    }
}

/// Datagram channel for efficient UDP communication
pub struct DatagramChannel {
    socket: UdpSocket,
}

impl DatagramChannel {
    /// Create a new unbound datagram channel
    pub fn new() -> Result<Self> {
        let socket = UdpSocket::new()?;
        Ok(Self { socket })
    }
    
    /// Create a bound datagram channel
    pub async fn bind(addr: SocketAddress) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket })
    }
    
    /// Set the I/O backend for this channel
    pub fn set_io_backend(&mut self, backend: Arc<Mutex<dyn IoBackend>>) {
        self.socket.set_io_backend(backend);
    }
    
    /// Send a datagram to the specified address
    pub async fn send_to(&mut self, data: &[u8], addr: &SocketAddress) -> Result<usize> {
        self.socket.send_to(data, addr).await
    }
    
    /// Receive a datagram from any address
    pub async fn recv_from(&mut self, buffer: &mut [u8]) -> Result<(usize, SocketAddress)> {
        self.socket.recv_from(buffer).await
    }
    
    /// Get the local address this channel is bound to
    pub fn local_addr(&self) -> Result<SocketAddress> {
        self.socket.local_addr()
    }
    
    /// Get access to the underlying UDP socket
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }
    
    /// Get mutable access to the underlying UDP socket
    pub fn socket_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }
}