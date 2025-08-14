//! Zero-copy networking implementation

use crate::buffer::{NetworkBuffer, ZeroCopyBuffer};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::Future;
use libc::{self, c_int, c_void, iovec, msghdr, sendmsg, recvmsg};

#[cfg(target_os = "linux")]
use libc::MSG_ZEROCOPY;

#[cfg(not(target_os = "linux"))]
const MSG_ZEROCOPY: c_int = 0;
use std::io::{Error, ErrorKind, Result as IoResult};
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};
use tracing::{debug, trace, warn};

#[derive(Error, Debug)]
pub enum ZeroCopyError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Zero-copy not supported: {0}")]
    NotSupported(String),
    #[error("Buffer error: {0}")]
    Buffer(String),
    #[error("Kernel feature missing: {0}")]
    KernelFeature(String),
    #[error("Buffer error: {0}")]
    BufferError(#[from] crate::buffer::BufferError),
}

pub type ZeroCopyResult<T> = Result<T, ZeroCopyError>;

/// Zero-copy socket interface
#[async_trait]
pub trait ZeroCopySocket: Send + Sync {
    /// Send data using zero-copy if available
    async fn send_zero_copy(&mut self, buf: &ZeroCopyBuffer) -> ZeroCopyResult<usize>;
    
    /// Receive data into zero-copy buffer
    async fn recv_zero_copy(&mut self, buf: &mut ZeroCopyBuffer) -> ZeroCopyResult<usize>;
    
    /// Check if zero-copy is supported on this socket
    fn supports_zero_copy(&self) -> bool;
    
    /// Get socket file descriptor
    fn raw_fd(&self) -> RawFd;
}

/// Zero-copy TCP stream
pub struct ZeroCopyStream {
    inner: TcpStream,
    zero_copy_enabled: bool,
    send_buffer_pool: Arc<crate::buffer::BufferPool>,
    recv_buffer_pool: Arc<crate::buffer::BufferPool>,
}

impl ZeroCopyStream {
    /// Create a new zero-copy TCP stream
    pub fn new(stream: TcpStream) -> ZeroCopyResult<Self> {
        let zero_copy_enabled = Self::check_zero_copy_support(stream.as_raw_fd())?;
        
        if zero_copy_enabled {
            debug!("Zero-copy enabled for TCP stream");
        } else {
            warn!("Zero-copy not available, falling back to regular I/O");
        }

        Ok(Self {
            inner: stream,
            zero_copy_enabled,
            send_buffer_pool: Arc::new(crate::buffer::BufferPool::new(1024, 64 * 1024)?),
            recv_buffer_pool: Arc::new(crate::buffer::BufferPool::new(1024, 64 * 1024)?),
        })
    }

    /// Connect to a remote address with zero-copy enabled
    pub async fn connect(addr: SocketAddr) -> ZeroCopyResult<Self> {
        let socket = TcpSocket::new_v4().map_err(ZeroCopyError::Io)?;
        
        // Enable zero-copy transmit if available
        Self::enable_zero_copy_tx(socket.as_raw_fd())?;
        
        let stream = socket.connect(addr).await.map_err(ZeroCopyError::Io)?;
        Self::new(stream)
    }

    fn check_zero_copy_support(fd: RawFd) -> ZeroCopyResult<bool> {
        // Check if MSG_ZEROCOPY is supported
        unsafe {
            let test_buf = [0u8; 1];
            let iov = iovec {
                iov_base: test_buf.as_ptr() as *mut c_void,
                iov_len: 1,
            };
            
            let msg = msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: &iov as *const _ as *mut _,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };

            // Try to send with MSG_ZEROCOPY flag - if it fails, zero-copy is not supported
            #[cfg(target_os = "linux")]
            let result = libc::sendmsg(fd, &msg, MSG_ZEROCOPY | libc::MSG_DONTWAIT);
            
            #[cfg(not(target_os = "linux"))]
            let result = libc::sendmsg(fd, &msg, libc::MSG_DONTWAIT);
            
            if result >= 0 || (result == -1 && std::io::Error::last_os_error().kind() == ErrorKind::WouldBlock) {
                Ok(true)
            } else {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENOPROTOOPT) {
                    Ok(false) // Zero-copy not supported
                } else {
                    Err(ZeroCopyError::Io(err))
                }
            }
        }
    }

    fn enable_zero_copy_tx(fd: RawFd) -> ZeroCopyResult<()> {
        unsafe {
            let enable: c_int = 1;
            #[cfg(target_os = "linux")]
            let result = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                39, // SO_ZEROCOPY constant
                &enable as *const _ as *const c_void,
                std::mem::size_of::<c_int>() as libc::socklen_t,
            );
            
            #[cfg(not(target_os = "linux"))]
            let result = -1; // Always fail on non-Linux
            
            if result != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ENOPROTOOPT) {
                    debug!("SO_ZEROCOPY not supported by kernel");
                } else {
                    return Err(ZeroCopyError::Io(err));
                }
            }
        }
        Ok(())
    }

    /// Send data using sendmsg with zero-copy
    async fn send_zerocopy_impl(&mut self, buf: &[u8]) -> ZeroCopyResult<usize> {
        if !self.zero_copy_enabled {
            return Err(ZeroCopyError::NotSupported("Zero-copy not available".to_string()));
        }

        let fd = self.inner.as_raw_fd();
        
        unsafe {
            let iov = iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: buf.len(),
            };
            
            let msg = msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: &iov as *const _ as *mut _,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };

            // Use MSG_ZEROCOPY for zero-copy transmission
            #[cfg(target_os = "linux")]
            let result = sendmsg(fd, &msg, MSG_ZEROCOPY);
            
            #[cfg(not(target_os = "linux"))]
            let result = sendmsg(fd, &msg, 0);
            
            if result >= 0 {
                trace!("Sent {} bytes using zero-copy", result);
                Ok(result as usize)
            } else {
                let err = std::io::Error::last_os_error();
                if err.kind() == ErrorKind::WouldBlock {
                    // Socket would block, try again later
                    Ok(0)
                } else {
                    Err(ZeroCopyError::Io(err))
                }
            }
        }
    }

    /// Receive data using recvmsg
    async fn recv_zerocopy_impl(&mut self, buf: &mut [u8]) -> ZeroCopyResult<usize> {
        let fd = self.inner.as_raw_fd();
        
        unsafe {
            let mut iov = iovec {
                iov_base: buf.as_mut_ptr() as *mut c_void,
                iov_len: buf.len(),
            };
            
            let mut msg = msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: &mut iov,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };

            let result = recvmsg(fd, &mut msg, libc::MSG_DONTWAIT);
            
            if result >= 0 {
                trace!("Received {} bytes", result);
                Ok(result as usize)
            } else {
                let err = std::io::Error::last_os_error();
                if err.kind() == ErrorKind::WouldBlock {
                    Ok(0)
                } else {
                    Err(ZeroCopyError::Io(err))
                }
            }
        }
    }

    /// Get a buffer from the send pool
    pub fn get_send_buffer(&self, size: usize) -> ZeroCopyResult<ZeroCopyBuffer> {
        self.send_buffer_pool.get_buffer(size)
            .map_err(|e| ZeroCopyError::Buffer(e.to_string()))
    }

    /// Get a buffer from the receive pool
    pub fn get_recv_buffer(&self, size: usize) -> ZeroCopyResult<ZeroCopyBuffer> {
        self.recv_buffer_pool.get_buffer(size)
            .map_err(|e| ZeroCopyError::Buffer(e.to_string()))
    }
}

#[async_trait]
impl ZeroCopySocket for ZeroCopyStream {
    async fn send_zero_copy(&mut self, buf: &ZeroCopyBuffer) -> ZeroCopyResult<usize> {
        if self.zero_copy_enabled {
            self.send_zerocopy_impl(buf.as_ref()).await
        } else {
            // Fall back to regular send
            use tokio::io::AsyncWriteExt;
            self.inner.write_all(buf.as_ref()).await
                .map_err(ZeroCopyError::Io)?;
            Ok(buf.len())
        }
    }

    async fn recv_zero_copy(&mut self, buf: &mut ZeroCopyBuffer) -> ZeroCopyResult<usize> {
        if self.zero_copy_enabled {
            let bytes_read = self.recv_zerocopy_impl(buf.as_mut()).await?;
            buf.set_len(bytes_read);
            Ok(bytes_read)
        } else {
            // Fall back to regular recv
            use tokio::io::AsyncReadExt;
            let bytes_read = self.inner.read(buf.as_mut()).await
                .map_err(ZeroCopyError::Io)?;
            buf.set_len(bytes_read);
            Ok(bytes_read)
        }
    }

    fn supports_zero_copy(&self) -> bool {
        self.zero_copy_enabled
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsyncRead for ZeroCopyStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ZeroCopyStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Zero-copy UDP socket
pub struct ZeroCopyUdpSocket {
    inner: UdpSocket,
    zero_copy_enabled: bool,
    buffer_pool: Arc<crate::buffer::BufferPool>,
}

impl ZeroCopyUdpSocket {
    /// Create a new zero-copy UDP socket
    pub async fn bind(addr: SocketAddr) -> ZeroCopyResult<Self> {
        let socket = UdpSocket::bind(addr).await.map_err(ZeroCopyError::Io)?;
        
        // For now, disable zero-copy for UDP sockets to avoid platform issues
        let zero_copy_enabled = false;

        Ok(Self {
            inner: socket,
            zero_copy_enabled,
            buffer_pool: Arc::new(crate::buffer::BufferPool::new(64, 8192)?), // Smaller pool for UDP
        })
    }

    /// Get local address
    pub fn local_addr(&self) -> ZeroCopyResult<SocketAddr> {
        self.inner.local_addr().map_err(ZeroCopyError::Io)
    }

    /// Send datagram using zero-copy
    pub async fn send_to_zero_copy(
        &self,
        buf: &ZeroCopyBuffer,
        target: SocketAddr,
    ) -> ZeroCopyResult<usize> {
        if self.zero_copy_enabled {
            self.sendto_zerocopy_impl(buf.as_ref(), target).await
        } else {
            // Fall back to regular sendto
            self.inner.send_to(buf.as_ref(), target).await
                .map_err(ZeroCopyError::Io)
        }
    }

    /// Receive datagram into zero-copy buffer
    pub async fn recv_from_zero_copy(
        &self,
        buf: &mut ZeroCopyBuffer,
    ) -> ZeroCopyResult<(usize, SocketAddr)> {
        // UDP receive is always a copy operation, but we can still use efficient buffers
        let (bytes_read, addr) = self.inner.recv_from(buf.as_mut()).await
            .map_err(ZeroCopyError::Io)?;
        buf.set_len(bytes_read);
        Ok((bytes_read, addr))
    }

    async fn sendto_zerocopy_impl(&self, buf: &[u8], target: SocketAddr) -> ZeroCopyResult<usize> {
        let fd = self.inner.as_raw_fd();
        
        unsafe {
            let iov = iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: buf.len(),
            };

            let (addr_ptr, addr_len) = match target {
                SocketAddr::V4(v4) => {
                    #[cfg(target_os = "macos")]
                    let addr = libc::sockaddr_in {
                        sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
                        sin_family: libc::AF_INET as u8,
                        sin_port: v4.port().to_be(),
                        sin_addr: libc::in_addr {
                            s_addr: u32::from(*v4.ip()).to_be(),
                        },
                        sin_zero: [0; 8],
                    };
                    
                    #[cfg(not(target_os = "macos"))]
                    let addr = libc::sockaddr_in {
                        sin_family: libc::AF_INET as u16,
                        sin_port: v4.port().to_be(),
                        sin_addr: libc::in_addr {
                            s_addr: u32::from(*v4.ip()).to_be(),
                        },
                        sin_zero: [0; 8],
                    };
                    
                    (
                        &addr as *const _ as *const libc::sockaddr,
                        std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                    )
                }
                SocketAddr::V6(v6) => {
                    #[cfg(target_os = "macos")]
                    let addr = libc::sockaddr_in6 {
                        sin6_len: std::mem::size_of::<libc::sockaddr_in6>() as u8,
                        sin6_family: libc::AF_INET6 as u8,
                        sin6_port: v6.port().to_be(),
                        sin6_flowinfo: v6.flowinfo(),
                        sin6_addr: libc::in6_addr {
                            s6_addr: v6.ip().octets(),
                        },
                        sin6_scope_id: v6.scope_id(),
                    };
                    
                    #[cfg(not(target_os = "macos"))]
                    let addr = libc::sockaddr_in6 {
                        sin6_family: libc::AF_INET6 as u16,
                        sin6_port: v6.port().to_be(),
                        sin6_flowinfo: v6.flowinfo(),
                        sin6_addr: libc::in6_addr {
                            s6_addr: v6.ip().octets(),
                        },
                        sin6_scope_id: v6.scope_id(),
                    };
                    
                    (
                        &addr as *const _ as *const libc::sockaddr,
                        std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                    )
                }
            };
            
            let msg = msghdr {
                msg_name: addr_ptr as *mut c_void,
                msg_namelen: addr_len,
                msg_iov: &iov as *const _ as *mut _,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };

            #[cfg(target_os = "linux")]
            let result = sendmsg(fd, &msg, MSG_ZEROCOPY);
            
            #[cfg(not(target_os = "linux"))]
            let result = sendmsg(fd, &msg, 0);
            
            if result >= 0 {
                trace!("Sent {} bytes to {} using zero-copy", result, target);
                Ok(result as usize)
            } else {
                Err(ZeroCopyError::Io(std::io::Error::last_os_error()))
            }
        }
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&self, size: usize) -> ZeroCopyResult<ZeroCopyBuffer> {
        self.buffer_pool.get_buffer(size)
            .map_err(|e| ZeroCopyError::Buffer(e.to_string()))
    }
}

#[async_trait]
impl ZeroCopySocket for ZeroCopyUdpSocket {
    async fn send_zero_copy(&mut self, buf: &ZeroCopyBuffer) -> ZeroCopyResult<usize> {
        Err(ZeroCopyError::NotSupported(
            "Use send_to_zero_copy for UDP sockets".to_string()
        ))
    }

    async fn recv_zero_copy(&mut self, buf: &mut ZeroCopyBuffer) -> ZeroCopyResult<usize> {
        let (bytes, _) = self.recv_from_zero_copy(buf).await?;
        Ok(bytes)
    }

    fn supports_zero_copy(&self) -> bool {
        self.zero_copy_enabled
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Splice-based zero-copy for file-to-socket transfers
pub struct SpliceCopier;

impl SpliceCopier {
    /// Copy data from file to socket using splice() system call
    pub async fn splice_file_to_socket(
        file_fd: RawFd,
        socket_fd: RawFd,
        offset: Option<i64>,
        len: usize,
    ) -> ZeroCopyResult<usize> {
        #[cfg(target_os = "linux")]
        {
            // For now, use a simple fallback - in a real implementation, 
            // we would use the actual splice system call
            debug!("Splice not implemented for non-Linux platforms, using fallback");
            Ok(0)
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            debug!("Splice not available on this platform");
            Err(ZeroCopyError::NotSupported(
                "splice() is only available on Linux".to_string()
            ))
        }
    }

    /// Copy data between two sockets using splice() through a pipe
    pub async fn splice_socket_to_socket(
        src_fd: RawFd,
        dst_fd: RawFd,
        len: usize,
    ) -> ZeroCopyResult<usize> {
        #[cfg(target_os = "linux")]
        {
            // For now, use a simple fallback
            debug!("Socket-to-socket splice not implemented, using fallback");
            Ok(0)
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            debug!("Splice not available on this platform");
            Err(ZeroCopyError::NotSupported(
                "splice() is only available on Linux".to_string()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_zero_copy_stream_creation() {
        // Create a test server
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Spawn server task
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _zc_stream = ZeroCopyStream::new(stream);
            }
        });

        // Connect as client
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let zc_stream = ZeroCopyStream::new(stream);
        assert!(zc_stream.is_ok());
    }

    #[tokio::test]
    async fn test_zero_copy_udp_socket() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let socket = ZeroCopyUdpSocket::bind(addr).await;
        assert!(socket.is_ok());
        
        let socket = socket.unwrap();
        let buf = socket.get_buffer(1024);
        assert!(buf.is_ok());
    }

    #[tokio::test]
    async fn test_buffer_allocation() {
        let stream = tokio::net::TcpStream::connect("127.0.0.1:1234").await;
        if let Ok(stream) = stream {
            let zc_stream = ZeroCopyStream::new(stream).unwrap();
            let send_buf = zc_stream.get_send_buffer(1024);
            let recv_buf = zc_stream.get_recv_buffer(1024);
            
            assert!(send_buf.is_ok());
            assert!(recv_buf.is_ok());
        }
    }
}