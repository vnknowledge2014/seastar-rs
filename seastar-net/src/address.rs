//! Socket address types

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use seastar_core::{Error, Result};

/// Socket address that can represent IPv4, IPv6, or Unix domain sockets
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketAddress {
    Inet(SocketAddr),
    Unix(String),
}

impl SocketAddress {
    /// Create a new IPv4 socket address
    pub fn new_v4(ip: Ipv4Addr, port: u16) -> Self {
        Self::Inet(SocketAddr::new(IpAddr::V4(ip), port))
    }
    
    /// Create a new IPv6 socket address
    pub fn new_v6(ip: Ipv6Addr, port: u16) -> Self {
        Self::Inet(SocketAddr::new(IpAddr::V6(ip), port))
    }
    
    /// Create a Unix domain socket address
    pub fn unix<P: Into<String>>(path: P) -> Self {
        Self::Unix(path.into())
    }
    
    /// Get the port for IP addresses
    pub fn port(&self) -> Option<u16> {
        match self {
            Self::Inet(addr) => Some(addr.port()),
            Self::Unix(_) => None,
        }
    }
    
    /// Get the IP address for IP addresses
    pub fn ip(&self) -> Option<IpAddr> {
        match self {
            Self::Inet(addr) => Some(addr.ip()),
            Self::Unix(_) => None,
        }
    }
    
    /// Check if this is an IPv4 address
    pub fn is_ipv4(&self) -> bool {
        matches!(self, Self::Inet(addr) if addr.is_ipv4())
    }
    
    /// Check if this is an IPv6 address
    pub fn is_ipv6(&self) -> bool {
        matches!(self, Self::Inet(addr) if addr.is_ipv6())
    }
    
    /// Check if this is a Unix domain socket
    pub fn is_unix(&self) -> bool {
        matches!(self, Self::Unix(_))
    }
}

impl From<SocketAddr> for SocketAddress {
    fn from(addr: SocketAddr) -> Self {
        Self::Inet(addr)
    }
}

impl From<(IpAddr, u16)> for SocketAddress {
    fn from((ip, port): (IpAddr, u16)) -> Self {
        Self::Inet(SocketAddr::new(ip, port))
    }
}

impl FromStr for SocketAddress {
    type Err = Error;
    
    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with('/') || s.starts_with('@') {
            // Unix domain socket
            Ok(Self::Unix(s.to_string()))
        } else {
            // Try to parse as IP address
            s.parse::<SocketAddr>()
                .map(Self::Inet)
                .map_err(|_| Error::InvalidArgument(format!("Invalid socket address: {}", s)))
        }
    }
}

/// IP address type
pub type IpAddress = IpAddr;

/// Create a socket address from a string
pub fn parse_socket_address(s: &str) -> Result<SocketAddress> {
    s.parse()
}

/// Common socket addresses
pub mod common {
    use super::*;
    
    /// Localhost IPv4 address
    pub fn localhost_v4(port: u16) -> SocketAddress {
        SocketAddress::new_v4(Ipv4Addr::LOCALHOST, port)
    }
    
    /// Localhost IPv6 address
    pub fn localhost_v6(port: u16) -> SocketAddress {
        SocketAddress::new_v6(Ipv6Addr::LOCALHOST, port)
    }
    
    /// Any IPv4 address (0.0.0.0)
    pub fn any_v4(port: u16) -> SocketAddress {
        SocketAddress::new_v4(Ipv4Addr::UNSPECIFIED, port)
    }
    
    /// Any IPv6 address (::)
    pub fn any_v6(port: u16) -> SocketAddress {
        SocketAddress::new_v6(Ipv6Addr::UNSPECIFIED, port)
    }
}