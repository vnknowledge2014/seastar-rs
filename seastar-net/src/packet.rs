//! Network packet handling and processing

use bytes::{Bytes, BytesMut};
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

/// Network packet representation
#[derive(Debug, Clone)]
pub struct Packet {
    pub header: PacketHeader,
    pub payload: Bytes,
    pub timestamp: Instant,
}

/// Packet header information
#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub src_addr: SocketAddr,
    pub dst_addr: SocketAddr,
    pub protocol: Protocol,
    pub flags: PacketFlags,
    pub sequence: Option<u32>,
    pub acknowledgment: Option<u32>,
    pub window_size: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Protocol {
    Tcp,
    Udp,
    Icmp,
}

#[derive(Debug, Clone, Default)]
pub struct PacketFlags {
    pub syn: bool,
    pub ack: bool,
    pub fin: bool,
    pub rst: bool,
    pub psh: bool,
    pub urg: bool,
}

impl Packet {
    pub fn new(header: PacketHeader, payload: Bytes) -> Self {
        Self {
            header,
            payload,
            timestamp: Instant::now(),
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}