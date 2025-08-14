//! RPC message encoding/decoding

use crate::protocol::{RpcMessageType, RpcProtocolConfig};
use seastar_core::{Result, Error};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::marker::PhantomData;

/// Serialization format for RPC messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// JSON serialization (human-readable)
    Json,
    /// MessagePack serialization (binary, compact)
    MessagePack,
    /// Bincode serialization (binary, fast)
    Bincode,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        SerializationFormat::Json
    }
}

/// Wire format for RPC messages
#[derive(Debug, Clone)]
pub struct RpcMessage {
    /// Message format
    pub format: SerializationFormat,
    /// Serialized message data
    pub data: Vec<u8>,
    /// Optional compression flag
    pub compressed: bool,
}

impl RpcMessage {
    /// Create a new RPC message
    pub fn new(format: SerializationFormat, data: Vec<u8>) -> Self {
        Self {
            format,
            data,
            compressed: false,
        }
    }
    
    /// Create a compressed RPC message
    pub fn new_compressed(format: SerializationFormat, data: Vec<u8>) -> Self {
        Self {
            format,
            data,
            compressed: true,
        }
    }
    
    /// Get message size
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// RPC codec for message serialization/deserialization
#[derive(Clone)]
pub struct RpcCodec {
    format: SerializationFormat,
    compression: bool,
    max_message_size: usize,
}

impl RpcCodec {
    /// Create a new RPC codec with default JSON format
    pub fn new() -> Self {
        Self {
            format: SerializationFormat::default(),
            compression: false,
            max_message_size: 16 * 1024 * 1024, // 16MB
        }
    }
    
    /// Create a new RPC codec with specified format
    pub fn with_format(format: SerializationFormat) -> Self {
        Self {
            format,
            compression: false,
            max_message_size: 16 * 1024 * 1024,
        }
    }
    
    /// Create a new RPC codec from protocol config
    pub fn from_config(config: &RpcProtocolConfig) -> Self {
        Self {
            format: SerializationFormat::Json, // Default to JSON
            compression: config.compression,
            max_message_size: config.max_message_size,
        }
    }
    
    /// Enable compression
    pub fn with_compression(mut self) -> Self {
        self.compression = true;
        self
    }
    
    /// Set maximum message size
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_message_size = max_size;
        self
    }
    
    /// Serialize an RPC message
    pub fn encode(&self, message: &RpcMessageType) -> Result<RpcMessage> {
        // Serialize message based on format
        let mut data = match self.format {
            SerializationFormat::Json => {
                serde_json::to_vec(message)
                    .map_err(|e| Error::Internal(format!("JSON serialization failed: {}", e)))?
            }
            SerializationFormat::MessagePack => {
                rmp_serde::to_vec(message)
                    .map_err(|e| Error::Internal(format!("MessagePack serialization failed: {}", e)))?
            }
            SerializationFormat::Bincode => {
                bincode::serialize(message)
                    .map_err(|e| Error::Internal(format!("Bincode serialization failed: {}", e)))?
            }
        };
        
        // Apply compression if enabled
        if self.compression {
            data = self.compress(data)?;
        }
        
        // Check message size
        if data.len() > self.max_message_size {
            return Err(Error::InvalidArgument(format!(
                "Message size {} exceeds maximum size {}",
                data.len(),
                self.max_message_size
            )));
        }
        
        Ok(RpcMessage {
            format: self.format,
            data,
            compressed: self.compression,
        })
    }
    
    /// Deserialize an RPC message
    pub fn decode(&self, message: &RpcMessage) -> Result<RpcMessageType> {
        // Check message size
        if message.data.len() > self.max_message_size {
            return Err(Error::InvalidArgument(format!(
                "Message size {} exceeds maximum size {}",
                message.data.len(),
                self.max_message_size
            )));
        }
        
        // Decompress if needed
        let data = if message.compressed {
            self.decompress(&message.data)?
        } else {
            message.data.clone()
        };
        
        // Deserialize based on format
        match message.format {
            SerializationFormat::Json => {
                serde_json::from_slice(&data)
                    .map_err(|e| Error::Internal(format!("JSON deserialization failed: {}", e)))
            }
            SerializationFormat::MessagePack => {
                rmp_serde::from_slice(&data)
                    .map_err(|e| Error::Internal(format!("MessagePack deserialization failed: {}", e)))
            }
            SerializationFormat::Bincode => {
                bincode::deserialize(&data)
                    .map_err(|e| Error::Internal(format!("Bincode deserialization failed: {}", e)))
            }
        }
    }
    
    /// Serialize typed data to bytes
    pub fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        match self.format {
            SerializationFormat::Json => {
                serde_json::to_vec(value)
                    .map_err(|e| Error::Internal(format!("JSON serialization failed: {}", e)))
            }
            SerializationFormat::MessagePack => {
                rmp_serde::to_vec(value)
                    .map_err(|e| Error::Internal(format!("MessagePack serialization failed: {}", e)))
            }
            SerializationFormat::Bincode => {
                bincode::serialize(value)
                    .map_err(|e| Error::Internal(format!("Bincode serialization failed: {}", e)))
            }
        }
    }
    
    /// Deserialize typed data from bytes
    pub fn deserialize<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T> {
        match self.format {
            SerializationFormat::Json => {
                serde_json::from_slice(data)
                    .map_err(|e| Error::Internal(format!("JSON deserialization failed: {}", e)))
            }
            SerializationFormat::MessagePack => {
                rmp_serde::from_slice(data)
                    .map_err(|e| Error::Internal(format!("MessagePack deserialization failed: {}", e)))
            }
            SerializationFormat::Bincode => {
                bincode::deserialize(data)
                    .map_err(|e| Error::Internal(format!("Bincode deserialization failed: {}", e)))
            }
        }
    }
    
    /// Get codec format
    pub fn format(&self) -> SerializationFormat {
        self.format
    }
    
    /// Check if compression is enabled
    pub fn compression_enabled(&self) -> bool {
        self.compression
    }
    
    /// Get maximum message size
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }
    
    /// Compress data using deflate
    fn compress(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        use flate2::write::DeflateEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&data)
            .map_err(|e| Error::Internal(format!("Compression failed: {}", e)))?;
        
        encoder.finish()
            .map_err(|e| Error::Internal(format!("Compression finish failed: {}", e)))
    }
    
    /// Decompress data using deflate
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::DeflateDecoder;
        use std::io::Read;
        
        let mut decoder = DeflateDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| Error::Internal(format!("Decompression failed: {}", e)))?;
        
        Ok(decompressed)
    }
}

impl Default for RpcCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Typed RPC codec for specific request/response types
pub struct TypedRpcCodec<Req, Resp> {
    inner: RpcCodec,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req, Resp> TypedRpcCodec<Req, Resp>
where
    Req: Serialize + DeserializeOwned,
    Resp: Serialize + DeserializeOwned,
{
    /// Create a new typed codec
    pub fn new(codec: RpcCodec) -> Self {
        Self {
            inner: codec,
            _phantom: PhantomData,
        }
    }
    
    /// Serialize request
    pub fn encode_request(&self, request: &Req) -> Result<Vec<u8>> {
        self.inner.serialize(request)
    }
    
    /// Deserialize request
    pub fn decode_request(&self, data: &[u8]) -> Result<Req> {
        self.inner.deserialize(data)
    }
    
    /// Serialize response
    pub fn encode_response(&self, response: &Resp) -> Result<Vec<u8>> {
        self.inner.serialize(response)
    }
    
    /// Deserialize response
    pub fn decode_response(&self, data: &[u8]) -> Result<Resp> {
        self.inner.deserialize(data)
    }
    
    /// Get inner codec
    pub fn inner(&self) -> &RpcCodec {
        &self.inner
    }
}

/// Wire protocol for RPC messages over network
#[derive(Clone)]
pub struct WireProtocol {
    codec: RpcCodec,
}

impl WireProtocol {
    /// Create a new wire protocol
    pub fn new(codec: RpcCodec) -> Self {
        Self { codec }
    }
    
    /// Encode message for wire transmission
    pub fn encode_for_wire(&self, message: &RpcMessageType) -> Result<Vec<u8>> {
        let rpc_message = self.codec.encode(message)?;
        
        // Create wire format: [length: 4 bytes][format: 1 byte][flags: 1 byte][data]
        let mut wire_data = Vec::new();
        
        // Message length (4 bytes, big-endian)
        let length = rpc_message.data.len() as u32;
        wire_data.extend_from_slice(&length.to_be_bytes());
        
        // Format byte
        let format_byte = match rpc_message.format {
            SerializationFormat::Json => 0u8,
            SerializationFormat::MessagePack => 1u8,
            SerializationFormat::Bincode => 2u8,
        };
        wire_data.push(format_byte);
        
        // Flags byte
        let mut flags = 0u8;
        if rpc_message.compressed {
            flags |= 0x01; // Compression flag
        }
        wire_data.push(flags);
        
        // Message data
        wire_data.extend_from_slice(&rpc_message.data);
        
        Ok(wire_data)
    }
    
    /// Decode message from wire transmission
    pub fn decode_from_wire(&self, wire_data: &[u8]) -> Result<RpcMessageType> {
        if wire_data.len() < 6 {
            return Err(Error::InvalidArgument("Wire data too short".to_string()));
        }
        
        // Parse header
        let length = u32::from_be_bytes([
            wire_data[0], wire_data[1], wire_data[2], wire_data[3]
        ]) as usize;
        
        let format = match wire_data[4] {
            0 => SerializationFormat::Json,
            1 => SerializationFormat::MessagePack,
            2 => SerializationFormat::Bincode,
            f => return Err(Error::InvalidArgument(format!("Unknown format: {}", f))),
        };
        
        let flags = wire_data[5];
        let compressed = (flags & 0x01) != 0;
        
        // Validate length
        if wire_data.len() < 6 + length {
            return Err(Error::InvalidArgument(format!(
                "Wire data length mismatch: expected {}, got {}",
                6 + length,
                wire_data.len()
            )));
        }
        
        // Extract message data
        let message_data = &wire_data[6..6 + length];
        
        let rpc_message = RpcMessage {
            format,
            data: message_data.to_vec(),
            compressed,
        };
        
        self.codec.decode(&rpc_message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::*;
    
    #[test]
    fn test_codec_json() {
        let codec = RpcCodec::with_format(SerializationFormat::Json);
        
        let message = RpcMessageType::Request {
            id: 1,
            service: "test".to_string(),
            method: "hello".to_string(),
            payload: b"world".to_vec(),
        };
        
        let encoded = codec.encode(&message).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        
        match decoded {
            RpcMessageType::Request { id, service, method, payload } => {
                assert_eq!(id, 1);
                assert_eq!(service, "test");
                assert_eq!(method, "hello");
                assert_eq!(payload, b"world");
            }
            _ => panic!("Unexpected message type"),
        }
    }
    
    #[test]
    fn test_wire_protocol() {
        let codec = RpcCodec::new();
        let wire = WireProtocol::new(codec);
        
        let message = RpcMessageType::Response {
            id: 42,
            payload: b"test response".to_vec(),
        };
        
        let wire_data = wire.encode_for_wire(&message).unwrap();
        let decoded = wire.decode_from_wire(&wire_data).unwrap();
        
        match decoded {
            RpcMessageType::Response { id, payload } => {
                assert_eq!(id, 42);
                assert_eq!(payload, b"test response");
            }
            _ => panic!("Unexpected message type"),
        }
    }
}