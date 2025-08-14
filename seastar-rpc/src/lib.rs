//! # Seastar-RS RPC
//! 
//! High-performance RPC framework for Seastar-RS with serialization support.
//! 
//! This crate provides a complete RPC system with:
//! - Multiple serialization formats (JSON, MessagePack, Bincode)
//! - Type-safe method handlers
//! - Service discovery and reflection
//! - Connection management and request routing
//! - Async/await support with Tokio integration

pub mod client;
pub mod server;
pub mod protocol;
pub mod codec;

// Client exports
pub use client::{RpcClient, RpcClientConfig, RpcClientBuilder};

// Server exports
pub use server::{
    RpcServer, RpcServerConfig, RpcService,
    RpcMethodHandler, ClosureHandler, TypedHandler
};

// Protocol exports
pub use protocol::{
    RpcProtocol, RpcProtocolConfig, RpcMessageType, RpcError, RpcErrorCode,
    RequestId, ServiceName, MethodName, RpcServiceRegistry, RpcServiceInfo, RpcMethodInfo
};

// Codec exports
pub use codec::{
    RpcCodec, RpcMessage, SerializationFormat, TypedRpcCodec, WireProtocol
};

// Prelude for common imports
pub mod prelude {
    //! Common imports for Seastar-RS RPC applications
    
    pub use crate::client::{RpcClient, RpcClientBuilder};
    pub use crate::server::{RpcServer, RpcService, RpcMethodHandler};
    pub use crate::protocol::{RpcError, RpcErrorCode, ServiceName, MethodName};
    pub use crate::codec::{RpcCodec, SerializationFormat};
    pub use seastar_core::{Result, Error};
    pub use serde::{Serialize, Deserialize};
}