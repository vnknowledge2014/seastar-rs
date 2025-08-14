//! # Seastar-RS Core
//! 
//! Core async runtime and futures implementation for Seastar-RS.
//! 
//! This crate provides the fundamental building blocks for high-performance
//! asynchronous programming, inspired by the Seastar C++ framework.

pub mod error;
pub mod future;
pub mod promise;
pub mod reactor;
pub mod task;
pub mod memory;
pub mod smp;
pub mod scheduling;
pub mod timer;
pub mod io;
pub mod file;
pub mod metrics;
pub mod shutdown;
pub mod tls;
pub mod websocket;
pub mod dns;
pub mod mmap;
#[cfg(feature = "database")]
pub mod database;

pub use error::{Error, Result, ErrorCode};
pub use future::{Future, FutureExt};
pub use promise::{Promise, PromiseState};
pub use reactor::{Reactor, ReactorHandle};
pub use task::{Task, TaskHandle};
pub use memory::{MemoryPool, AlignedBuffer};
pub use smp::{Shard, ShardId, SmpService};
pub use metrics::{Counter, Gauge, Histogram, MetricRegistry, global_registry, global_system_metrics};
pub use shutdown::{ShutdownCoordinator, ShutdownResource, ShutdownPhase, ShutdownToken, global_shutdown_coordinator, register_shutdown_resource, shutdown, force_shutdown};
pub use tls::{TlsServer, TlsClient, TlsServerConfig, TlsClientConfig, SecureConnection};
pub use websocket::{WebSocketServer, WebSocketClient, WebSocketHandler, WebSocketMessage, ConnectionId};
pub use dns::{DnsResolver, DnsConfig, DnsResult, resolve, resolve_one, resolve_socket_addr};
pub use mmap::{MappedFile, MappedFileMut, SharedMappedFile, MappedBuffer};
#[cfg(feature = "database")]
pub use database::{DatabaseConnection, DatabaseConfig, DatabasePool, QueryBuilder, Migration};

pub mod prelude {
    //! Common imports for Seastar-RS applications
    
    pub use crate::error::{Error, Result, ErrorCode};
    pub use crate::future::{Future, FutureExt};
    pub use crate::promise::{Promise, PromiseState};
    pub use crate::reactor::{Reactor, ReactorHandle}; 
    pub use crate::task::{Task, TaskHandle};
    pub use crate::smp::{Shard, ShardId};
    pub use crate::metrics::{Counter, Gauge, Histogram, global_registry, global_system_metrics};
    pub use crate::shutdown::{ShutdownToken, shutdown, force_shutdown, register_shutdown_resource};
    pub use crate::tls::{TlsServer, TlsClient, TlsServerConfig, TlsClientConfig};
    pub use crate::websocket::{WebSocketServer, WebSocketClient, WebSocketHandler, WebSocketMessage};
    pub use crate::dns::{resolve, resolve_one, resolve_socket_addr};
    pub use crate::mmap::{MappedFile, MappedFileMut, SharedMappedFile, MappedBuffer};
}