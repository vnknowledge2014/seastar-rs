//! # Seastar-RS Networking Stack
//! 
//! High-performance user-space networking stack with zero-copy I/O,
//! advanced TCP features, and protocol acceleration.

pub mod socket;
pub mod tcp;
pub mod udp;
pub mod address;
pub mod listener;
pub mod packet;
pub mod buffer;
pub mod stack;
pub mod protocols;
pub mod zero_copy;
pub mod connection_pool;
pub mod load_balancer;

pub use socket::{Socket, ConnectedSocket};
pub use tcp::{TcpListener, TcpStream, TcpConnection};
pub use udp::{UdpSocket, DatagramChannel};
pub use address::{SocketAddress, IpAddress};
pub use listener::ServerSocket;
pub use packet::{Packet, PacketHeader};
pub use buffer::{NetworkBuffer, BufferPool, ZeroCopyBuffer};
pub use stack::{NetworkStack, StackBuilder, StackConfig};
pub use protocols::{Protocol, ProtocolHandler};
pub use zero_copy::{ZeroCopySocket, ZeroCopyStream};
pub use connection_pool::{ConnectionPool, PooledConnection};
pub use load_balancer::{LoadBalancer, LoadBalanceStrategy};