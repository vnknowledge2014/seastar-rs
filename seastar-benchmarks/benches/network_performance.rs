use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use seastar_core::prelude::*;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{Bytes, BytesMut};

async fn setup_echo_server(port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
        
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0; 8192];
                    
                    loop {
                        match socket.read(&mut buffer).await {
                            Ok(0) => break, // Connection closed
                            Ok(n) => {
                                if socket.write_all(&buffer[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
        }
    })
}

fn bench_tcp_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("tcp_throughput");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));
    
    for message_size in [64, 256, 1024, 4096, 16384].iter() {
        group.bench_with_input(
            BenchmarkId::new("tcp_echo", message_size),
            message_size,
            |b, &message_size| {
                b.iter(|| {
                    rt.block_on(async {
                        let port = 18080;
                        let _server = setup_echo_server(port).await;
                        
                        // Give server time to start
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        
                        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                            .await
                            .unwrap();
                        
                        let message = vec![0xAA; message_size];
                        let mut response = vec![0; message_size];
                        
                        // Send and receive message
                        stream.write_all(&message).await.unwrap();
                        stream.read_exact(&mut response).await.unwrap();
                        
                        assert_eq!(message, response);
                        black_box(response);
                    });
                });
            },
        );
    }
    
    group.finish();
}

fn bench_tcp_connection_setup(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("tcp_connection_setup", |b| {
        b.iter(|| {
            rt.block_on(async {
                let port = 18081;
                let _server = setup_echo_server(port).await;
                
                // Give server time to start
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                
                // Measure connection establishment time
                let start = std::time::Instant::now();
                let _stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                    .await
                    .unwrap();
                let connection_time = start.elapsed();
                
                black_box(connection_time);
            });
        });
    });
}

fn bench_concurrent_connections(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("concurrent_connections");
    group.sample_size(10);
    
    for connection_count in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_tcp", connection_count),
            connection_count,
            |b, &connection_count| {
                b.iter(|| {
                    rt.block_on(async {
                        let port = 18082;
                        let _server = setup_echo_server(port).await;
                        
                        // Give server time to start
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        
                        let mut handles = Vec::new();
                        
                        for _ in 0..connection_count {
                            let handle = tokio::spawn(async move {
                                let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                                    .await
                                    .unwrap();
                                
                                let message = b"Hello, World!";
                                let mut response = [0; 13];
                                
                                stream.write_all(message).await.unwrap();
                                stream.read_exact(&mut response).await.unwrap();
                                
                                assert_eq!(message, &response);
                            });
                            handles.push(handle);
                        }
                        
                        // Wait for all connections to complete
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );
    }
    
    group.finish();
}

fn bench_udp_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("udp_throughput");
    
    for packet_size in [64, 256, 1024, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::new("udp_echo", packet_size),
            packet_size,
            |b, &packet_size| {
                b.iter(|| {
                    rt.block_on(async {
                        use tokio::net::UdpSocket;
                        
                        let server_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                        let server_addr = server_socket.local_addr().unwrap();
                        
                        // Server task
                        let server_handle = tokio::spawn(async move {
                            let mut buf = vec![0; 65536];
                            if let Ok((len, addr)) = server_socket.recv_from(&mut buf).await {
                                server_socket.send_to(&buf[..len], addr).await.unwrap();
                            }
                        });
                        
                        // Client
                        let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                        let message = vec![0xBB; packet_size];
                        let mut response = vec![0; packet_size];
                        
                        client_socket.send_to(&message, server_addr).await.unwrap();
                        let (len, _) = client_socket.recv_from(&mut response).await.unwrap();
                        
                        assert_eq!(len, packet_size);
                        assert_eq!(message, response[..len]);
                        
                        server_handle.await.unwrap();
                        black_box(response);
                    });
                });
            },
        );
    }
    
    group.finish();
}

fn bench_buffer_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_serialization");
    
    for data_size in [256, 1024, 4096, 16384].iter() {
        let data = vec![0xCC; *data_size];
        
        group.bench_with_input(
            BenchmarkId::new("bytes_from_vec", data_size),
            &data,
            |b, data| {
                b.iter(|| {
                    let bytes = Bytes::copy_from_slice(data);
                    black_box(bytes);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("bytesmut_extend", data_size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut buf = BytesMut::new();
                    buf.extend_from_slice(data);
                    let frozen = buf.freeze();
                    black_box(frozen);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_zero_copy_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("zero_copy_buffer_sharing", |b| {
        b.iter(|| {
            rt.block_on(async {
                let original_data = vec![0xDD; 4096];
                let bytes = Bytes::copy_from_slice(&original_data);
                
                // Simulate sharing buffer across multiple tasks
                let mut handles = Vec::new();
                
                for _ in 0..10 {
                    let bytes_clone = bytes.clone(); // This should be zero-copy
                    let handle = tokio::spawn(async move {
                        // Simulate processing
                        let sum: u64 = bytes_clone.iter().map(|&b| b as u64).sum();
                        black_box(sum);
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.await.unwrap();
                }
            });
        });
    });
}

criterion_group!(
    benches,
    bench_tcp_throughput,
    bench_tcp_connection_setup,
    bench_concurrent_connections,
    bench_udp_throughput,
    bench_buffer_serialization,
    bench_zero_copy_operations
);
criterion_main!(benches);