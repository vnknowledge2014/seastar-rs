use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use seastar_core::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

// Simple HTTP response builder for benchmarking
fn build_http_response(status: u16, body: &[u8]) -> Vec<u8> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found", 
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status, status_text, body.len()
    );
    
    let mut full_response = response.into_bytes();
    full_response.extend_from_slice(body);
    full_response
}

async fn simple_http_server(port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
        
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = [0; 8192];
                
                match socket.read(&mut buffer).await {
                    Ok(n) if n > 0 => {
                        let request = String::from_utf8_lossy(&buffer[..n]);
                        
                        let response = if request.starts_with("GET / ") {
                            build_http_response(200, b"Hello, World!")
                        } else if request.starts_with("GET /json") {
                            let json_body = r#"{"message":"Hello, World!","status":"success"}"#;
                            build_http_response(200, json_body.as_bytes())
                        } else if request.starts_with("GET /large") {
                            let large_body = vec![b'A'; 64 * 1024]; // 64KB response
                            build_http_response(200, &large_body)
                        } else {
                            build_http_response(404, b"Not Found")
                        };
                        
                        let _ = socket.write_all(&response).await;
                    }
                    _ => {}
                }
            });
        }
    })
}

fn bench_http_request_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_request_parsing");
    
    let requests = vec![
        ("simple_get", "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        ("get_with_headers", 
         "GET /path HTTP/1.1\r\nHost: localhost:8080\r\nUser-Agent: bench/1.0\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n"),
        ("post_with_body", 
         "POST /api/data HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 25\r\n\r\n{\"message\":\"Hello World\"}"),
    ];
    
    for (name, request) in requests {
        group.bench_with_input(
            BenchmarkId::new("parse_request", name),
            &request,
            |b, &request| {
                b.iter(|| {
                    // Simple HTTP request parsing benchmark
                    let lines: Vec<&str> = request.split("\r\n").collect();
                    let request_line = lines[0];
                    let parts: Vec<&str> = request_line.split_whitespace().collect();
                    
                    let method = parts.get(0).unwrap_or(&"GET");
                    let path = parts.get(1).unwrap_or(&"/");
                    let version = parts.get(2).unwrap_or(&"HTTP/1.1");
                    
                    let mut headers = std::collections::HashMap::new();
                    for line in &lines[1..] {
                        if line.is_empty() {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(": ") {
                            headers.insert(key.to_lowercase(), value.to_string());
                        }
                    }
                    
                    black_box((method, path, version, headers));
                });
            },
        );
    }
    
    group.finish();
}

fn bench_http_response_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_response_generation");
    
    let response_sizes = [
        ("small", 13),      // "Hello, World!"
        ("medium", 1024),   // 1KB
        ("large", 64 * 1024), // 64KB
    ];
    
    for (name, size) in response_sizes {
        group.bench_with_input(
            BenchmarkId::new("generate_response", name),
            &size,
            |b, &size| {
                let body = vec![b'X'; size];
                b.iter(|| {
                    let response = build_http_response(200, &body);
                    black_box(response);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_http_server_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("http_server_throughput");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));
    
    let endpoints = [
        ("root", "/"),
        ("json", "/json"), 
        ("large", "/large"),
    ];
    
    for (name, endpoint) in endpoints {
        group.bench_with_input(
            BenchmarkId::new("http_get", name),
            &endpoint,
            |b, &endpoint| {
                b.iter(|| {
                    rt.block_on(async {
                        let port = 19080 + (endpoint.len() % 100) as u16;
                        let _server = simple_http_server(port).await;
                        
                        // Give server time to start
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        
                        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                            .await
                            .unwrap();
                        
                        let request = format!("GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n", endpoint);
                        stream.write_all(request.as_bytes()).await.unwrap();
                        
                        let mut response = Vec::new();
                        stream.read_to_end(&mut response).await.unwrap();
                        
                        black_box(response);
                    });
                });
            },
        );
    }
    
    group.finish();
}

fn bench_concurrent_http_requests(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("concurrent_http_requests");
    group.sample_size(10);
    
    for request_count in [1, 5, 10, 25].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_requests", request_count),
            request_count,
            |b, &request_count| {
                b.iter(|| {
                    rt.block_on(async {
                        let port = 19090;
                        let _server = simple_http_server(port).await;
                        
                        // Give server time to start
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        
                        let mut handles = Vec::new();
                        
                        for i in 0..request_count {
                            let handle = tokio::spawn(async move {
                                let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
                                    .await
                                    .unwrap();
                                
                                let endpoint = if i % 3 == 0 { "/" } else if i % 3 == 1 { "/json" } else { "/large" };
                                let request = format!("GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n", endpoint);
                                stream.write_all(request.as_bytes()).await.unwrap();
                                
                                let mut response = Vec::new();
                                stream.read_to_end(&mut response).await.unwrap();
                                
                                // Basic validation that we got a response
                                assert!(response.len() > 0);
                                assert!(response.starts_with(b"HTTP/1.1"));
                            });
                            handles.push(handle);
                        }
                        
                        // Wait for all requests to complete
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

fn bench_http_header_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_header_processing");
    
    let header_counts = [
        ("few_headers", 3),
        ("many_headers", 15),
        ("large_headers", 50),
    ];
    
    for (name, header_count) in header_counts {
        group.bench_with_input(
            BenchmarkId::new("process_headers", name),
            &header_count,
            |b, &header_count| {
                // Generate test headers
                let mut headers_text = String::from("GET / HTTP/1.1\r\n");
                for i in 0..header_count {
                    headers_text.push_str(&format!("Header-{}: Value-{}\r\n", i, i));
                }
                headers_text.push_str("\r\n");
                
                b.iter(|| {
                    let mut headers = std::collections::HashMap::new();
                    
                    for line in headers_text.lines().skip(1) {
                        if line.is_empty() {
                            break;
                        }
                        if let Some((key, value)) = line.split_once(": ") {
                            headers.insert(key.to_lowercase(), value.to_string());
                        }
                    }
                    
                    black_box(headers);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_json_serialization(c: &mut Criterion) {
    use serde_json;
    use serde::{Serialize, Deserialize};
    
    #[derive(Serialize, Deserialize)]
    struct TestData {
        id: u64,
        name: String,
        active: bool,
        values: Vec<f64>,
        metadata: std::collections::HashMap<String, String>,
    }
    
    let mut group = c.benchmark_group("json_serialization");
    
    let test_data = TestData {
        id: 12345,
        name: "Test Object".to_string(),
        active: true,
        values: vec![1.0, 2.5, 3.14, 42.0, 100.5],
        metadata: {
            let mut map = std::collections::HashMap::new();
            map.insert("key1".to_string(), "value1".to_string());
            map.insert("key2".to_string(), "value2".to_string());
            map.insert("timestamp".to_string(), "2023-01-01T00:00:00Z".to_string());
            map
        },
    };
    
    group.bench_function("serialize_json", |b| {
        b.iter(|| {
            let json = serde_json::to_string(&test_data).unwrap();
            black_box(json);
        });
    });
    
    let json_string = serde_json::to_string(&test_data).unwrap();
    
    group.bench_function("deserialize_json", |b| {
        b.iter(|| {
            let data: TestData = serde_json::from_str(&json_string).unwrap();
            black_box(data);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_http_request_parsing,
    bench_http_response_generation,
    bench_http_server_throughput,
    bench_concurrent_http_requests,
    bench_http_header_processing,
    bench_json_serialization
);
criterion_main!(benches);