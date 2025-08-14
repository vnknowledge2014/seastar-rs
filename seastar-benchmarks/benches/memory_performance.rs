use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use seastar_core::{MemoryPool, AlignedBuffer};
use std::sync::Arc;
use bytes::{Bytes, BytesMut};

fn bench_memory_pool_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool_allocation");
    
    for size in [64, 256, 1024, 4096, 16384].iter() {
        group.bench_with_input(
            BenchmarkId::new("pool_allocate", size),
            size,
            |b, &size| {
                let pool = MemoryPool::new();
                b.iter(|| {
                    // Simulate pool allocation (simplified)
                    let buffer = vec![0u8; size];
                    black_box(buffer);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("std_allocate", size),
            size, 
            |b, &size| {
                b.iter(|| {
                    let buffer = vec![0u8; size];
                    black_box(buffer);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_aligned_buffer_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("aligned_buffer");
    
    for alignment in [16, 64, 256, 512, 4096].iter() {
        group.bench_with_input(
            BenchmarkId::new("aligned_buffer_create", alignment),
            alignment,
            |b, &alignment| {
                b.iter(|| {
                    let buffer = AlignedBuffer::new(4096, alignment);
                    black_box(buffer);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_buffer_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_operations");
    
    let sizes = [1024, 4096, 16384, 65536];
    
    for &size in &sizes {
        // BytesMut allocation and writing
        group.bench_with_input(
            BenchmarkId::new("bytesmut_write", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(size);
                    for i in 0..size {
                        buf.extend_from_slice(&[(i % 256) as u8]);
                    }
                    let frozen = buf.freeze();
                    black_box(frozen);
                });
            },
        );
        
        // Vec allocation and writing
        group.bench_with_input(
            BenchmarkId::new("vec_write", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut buf = Vec::with_capacity(size);
                    for i in 0..size {
                        buf.push((i % 256) as u8);
                    }
                    black_box(buf);
                });
            },
        );
        
        // AlignedBuffer operations
        group.bench_with_input(
            BenchmarkId::new("aligned_buffer_write", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let mut buf = AlignedBuffer::new(size, 64);
                    let slice = buf.as_mut_slice();
                    for i in 0..size.min(slice.len()) {
                        slice[i] = (i % 256) as u8;
                    }
                    black_box(buf);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_memory_copy_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_copy");
    
    let sizes = [1024, 4096, 16384, 65536, 262144];
    
    for &size in &sizes {
        let source_data = vec![0xAA; size];
        
        group.bench_with_input(
            BenchmarkId::new("memcpy_std", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let mut dest = vec![0u8; size];
                    dest.copy_from_slice(&source_data);
                    black_box(dest);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("memcpy_bytes", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let bytes = Bytes::copy_from_slice(&source_data);
                    black_box(bytes);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("memcpy_aligned", size),
            &size,
            |b, &_size| {
                b.iter(|| {
                    let mut aligned = AlignedBuffer::new(size, 64);
                    aligned.as_mut_slice().copy_from_slice(&source_data);
                    black_box(aligned);
                });
            },
        );
    }
    
    group.finish();
}

fn bench_concurrent_allocations(c: &mut Criterion) {
    c.bench_function("concurrent_allocations", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..8).map(|_| {
                std::thread::spawn(|| {
                    let pool = MemoryPool::new();
                    let mut buffers = Vec::new();
                    
                    // Allocate many buffers concurrently
                    for _ in 0..1000 {
                        let buffer = vec![0u8; 1024];
                        buffers.push(buffer);
                    }
                    
                    black_box(buffers);
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

fn bench_memory_fragmentation(c: &mut Criterion) {
    c.bench_function("memory_fragmentation", |b| {
        b.iter(|| {
            let mut buffers = Vec::new();
            
            // Allocate varying sizes to test fragmentation
            for i in 0..1000 {
                let size = if i % 2 == 0 { 64 } else { 4096 };
                let buffer = vec![0u8; size];
                buffers.push(buffer);
                
                // Deallocate some buffers randomly
                if i % 10 == 0 && !buffers.is_empty() {
                    buffers.remove(i % buffers.len());
                }
            }
            
            black_box(buffers);
        });
    });
}

criterion_group!(
    benches,
    bench_memory_pool_allocation,
    bench_aligned_buffer_creation,
    bench_buffer_operations,
    bench_memory_copy_performance,
    bench_concurrent_allocations,
    bench_memory_fragmentation
);
criterion_main!(benches);