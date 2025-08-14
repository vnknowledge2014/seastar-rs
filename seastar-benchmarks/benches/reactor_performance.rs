use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use seastar_core::{Reactor, ReactorConfig, Task, TaskPriority};
use std::time::Duration;
use tokio;

struct BenchTask {
    work_amount: usize,
    completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl BenchTask {
    fn new(work_amount: usize) -> (Self, std::sync::Arc<std::sync::atomic::AtomicBool>) {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = Self {
            work_amount,
            completed: completed.clone(),
        };
        (task, completed)
    }
}

impl Task for BenchTask {
    async fn execute(&mut self) -> seastar_core::task::TaskState {
        // Simulate work
        let mut sum = 0u64;
        for i in 0..self.work_amount {
            sum += i as u64;
        }
        black_box(sum);
        
        self.completed.store(true, std::sync::atomic::Ordering::Release);
        seastar_core::task::TaskState::Completed
    }

    fn handle(&self) -> seastar_core::TaskHandle {
        seastar_core::TaskHandle {
            id: seastar_core::task::TaskId(0),
            name: Some("bench_task".to_string()),
            priority: TaskPriority::Normal,
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }
}

fn bench_task_scheduling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("task_scheduling");
    
    for task_count in [1, 10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("schedule_tasks", task_count),
            task_count,
            |b, &task_count| {
                b.iter(|| {
                    rt.block_on(async {
                        let config = ReactorConfig::default();
                        let mut reactor = Reactor::new(config).unwrap();
                        
                        let mut completion_flags = Vec::new();
                        
                        // Schedule tasks
                        for _ in 0..task_count {
                            let (task, completed) = BenchTask::new(1000);
                            completion_flags.push(completed);
                            reactor.submit_task(Box::new(task));
                        }
                        
                        // Wait for completion
                        while !completion_flags.iter().all(|f| f.load(std::sync::atomic::Ordering::Acquire)) {
                            tokio::task::yield_now().await;
                        }
                    })
                });
            },
        );
    }
    
    group.finish();
}

fn bench_reactor_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("reactor_throughput", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = ReactorConfig {
                    max_tasks_per_iteration: 10000,
                    ..Default::default()
                };
                let mut reactor = Reactor::new(config).unwrap();
                
                let task_count = 10000;
                let mut completion_flags = Vec::with_capacity(task_count);
                
                // Schedule many small tasks
                for _ in 0..task_count {
                    let (task, completed) = BenchTask::new(10);
                    completion_flags.push(completed);
                    reactor.submit_task(Box::new(task));
                }
                
                let start = std::time::Instant::now();
                
                // Process all tasks
                while !completion_flags.iter().all(|f| f.load(std::sync::atomic::Ordering::Acquire)) {
                    tokio::task::yield_now().await;
                }
                
                let duration = start.elapsed();
                black_box(duration);
            })
        });
    });
}

fn bench_task_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("task_latency", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = ReactorConfig::default();
                let mut reactor = Reactor::new(config).unwrap();
                
                let (task, completed) = BenchTask::new(100);
                let submit_time = std::time::Instant::now();
                
                reactor.submit_task(Box::new(task));
                
                // Wait for task completion
                while !completed.load(std::sync::atomic::Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
                
                let completion_time = std::time::Instant::now();
                let latency = completion_time.duration_since(submit_time);
                black_box(latency);
            })
        });
    });
}

fn bench_reactor_overhead(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("reactor_overhead", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = ReactorConfig::default();
                let reactor = Reactor::new(config).unwrap();
                
                // Measure just reactor creation overhead
                black_box(reactor);
            })
        });
    });
}

criterion_group!(
    benches,
    bench_task_scheduling,
    bench_reactor_throughput, 
    bench_task_latency,
    bench_reactor_overhead
);
criterion_main!(benches);