use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulsora::config::Config;
use pulsora::storage::{query, StorageEngine};
// use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_storage_with_data(rows: usize) -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = rt.block_on(async {
        let storage = StorageEngine::new(&config).await.unwrap();

        // Generate and insert test data
        let csv_data = generate_csv_data(rows);
        storage
            .ingest_csv("benchmark_table", csv_data)
            .await
            .unwrap();

        storage
    });

    (storage, temp_dir)
}

fn generate_csv_data(rows: usize) -> String {
    let mut csv = String::from("timestamp,symbol,price,volume\n");

    for i in 0..rows {
        // Generate proper timestamp format with valid hours (0-23)
        let total_seconds = i;
        let hours = (10 + (total_seconds / 3600)) % 24; // Keep hours in 0-23 range
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let timestamp = format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds);

        csv.push_str(&format!(
            "{},AAPL,{:.2},{}\n",
            timestamp,
            150.0 + (i as f64 * 0.01),
            1000 + i
        ));
    }
    csv
}

fn bench_time_range_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_range_queries");

    // Test with different dataset sizes
    for dataset_size in [1000, 10000, 100000].iter() {
        let (storage, _temp_dir) = create_test_storage_with_data(*dataset_size);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = rt.block_on(async {
            let schemas = storage.schemas.read().await;
            schemas.get_schema("benchmark_table").unwrap().clone()
        });

        group.throughput(Throughput::Elements(*dataset_size as u64));

        // Full table scan
        group.bench_with_input(
            BenchmarkId::new("full_scan", dataset_size),
            &(&storage, &schema),
            |b, (storage, schema)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            None,
                            None,
                            Some(1000),
                            None,
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // Time range query (first 10% of data)
        group.bench_with_input(
            BenchmarkId::new("time_range_10pct", dataset_size),
            &(&storage, &schema),
            |b, (storage, schema)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            Some("2024-01-01 10:00:00".to_string()),
                            Some("2024-01-01 10:01:40".to_string()), // ~10% of 1000 seconds
                            Some(1000),
                            None,
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // Recent data query (last 1% of data)
        group.bench_with_input(
            BenchmarkId::new("recent_data_1pct", dataset_size),
            &(&storage, &schema),
            |b, (storage, schema)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            Some("2024-01-01 10:16:30".to_string()), // Last ~1% for 1000 rows
                            None,
                            Some(1000),
                            None,
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_pagination(c: &mut Criterion) {
    let mut group = c.benchmark_group("pagination");
    let (storage, _temp_dir) = create_test_storage_with_data(10000);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = rt.block_on(async {
        let schemas = storage.schemas.read().await;
        schemas.get_schema("benchmark_table").unwrap().clone()
    });

    for limit in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*limit as u64));

        // First page
        group.bench_with_input(
            BenchmarkId::new("first_page", limit),
            &(&storage, &schema, *limit),
            |b, (storage, schema, limit)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            None,
                            None,
                            Some(*limit),
                            Some(0),
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // Middle page
        group.bench_with_input(
            BenchmarkId::new("middle_page", limit),
            &(&storage, &schema, *limit),
            |b, (storage, schema, limit)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            None,
                            None,
                            Some(*limit),
                            Some(5000), // Middle of 10k dataset
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_query_result_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("result_sizes");
    let (storage, _temp_dir) = create_test_storage_with_data(50000);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = rt.block_on(async {
        let schemas = storage.schemas.read().await;
        schemas.get_schema("benchmark_table").unwrap().clone()
    });

    for result_size in [10, 100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*result_size as u64));

        group.bench_with_input(
            BenchmarkId::new("query_with_limit", result_size),
            &(&storage, &schema, *result_size),
            |b, (storage, schema, result_size)| {
                b.iter(|| {
                    black_box(
                        query::execute_query(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            None,
                            None,
                            Some(*result_size),
                            None,
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_concurrent_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_queries");
    let (storage, _temp_dir) = create_test_storage_with_data(10000);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = rt.block_on(async {
        let schemas = storage.schemas.read().await;
        schemas.get_schema("benchmark_table").unwrap().clone()
    });

    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_reads", thread_count),
            &(&storage, &schema, *thread_count),
            |b, (_storage, _schema, thread_count)| {
                b.iter_batched(
                    || {
                        let (storage, _temp_dir) = create_test_storage_with_data(1000);
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        let schema = rt.block_on(async {
                            let schemas = storage.schemas.read().await;
                            schemas.get_schema("benchmark_table").unwrap().clone()
                        });
                        (storage, schema)
                    },
                    |(storage, schema)| {
                        let handles: Vec<_> = (0..*thread_count)
                            .map(|_| {
                                let storage = storage.clone();
                                let schema = schema.clone();
                                std::thread::spawn(move || {
                                    query::execute_query(
                                        &storage.db,
                                        "benchmark_table",
                                        &schema,
                                        None,
                                        None,
                                        Some(100),
                                        None,
                                    )
                                    .unwrap()
                                })
                            })
                            .collect();

                        for handle in handles {
                            black_box(handle.join().unwrap());
                        }
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_time_range_queries,
    bench_pagination,
    bench_query_result_sizes,
    bench_concurrent_queries
);
criterion_main!(benches);
