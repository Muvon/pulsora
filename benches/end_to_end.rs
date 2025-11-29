use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulsora::config::Config;
use pulsora::server::start;
use std::time::Duration;
use tempfile::TempDir;
// use tokio::time::timeout;

fn generate_market_csv(rows: usize, symbols: &[&str]) -> String {
    let mut csv = String::from("timestamp,symbol,price,volume\n");

    for i in 0..rows {
        // Generate proper timestamp format with valid hours (0-23)
        let total_seconds = i;
        let hours = (10 + (total_seconds / 3600)) % 24; // Keep hours in 0-23 range
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let timestamp = format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds);

        let symbol = symbols[i % symbols.len()];
        let base_price = match symbol {
            "AAPL" => 150.0,
            "GOOGL" => 2800.0,
            "MSFT" => 300.0,
            "TSLA" => 200.0,
            _ => 100.0,
        };

        csv.push_str(&format!(
            "{},{},{:.2},{}\n",
            timestamp,
            symbol,
            base_price + (i as f64 * 0.01),
            1000 + (i * 10)
        ));
    }
    csv
}

async fn _create_test_server() -> (String, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.server.port = 0; // Let OS assign port

    // Start server in background
    let _server_handle = tokio::spawn(async move {
        start(config).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let base_url = "http://127.0.0.1:8080".to_string(); // Use default port for now
    (base_url, temp_dir)
}

fn bench_http_ingestion(c: &mut Criterion) {
    let mut group = c.benchmark_group("http_ingestion");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for size in [100, 500].iter() {
        // Reduced sizes for faster testing
        let csv_data = generate_market_csv(*size, &["AAPL"]);
        group.throughput(Throughput::Elements(*size as u64));

        // Create one storage engine for this benchmark size
        let (storage, _temp_dir) = rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

            let storage = pulsora::storage::StorageEngine::new(&config).await.unwrap();
            (storage, temp_dir)
        });

        group.bench_with_input(
            BenchmarkId::new("http_post_csv", size),
            &csv_data,
            |b, csv| {
                b.iter(|| {
                    rt.block_on(async {
                        // Use a fixed table name to avoid opening too many files
                        let table_name = "benchmark_table";
                        black_box(
                            storage
                                .ingest_csv(table_name, black_box(csv.clone()))
                                .await
                                .unwrap(),
                        )
                    })
                })
            },
        );
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Simulate mixed read/write workload
    for ratio in [(90, 10), (70, 30), (50, 50)].iter() {
        // (read%, write%)
        let (read_pct, write_pct) = *ratio;

        // Create one storage engine for this benchmark ratio
        let (storage, _temp_dir) = rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

            let storage = pulsora::storage::StorageEngine::new(&config).await.unwrap();

            // Pre-populate with some data
            let initial_data = generate_market_csv(1000, &["AAPL"]);
            storage
                .ingest_csv("benchmark_table", initial_data)
                .await
                .unwrap();

            (storage, temp_dir)
        });

        group.bench_with_input(
            BenchmarkId::new("mixed_workload", format!("{}r_{}w", read_pct, write_pct)),
            &(read_pct, write_pct),
            |b, (read_pct, _write_pct)| {
                b.iter(|| {
                    rt.block_on(async {
                        let operations = 100;

                        for i in 0..operations {
                            if (i * 100 / operations) < *read_pct {
                                // Read operation
                                black_box(
                                    storage
                                        .query(
                                            "benchmark_table",
                                            None,
                                            None,
                                            Some(10),
                                            Some(i * 10),
                                        )
                                        .await
                                        .unwrap(),
                                );
                            } else {
                                // Write operation - use limited set of tables to avoid file limit
                                let table_name = format!("benchmark_table_write_{}", i % 10);
                                let small_csv = generate_market_csv(10, &["AAPL"]);
                                black_box(
                                    storage.ingest_csv(&table_name, small_csv).await.unwrap(),
                                );
                            }
                        }
                    })
                })
            },
        );
    }
    group.finish();
}

fn bench_table_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_scaling");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test performance with multiple tables
    for table_count in [1, 5, 10].iter() {
        // Reduced from 20 to avoid too many tables
        // Create one storage engine for this benchmark
        let (storage, _temp_dir) = rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

            let storage = pulsora::storage::StorageEngine::new(&config).await.unwrap();
            (storage, temp_dir)
        });

        group.bench_with_input(
            BenchmarkId::new("multi_table_operations", table_count),
            table_count,
            |b, table_count| {
                b.iter(|| {
                    rt.block_on(async {
                        // Create and populate multiple tables
                        for i in 0..*table_count {
                            let table_name = format!("table_{}", i);
                            let csv_data = generate_market_csv(100, &["AAPL"]);

                            black_box(storage.ingest_csv(&table_name, csv_data).await.unwrap());
                        }

                        // Query from all tables
                        for i in 0..*table_count {
                            let table_name = format!("table_{}", i);
                            black_box(
                                storage
                                    .query(&table_name, None, None, Some(10), None)
                                    .await
                                    .unwrap(),
                            );
                        }
                    })
                })
            },
        );
    }
    group.finish();
}

fn bench_data_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_types");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test different data type combinations
    let test_cases = vec![
        ("simple", "timestamp,value\n2024-01-01 10:00:00,100\n"),
        ("mixed_types", "timestamp,symbol,price,volume,active\n2024-01-01 10:00:00,AAPL,150.25,1000,true\n"),
        ("string_heavy", "timestamp,description,category,notes\n2024-01-01 10:00:00,Long description text,Category A,Additional notes here\n"),
        ("numeric_heavy", "timestamp,price,volume,high,low,open,close\n2024-01-01 10:00:00,150.25,1000,151.0,149.5,150.0,150.25\n"),
    ];

    for (name, csv_template) in test_cases {
        // Create one storage engine for this test case
        let (storage, _temp_dir) = rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

            let storage = pulsora::storage::StorageEngine::new(&config).await.unwrap();
            (storage, temp_dir)
        });

        group.bench_with_input(
            BenchmarkId::new("schema_inference", name),
            &csv_template,
            |b, template| {
                b.iter(|| {
                    rt.block_on(async {
                        // Generate larger dataset with same schema
                        let mut csv_data = template.lines().next().unwrap().to_string() + "\n";
                        let data_line_template = template.lines().nth(1).unwrap();
                        for i in 0..1000 {
                            // Replace timestamp with proper format
                            let total_seconds = i;
                            let hours = (10 + (total_seconds / 3600)) % 24; // Keep hours in 0-23 range
                            let minutes = (total_seconds % 3600) / 60;
                            let seconds = total_seconds % 60;
                            let timestamp =
                                format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds);

                            let data_line =
                                data_line_template.replace("2024-01-01 10:00:00", &timestamp);
                            csv_data.push_str(&data_line);
                            csv_data.push('\n');
                        }

                        // Use fixed table name per test case
                        let table_name = format!("benchmark_table_{}", name);
                        black_box(storage.ingest_csv(&table_name, csv_data).await.unwrap())
                    })
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_http_ingestion,
    bench_mixed_workload,
    bench_table_scaling,
    bench_data_types
);
criterion_main!(benches);
