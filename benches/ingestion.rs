use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pulsora::config::Config;
use pulsora::storage::id_manager::IdManagerRegistry;
use pulsora::storage::{ingestion, StorageEngine};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = rt.block_on(async { StorageEngine::new(&config).await.unwrap() });

    (storage, temp_dir)
}

fn generate_csv_data(rows: usize) -> String {
    let mut csv = String::from("timestamp,symbol,price,volume\n");
    for i in 0..rows {
        let total_seconds = i;
        let hours = (10 + (total_seconds / 3600)) % 24; // Keep hours in 0-23 range
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        csv.push_str(&format!(
            "2024-01-01 {:02}:{:02}:{:02},AAPL,{:.2},{}\n",
            hours,
            minutes,
            seconds,
            150.0 + (i as f64 * 0.01),
            1000 + i
        ));
    }
    csv
}

fn generate_market_rows(count: usize) -> Vec<HashMap<String, String>> {
    (0..count)
        .map(|i| {
            let mut row = HashMap::new();
            let total_seconds = i;
            let hours = (10 + (total_seconds / 3600)) % 24; // Keep hours in 0-23 range
            let minutes = (total_seconds % 3600) / 60;
            let seconds = total_seconds % 60;
            row.insert(
                "timestamp".to_string(),
                format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds),
            );
            row.insert("symbol".to_string(), "AAPL".to_string());
            row.insert(
                "price".to_string(),
                format!("{:.2}", 150.0 + (i as f64 * 0.01)),
            );
            row.insert("volume".to_string(), (1000 + i).to_string());
            row
        })
        .collect()
}

fn bench_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");

    for size in [100, 1000, 10000].iter() {
        let csv_data = generate_csv_data(*size);
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("parse_csv", size), &csv_data, |b, csv| {
            b.iter(|| black_box(ingestion::parse_csv(black_box(csv)).unwrap()))
        });
    }
    group.finish();
}

fn bench_data_ingestion(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_ingestion");

    for size in [100, 1000, 10000].iter() {
        let (storage, _temp_dir) = create_test_storage();
        let rows = generate_market_rows(*size);

        // Create schema first
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = rt.block_on(async {
            let mut schemas = storage.schemas.write().await;
            schemas
                .get_or_create_schema("benchmark_table", &rows[0..1])
                .unwrap()
        });

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("insert_rows", size),
            &(*size, &storage, &schema, &rows),
            |b, (_size, storage, schema, rows)| {
                b.iter(|| {
                    let mut id_managers = IdManagerRegistry::new(storage.db.clone());
                    black_box(
                        ingestion::insert_rows(
                            &storage.db,
                            "benchmark_table",
                            schema,
                            &mut id_managers,
                            black_box(rows.to_vec()),
                            1000, // batch_size
                        )
                        .unwrap(),
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_full_ingestion_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_ingestion");
    group.sample_size(10); // Reduce sample size to avoid too many file descriptors

    for size in [100, 1000, 2000].iter() {
        // Reduced max size
        let csv_data = generate_csv_data(*size);
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("end_to_end_ingestion", size),
            &csv_data,
            |b, csv| {
                b.iter_batched_ref(
                    create_test_storage,
                    |(storage, _temp_dir)| {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        black_box(rt.block_on(async {
                            storage
                                .ingest_csv("benchmark_table", black_box(csv.clone()))
                                .await
                                .unwrap()
                        }))
                    },
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }
    group.finish();
}

fn bench_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_sizes");
    group.sample_size(10); // Reduce sample size to avoid too many file descriptors
    let rows = generate_market_rows(5000); // Reduced from 10000

    for batch_size in [100, 1000, 2500].iter() {
        // Reduced sizes
        let (storage, _temp_dir) = create_test_storage();

        // Create schema first
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = rt.block_on(async {
            let mut schemas = storage.schemas.write().await;
            schemas
                .get_or_create_schema("benchmark_table", &rows[0..1])
                .unwrap()
        });

        group.throughput(Throughput::Elements(5000)); // Updated to match rows

        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            &(*batch_size, &storage, &schema, &rows),
            |b, (batch_size, storage, schema, rows)| {
                b.iter_batched(
                    || (),
                    |_| {
                        black_box({
                            let mut id_managers = IdManagerRegistry::new(storage.db.clone());
                            ingestion::insert_rows(
                                &storage.db,
                                "benchmark_table",
                                schema,
                                &mut id_managers,
                                black_box(rows.to_vec()),
                                *batch_size,
                            )
                            .unwrap()
                        })
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
    bench_csv_parsing,
    bench_data_ingestion,
    bench_full_ingestion_pipeline,
    bench_batch_sizes
);
criterion_main!(benches);
