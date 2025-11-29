// Copyright 2025 Muvon Un Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use std::time::Instant;
use tempfile::TempDir;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use prost::Message;
use pulsora::storage::ingestion::{ProtoBatch, ProtoRow};
use std::collections::HashMap;
use std::sync::Arc;
#[tokio::main]
async fn main() {
    // Parse args
    let args: Vec<String> = std::env::args().collect();
    let mut format = "csv".to_string();
    let mut threads = 0;
    for i in 0..args.len() {
        if args[i] == "--threads" && i + 1 < args.len() {
            threads = args[i + 1].parse().unwrap_or(0);
        }
        if args[i] == "--format" && i + 1 < args.len() {
            format = args[i + 1].clone();
        }
    }

    // Setup
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    // Increase buffer size to avoid too many flushes during test
    config.storage.buffer_size = 100_000;
    config.storage.flush_interval_ms = 0; // Disable time-based flush for consistent block sizes
                                          // Set BOTH ingestion and query threads from --threads parameter
    config.ingestion.ingestion_threads = threads;
    config.query.query_threads = threads;

    #[cfg(debug_assertions)]
    {
        println!("⚠️  WARNING: Running in DEBUG mode. Performance will be 10-100x slower.");
        println!(
            "⚠️  Use '--release' flag: cargo run --example throughput --release -- --threads 8"
        );
        println!("--------------------------------");
    }
    let storage = StorageEngine::new(&config).await.unwrap();

    let total_rows = 1_000_000;
    let batch_size = 50_000;
    let table_name = "throughput_test";

    println!("🚀 Starting Throughput Test");
    println!("Rows: {}", total_rows);
    println!("Batch Size: {}", batch_size);
    println!("Format: {}", format);
    println!(
        "Threads: {}",
        if threads == 0 {
            "Auto".to_string()
        } else {
            threads.to_string()
        }
    );
    println!("--------------------------------");

    // --- INGESTION ---
    let start_global = Instant::now();

    for i in 0..(total_rows / batch_size) {
        match format.as_str() {
            "arrow" => {
                let arrow_data = generate_arrow_batch(batch_size, i * batch_size);
                storage.ingest_arrow(table_name, arrow_data).await.unwrap();
            }
            "protobuf" => {
                let proto_data = generate_protobuf_batch(batch_size, i * batch_size);
                storage
                    .ingest_protobuf(table_name, proto_data)
                    .await
                    .unwrap();
            }
            _ => {
                let csv_data = generate_csv_batch(batch_size, i * batch_size);
                storage.ingest_csv(table_name, csv_data).await.unwrap();
            }
        }

        if (i + 1) % 10 == 0 {
            print!(".");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }
    println!();

    // Flush table to ensure all data is persisted
    storage.flush_table(table_name).await.unwrap();
    let total_time = start_global.elapsed().as_secs_f64();
    let rps = total_rows as f64 / total_time;

    println!("✅ Ingestion Complete");
    println!("Total Time: {:.2}s", total_time);
    println!("Ingestion RPS: {:.2} rows/sec", rps);
    println!("--------------------------------");

    // --- QUERY ---
    println!("🔍 Starting Query Test (Full Scan)");
    let start_query = Instant::now();

    // Query all rows
    let results = storage
        .query(
            table_name,
            None,
            None,
            Some(total_rows), // Limit
            None,
        )
        .await
        .unwrap();

    let query_time = start_query.elapsed().as_secs_f64();
    let query_rps = results.len() as f64 / query_time;

    println!("✅ Query Complete");
    println!("Rows Retrieved: {}", results.len());
    println!("Query Time: {:.2}s", query_time);
    println!("Query RPS: {:.2} rows/sec", query_rps);
    println!("--------------------------------");
    // --- FILTERED QUERY ---
    println!("🔍 Starting Filtered Query Test (Last 10%)");
    let start_filtered = Instant::now();

    // Query last 10% of rows
    // Data starts at 2024-01-01 00:00:00
    // Total 1M seconds = ~11.5 days
    // Let's query the last day
    let results_filtered = storage
        .query(
            table_name,
            Some("2024-01-10 00:00:00".to_string()),
            None,
            Some(total_rows),
            None,
        )
        .await
        .unwrap();

    let filtered_time = start_filtered.elapsed().as_secs_f64();
    let filtered_rps = results_filtered.len() as f64 / filtered_time;

    println!("✅ Filtered Query Complete");
    println!("Rows Retrieved: {}", results_filtered.len());
    println!("Query Time: {:.2}s", filtered_time);
    println!("Query RPS: {:.2} rows/sec", filtered_rps);
    println!("--------------------------------");
}

fn generate_csv_batch(rows: usize, offset: usize) -> String {
    let mut csv = String::with_capacity(rows * 100);
    csv.push_str("timestamp,symbol,price,volume\n");

    for i in 0..rows {
        let id = offset + i;
        let total_seconds = id;
        let day = 1 + (total_seconds / 86400);
        let hours = (total_seconds / 3600) % 24;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        csv.push_str(&format!(
            "2024-01-{:02} {:02}:{:02}:{:02},AAPL,{:.2},{}\n",
            day,
            hours,
            minutes,
            seconds,
            150.0 + (id as f64 * 0.01),
            1000 + id
        ));
    }
    csv
}
fn generate_arrow_batch(rows: usize, offset: usize) -> Vec<u8> {
    let timestamp_array = StringArray::from(
        (0..rows)
            .map(|i| {
                let id = offset + i;
                let total_seconds = id;
                let hours = (10 + (total_seconds / 3600)) % 24;
                let minutes = (total_seconds % 3600) / 60;
                let seconds = total_seconds % 60;
                format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds)
            })
            .collect::<Vec<String>>(),
    );

    let symbol_array = StringArray::from(vec!["AAPL"; rows]);

    let price_array = Float64Array::from(
        (0..rows)
            .map(|i| 150.0 + ((offset + i) as f64 * 0.01))
            .collect::<Vec<f64>>(),
    );

    let volume_array = Int64Array::from(
        (0..rows)
            .map(|i| (1000 + offset + i) as i64)
            .collect::<Vec<i64>>(),
    );

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timestamp_array),
            Arc::new(symbol_array),
            Arc::new(price_array),
            Arc::new(volume_array),
        ],
    )
    .unwrap();

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

fn generate_protobuf_batch(rows: usize, offset: usize) -> Vec<u8> {
    let mut proto_rows = Vec::with_capacity(rows);

    for i in 0..rows {
        let id = offset + i;
        let total_seconds = id;
        let hours = (10 + (total_seconds / 3600)) % 24;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;

        let mut values = HashMap::new();
        values.insert(
            "timestamp".to_string(),
            format!("2024-01-01 {:02}:{:02}:{:02}", hours, minutes, seconds),
        );
        values.insert("symbol".to_string(), "AAPL".to_string());
        values.insert(
            "price".to_string(),
            format!("{:.2}", 150.0 + (id as f64 * 0.01)),
        );
        values.insert("volume".to_string(), (1000 + id).to_string());

        proto_rows.push(ProtoRow { values });
    }

    let batch = ProtoBatch { rows: proto_rows };
    batch.encode_to_vec()
}
