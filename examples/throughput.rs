use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use std::time::Instant;
use tempfile::TempDir;

#[tokio::main]
async fn main() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    // Increase buffer size to avoid too many flushes during test
    config.storage.buffer_size = 100_000; 
    
    let storage = StorageEngine::new(&config).await.unwrap();
    
    let total_rows = 500_000;
    let batch_size = 10_000;
    let table_name = "throughput_test";
    
    println!("🚀 Starting Throughput Test");
    println!("Rows: {}", total_rows);
    println!("Batch Size: {}", batch_size);
    println!("--------------------------------");

    // --- INGESTION ---
    let start_global = Instant::now();
    
    for i in 0..(total_rows / batch_size) {
        let csv_data = generate_csv_batch(batch_size, i * batch_size);
        storage.ingest_csv(table_name, csv_data).await.unwrap();
        
        if (i + 1) % 10 == 0 {
            print!(".");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }
    println!();
    
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
    let results = storage.query(
        table_name,
        None,
        None,
        Some(total_rows), // Limit
        None
    ).await.unwrap();
    
    let query_time = start_query.elapsed().as_secs_f64();
    let query_rps = results.len() as f64 / query_time;
    
    println!("✅ Query Complete");
    println!("Rows Retrieved: {}", results.len());
    println!("Query Time: {:.2}s", query_time);
    println!("Query RPS: {:.2} rows/sec", query_rps);
    println!("--------------------------------");
}

fn generate_csv_batch(rows: usize, offset: usize) -> String {
    let mut csv = String::with_capacity(rows * 100);
    csv.push_str("timestamp,symbol,price,volume\n");
    
    for i in 0..rows {
        let id = offset + i;
        let total_seconds = id;
        let hours = (10 + (total_seconds / 3600)) % 24;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        
        csv.push_str(&format!(
            "2024-01-01 {:02}:{:02}:{:02},AAPL,{:.2},{}\n",
            hours, minutes, seconds,
            150.0 + (id as f64 * 0.01),
            1000 + id
        ));
    }
    csv
}
