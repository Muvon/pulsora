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
use std::time::Duration;
use tempfile::TempDir;

async fn create_test_engine() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.buffer_size = 10; // Small buffer for testing
    config.storage.flush_interval_ms = 500; // Fast flush

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_buffer_query_consistency() {
    let (engine, _temp) = create_test_engine().await;
    let table = "buffer_test";

    // 1. Insert single row (should go to buffer)
    let csv = "id,timestamp,value\n1,1704067200000,10";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Query immediately - should find it in buffer
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1, "Should find buffered row");
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 10);

    // 3. Wait for flush interval
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 4. Query again - should find it in RocksDB (via merged query)
    let results_after = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results_after.len(), 1, "Should find flushed row");
    assert_eq!(results_after[0].get("value").unwrap().as_i64().unwrap(), 10);
}

#[tokio::test]
async fn test_buffer_overflow_flush() {
    let (engine, _temp) = create_test_engine().await;
    let table = "overflow_test";

    // 1. Insert 15 rows (buffer size is 10)
    // This should trigger a flush of the first 10, leaving 5 in buffer
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=15 {
        csv.push_str(&format!(
            "{},{},{}\n",
            i,
            1704067200000i64 + (i as i64),
            i * 10
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // 2. Query should return all 15 (10 from DB, 5 from buffer)
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 15, "Should find all rows");
}
