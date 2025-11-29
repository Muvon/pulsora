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
use tempfile::TempDir;

async fn create_test_engine() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_update_consistency_same_timestamp() {
    let (engine, _temp) = create_test_engine().await;
    let table = "consistency_test_1";

    // 1. Ingest initial row
    let csv_1 = "id,timestamp,value\n1,1704067200000,10";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // 2. Ingest update (same ID, same timestamp, new value)
    let csv_2 = "id,timestamp,value\n1,1704067200000,20";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    // 3. Query range
    let results = engine
        .query(
            table,
            Some("1704067000000".to_string()),
            Some("1704067300000".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    // 4. Verify results
    // Should have exactly 1 row
    assert_eq!(results.len(), 1, "Expected 1 row, got {}", results.len());

    // Value should be 20
    let value = results[0].get("value").unwrap().as_i64().unwrap();
    assert_eq!(value, 20, "Expected value 20, got {}", value);
}

#[tokio::test]
async fn test_update_consistency_diff_timestamp() {
    let (engine, _temp) = create_test_engine().await;
    let table = "consistency_test_2";

    // 1. Ingest initial row
    let csv_1 = "id,timestamp,value\n2,1704067200000,100";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // 2. Ingest update (same ID, different timestamp, new value)
    let csv_2 = "id,timestamp,value\n2,1704067201000,200";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    // 3. Query range covering both timestamps
    let results = engine
        .query(
            table,
            Some("1704067000000".to_string()),
            Some("1704067300000".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    // 4. Verify results
    // If ID is unique, we expect 1 row (the latest).
    assert_eq!(results.len(), 1, "Expected 1 row, got {}", results.len());

    let value = results[0].get("value").unwrap().as_i64().unwrap();
    assert_eq!(value, 200, "Expected value 200, got {}", value);

    let ts_val = results[0].get("timestamp").unwrap();
    let ts = if let Some(s) = ts_val.as_str() {
        s.to_string()
    } else if let Some(n) = ts_val.as_i64() {
        n.to_string()
    } else {
        panic!("Unexpected timestamp format");
    };

    // 2001 ms is 1970-01-01T00:00:02.001Z or just 1704067201000
    assert!(
        ts.contains("02.001") || ts.contains("1704067201000"),
        "Expected timestamp to be updated, got {}",
        ts
    );
}

#[tokio::test]
async fn test_update_all_data_types() {
    let (engine, _temp) = create_test_engine().await;
    let table = "update_all_types";

    // Initial insert with all types
    let csv_1 = "id,timestamp,int_col,float_col,str_col,bool_col\n\
                 1,1704067200000,100,1.5,initial,true";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // Update all columns
    let csv_2 = "id,timestamp,int_col,float_col,str_col,bool_col\n\
                 1,1704067201000,200,2.5,updated,false";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    let row = &results[0];
    assert_eq!(row.get("int_col").unwrap().as_i64().unwrap(), 200);
    assert!((row.get("float_col").unwrap().as_f64().unwrap() - 2.5).abs() < 0.01);
    assert_eq!(row.get("str_col").unwrap().as_str().unwrap(), "updated");
    assert!(!row.get("bool_col").unwrap().as_bool().unwrap());
}

#[tokio::test]
async fn test_multiple_updates_same_id() {
    let (engine, _temp) = create_test_engine().await;
    let table = "multiple_updates";

    // Insert initial
    let csv_1 = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // Update 1
    let csv_2 = "id,timestamp,value\n1,1704067201000,200";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    // Update 2
    let csv_3 = "id,timestamp,value\n1,1704067202000,300";
    engine.ingest_csv(table, csv_3.to_string()).await.unwrap();

    // Update 3
    let csv_4 = "id,timestamp,value\n1,1704067203000,400";
    engine.ingest_csv(table, csv_4.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should have only 1 row after multiple updates"
    );
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 400);
}

#[tokio::test]
async fn test_concurrent_updates_simulation() {
    let (engine, _temp) = create_test_engine().await;
    let table = "concurrent_updates";

    // Simulate concurrent updates by ingesting batch with same IDs
    let csv = "id,timestamp,value\n\
               1,1704067200000,100\n\
               1,1704067201000,200\n\
               1,1704067202000,300";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1, "Should deduplicate to 1 row");
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 300);
}

#[tokio::test]
async fn test_update_via_different_formats() {
    use prost::Message;
    use pulsora::storage::ingestion::{ProtoBatch, ProtoRow};
    use std::collections::HashMap;

    let (engine, _temp) = create_test_engine().await;
    let table = "cross_format_update";

    // Initial insert via CSV
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Update via Protobuf
    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("timestamp".to_string(), "1704067201000".to_string());
    values.insert("value".to_string(), "200".to_string());

    let batch = ProtoBatch {
        rows: vec![ProtoRow { values }],
    };

    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();
    engine.ingest_protobuf(table, buf).await.unwrap();

    // Query and verify
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 200);
}

#[tokio::test]
async fn test_no_duplicate_rows_after_update() {
    let (engine, _temp) = create_test_engine().await;
    let table = "no_duplicates";

    // Insert 3 unique rows
    let csv_1 = "id,timestamp,value\n\
                 1,1704067200000,100\n\
                 2,1704067201000,200\n\
                 3,1704067202000,300";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // Update row 2
    let csv_2 = "id,timestamp,value\n2,1704067203000,250";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 3, "Should still have 3 unique rows");

    // Verify row 2 was updated
    let row2 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 2)
        .unwrap();
    assert_eq!(row2.get("value").unwrap().as_i64().unwrap(), 250);

    // Verify others unchanged
    let row1 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1)
        .unwrap();
    assert_eq!(row1.get("value").unwrap().as_i64().unwrap(), 100);

    let row3 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 3)
        .unwrap();
    assert_eq!(row3.get("value").unwrap().as_i64().unwrap(), 300);
}

#[tokio::test]
async fn test_update_preserves_compression() {
    let (engine, _temp) = create_test_engine().await;
    let table = "update_compression";

    // Insert large batch to trigger compression
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=50 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Update one row
    let csv_update = "id,timestamp,value\n25,1704067250000,9999";
    engine
        .ingest_csv(table, csv_update.to_string())
        .await
        .unwrap();

    // Query and verify
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 50);

    let row25 = results
        .iter()
        .find(|r| r.get("id").unwrap().as_u64().unwrap() == 25)
        .unwrap();
    assert_eq!(row25.get("value").unwrap().as_i64().unwrap(), 9999);
}

#[tokio::test]
async fn test_update_float_precision() {
    let (engine, _temp) = create_test_engine().await;
    let table = "update_float_precision";

    // Initial value
    let csv_1 = "id,timestamp,price\n1,1704067200000,123.456789";
    engine.ingest_csv(table, csv_1.to_string()).await.unwrap();

    // Update with high precision
    let csv_2 = "id,timestamp,price\n1,1704067201000,987.654321";
    engine.ingest_csv(table, csv_2.to_string()).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    let price = results[0].get("price").unwrap().as_f64().unwrap();
    assert!((price - 987.654321).abs() < 1e-6, "Float precision lost");
}
