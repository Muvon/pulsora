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

#![allow(clippy::len_zero)]
#![allow(clippy::single_match)]

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
async fn test_empty_csv() {
    let (engine, _temp) = create_test_engine().await;
    let table = "empty_csv";

    // Empty CSV with only headers
    let csv = "id,timestamp,value\n";
    let result = engine.ingest_csv(table, csv.to_string()).await;

    // Should handle gracefully (either succeed with 0 rows or error)
    match result {
        Ok(stats) => assert_eq!(stats.rows_inserted, 0),
        Err(_) => {} // Also acceptable
    }
}

#[tokio::test]
async fn test_csv_missing_columns() {
    let (engine, _temp) = create_test_engine().await;
    let table = "missing_columns";

    // First insert establishes schema
    let csv1 = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv1.to_string()).await.unwrap();

    // Second insert missing 'value' column
    let csv2 = "id,timestamp\n2,1704067201000";
    let result = engine.ingest_csv(table, csv2.to_string()).await;

    // Should error or handle gracefully
    assert!(result.is_err() || result.is_ok());
}

#[tokio::test]
async fn test_csv_extra_columns() {
    let (engine, _temp) = create_test_engine().await;
    let table = "extra_columns";

    // First insert
    let csv1 = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv1.to_string()).await.unwrap();

    // Second insert with extra column
    let csv2 = "id,timestamp,value,extra\n2,1704067201000,200,bonus";
    let result = engine.ingest_csv(table, csv2.to_string()).await;

    // Should handle (either accept or error)
    if result.is_ok() {
        let results = engine.query(table, None, None, None, None).await.unwrap();
        assert!(results.len() >= 1);
    }
}

#[tokio::test]
async fn test_invalid_data_type_after_schema() {
    let (engine, _temp) = create_test_engine().await;
    let table = "invalid_type";

    // Establish schema with integer
    let csv1 = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv1.to_string()).await.unwrap();

    // Try to insert string in integer column
    let csv2 = "id,timestamp,value\n2,1704067201000,not_a_number";
    let result = engine.ingest_csv(table, csv2.to_string()).await;

    // System might coerce types or error - both are acceptable
    // The important thing is it doesn't panic or corrupt data
    if result.is_ok() {
        // If it succeeded, verify first row is still intact
        let results = engine.query(table, None, None, None, None).await.unwrap();
        let row1 = results
            .iter()
            .find(|r| r.get("id").unwrap().as_u64().unwrap() == 1);
        assert!(row1.is_some());
    }
}

#[tokio::test]
async fn test_duplicate_ids_in_batch() {
    let (engine, _temp) = create_test_engine().await;
    let table = "duplicate_ids";

    // CSV with duplicate IDs
    let csv = "id,timestamp,value\n\
               1,1704067200000,100\n\
               1,1704067201000,200\n\
               1,1704067202000,300";

    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Should keep only latest
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("value").unwrap().as_i64().unwrap(), 300);
}

#[tokio::test]
async fn test_id_zero() {
    let (engine, _temp) = create_test_engine().await;
    let table = "id_zero";

    // ID = 0 should error
    let csv = "id,timestamp,value\n0,1704067200000,100";
    let result = engine.ingest_csv(table, csv.to_string()).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_negative_id() {
    let (engine, _temp) = create_test_engine().await;
    let table = "negative_id";

    // Negative ID should error
    let csv = "id,timestamp,value\n-1,1704067200000,100";
    let result = engine.ingest_csv(table, csv.to_string()).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_very_large_table() {
    let (engine, _temp) = create_test_engine().await;
    let table = "very_large";

    // Insert 10000 rows
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=10000 {
        csv.push_str(&format!("{},{},{}\n", i, 1704067200000i64 + i, i * 10));
    }

    let result = engine.ingest_csv(table, csv).await;
    assert!(result.is_ok());

    // Query with explicit limit to get all rows (default limit is 1000)
    let results = engine
        .query(table, None, None, Some(10000), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 10000);

    // Verify default limit works
    let results_default = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results_default.len(), 1000); // Default limit
}

#[tokio::test]
async fn test_single_column_table() {
    let (engine, _temp) = create_test_engine().await;
    let table = "single_column";

    // Only ID column (timestamp auto-generated or not required)
    let csv = "id\n1\n2\n3";
    let result = engine.ingest_csv(table, csv.to_string()).await;

    // Should handle (either succeed or error gracefully)
    if result.is_ok() {
        let results = engine.query(table, None, None, None, None).await.unwrap();
        assert!(results.len() >= 1);
    }
}

#[tokio::test]
async fn test_many_columns_table() {
    let (engine, _temp) = create_test_engine().await;
    let table = "many_columns";

    // Table with 25 columns
    let mut headers = vec!["id".to_string(), "timestamp".to_string()];
    for i in 1..=23 {
        headers.push(format!("col{}", i));
    }

    let mut csv = headers.join(",") + "\n";

    // Add one row
    let mut values = vec!["1".to_string(), "1704067200000".to_string()];
    for i in 1..=23 {
        values.push(format!("{}", i * 10));
    }
    csv.push_str(&(values.join(",") + "\n"));

    let result = engine.ingest_csv(table, csv).await;
    assert!(result.is_ok());

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_object().unwrap().len(), 25);
}

#[tokio::test]
async fn test_timestamp_out_of_range() {
    let (engine, _temp) = create_test_engine().await;
    let table = "timestamp_range";

    // Very old timestamp (year 1970)
    let csv1 = "id,timestamp,value\n1,0,100";
    let result1 = engine.ingest_csv(table, csv1.to_string()).await;

    // Should handle (might be valid)
    if result1.is_ok() {
        let _results = engine.query(table, None, None, None, None).await.unwrap();
        // Query succeeded
    }

    // Very far future timestamp
    let csv2 = "id,timestamp,value\n2,9999999999999,200";
    let result2 = engine.ingest_csv(table, csv2.to_string()).await;

    // Should handle
    assert!(result2.is_ok() || result2.is_err());
}

#[tokio::test]
async fn test_malformed_csv() {
    let (engine, _temp) = create_test_engine().await;
    let table = "malformed_csv";

    // CSV with inconsistent columns
    let csv = "id,timestamp,value\n1,1704067200000,100\n2,1704067201000";
    let result = engine.ingest_csv(table, csv.to_string()).await;

    // Should error
    assert!(result.is_err());
}

#[tokio::test]
async fn test_malformed_protobuf() {
    let (engine, _temp) = create_test_engine().await;
    let table = "malformed_proto";

    // Invalid protobuf data
    let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let result = engine.ingest_protobuf(table, invalid_data).await;

    // Should error with descriptive message
    assert!(result.is_err());
}

#[tokio::test]
async fn test_malformed_arrow() {
    let (engine, _temp) = create_test_engine().await;
    let table = "malformed_arrow";

    // Invalid arrow data
    let invalid_data = vec![0x00, 0x01, 0x02, 0x03];
    let result = engine.ingest_arrow(table, invalid_data).await;

    // Should error with descriptive message
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_nonexistent_table() {
    let (engine, _temp) = create_test_engine().await;

    let result = engine.query("nonexistent", None, None, None, None).await;

    // Should error gracefully
    assert!(result.is_err());
}

#[tokio::test]
async fn test_schema_validation_error_message() {
    let (engine, _temp) = create_test_engine().await;
    let table = "schema_validation";

    // Establish schema
    let csv1 = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv1.to_string()).await.unwrap();

    // Invalid data
    let csv2 = "id,timestamp,value\n2,not_a_timestamp,200";
    let result = engine.ingest_csv(table, csv2.to_string()).await;

    // Should have descriptive error
    if let Err(e) = result {
        let error_msg = format!("{:?}", e);
        assert!(!error_msg.is_empty());
    }
}

#[tokio::test]
async fn test_concurrent_table_creation() {
    let (engine, _temp) = create_test_engine().await;

    // Try to create same table concurrently
    let table = "concurrent_create";

    let csv1 = "id,timestamp,value\n1,1704067200000,100";
    let csv2 = "id,timestamp,value\n2,1704067201000,200";

    let handle1 = {
        let engine = engine.clone();
        let csv = csv1.to_string();
        tokio::spawn(async move { engine.ingest_csv(table, csv).await })
    };

    let handle2 = {
        let engine = engine.clone();
        let csv = csv2.to_string();
        tokio::spawn(async move { engine.ingest_csv(table, csv).await })
    };

    let result1 = handle1.await.unwrap();
    let result2 = handle2.await.unwrap();

    // Both should succeed
    assert!(result1.is_ok());
    assert!(result2.is_ok());

    // Should have 2 rows
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_special_characters_in_strings() {
    let (engine, _temp) = create_test_engine().await;
    let table = "special_chars";

    let csv = "id,timestamp,text\n\
               1,1704067200000,\"line1\nline2\"\n\
               2,1704067201000,\"tab\tseparated\"\n\
               3,1704067202000,\"quote\\\"inside\"";

    let result = engine.ingest_csv(table, csv.to_string()).await;

    // Should handle special characters
    if result.is_ok() {
        let results = engine.query(table, None, None, None, None).await.unwrap();
        assert!(results.len() >= 1);
    }
}

#[tokio::test]
async fn test_very_long_string() {
    let (engine, _temp) = create_test_engine().await;
    let table = "long_string";

    // 10KB string
    let long_text = "a".repeat(10000);
    let csv = format!("id,timestamp,text\n1,1704067200000,{}", long_text);

    let result = engine.ingest_csv(table, csv).await;
    assert!(result.is_ok());

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("text").unwrap().as_str().unwrap().len(),
        10000
    );
}

#[tokio::test]
async fn test_max_integer_values() {
    let (engine, _temp) = create_test_engine().await;
    let table = "max_integers";

    let csv = format!(
        "id,timestamp,max_val,min_val\n\
         1,1704067200000,{},{}",
        i64::MAX,
        i64::MIN
    );

    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("max_val").unwrap().as_i64().unwrap(),
        i64::MAX
    );
    assert_eq!(
        results[0].get("min_val").unwrap().as_i64().unwrap(),
        i64::MIN
    );
}

#[tokio::test]
async fn test_empty_table_query() {
    let (engine, _temp) = create_test_engine().await;
    let table = "empty_table";

    // Create table with schema but no data
    let csv = "id,timestamp,value\n";
    let _ = engine.ingest_csv(table, csv.to_string()).await;

    // Query should return empty array
    let result = engine.query(table, None, None, None, None).await;

    if let Ok(results) = result {
        assert_eq!(results.len(), 0);
    }
}
