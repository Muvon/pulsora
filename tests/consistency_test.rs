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
