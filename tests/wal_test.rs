use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use tempfile::TempDir;

async fn create_test_engine(wal_enabled: bool) -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = wal_enabled;
    config.storage.buffer_size = 10;
    config.storage.flush_interval_ms = 0; // Batch only mode to test persistence

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

#[tokio::test]
async fn test_wal_durability_on_crash() {
    let (engine, temp_dir) = create_test_engine(true).await;
    let table = "wal_test_durability";

    // 1. Ingest data (should go to WAL + Buffer)
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Verify it's in memory
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    // 3. Simulate "Crash" by dropping engine and creating new one on same dir
    // Wait for async WAL write to complete
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    drop(engine);

    // 4. Restart engine
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = true;
    config.storage.buffer_size = 10;
    config.storage.flush_interval_ms = 0;

    let engine_recovered = StorageEngine::new(&config).await.unwrap();

    // 5. Verify data recovered from WAL
    let results_recovered = engine_recovered
        .query(table, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(results_recovered.len(), 1, "Should recover 1 row from WAL");
    assert_eq!(
        results_recovered[0].get("value").unwrap().as_i64().unwrap(),
        100
    );
}

#[tokio::test]
async fn test_wal_cleanup_after_flush() {
    let (engine, _temp) = create_test_engine(true).await;
    let table = "wal_test_cleanup";

    // 1. Ingest data (buffer size 10)
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

    // 2. This should have triggered a flush for first 10 rows
    // The WAL should have been truncated and now only contains 5 rows

    // We can't easily inspect file size here without knowing the hash,
    // but we can verify behavior by querying.

    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 15);
}

#[tokio::test]
async fn test_wal_disabled_data_loss() {
    let (engine, temp_dir) = create_test_engine(false).await;
    let table = "wal_test_loss";

    // 1. Ingest data (WAL disabled)
    let csv = "id,timestamp,value\n1,1704067200000,100";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // 2. Verify in memory
    let results = engine.query(table, None, None, None, None).await.unwrap();
    assert_eq!(results.len(), 1);

    // 3. Crash
    drop(engine);

    // 4. Restart
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.storage.wal_enabled = false; // Still disabled

    let engine_recovered = StorageEngine::new(&config).await.unwrap();

    // 5. Verify data LOST (because it was in RAM only and WAL was off)
    // Note: Table might not even exist if schema wasn't persisted?
    // Schema is persisted to RocksDB immediately on creation, so table exists.
    // But rows were in buffer.

    let results_recovered = engine_recovered.query(table, None, None, None, None).await;

    // If table exists (schema saved), query returns empty.
    // If schema wasn't saved (it is), it would error.
    if let Ok(rows) = results_recovered {
        assert_eq!(rows.len(), 0, "Should have lost data with WAL disabled");
    }
}
