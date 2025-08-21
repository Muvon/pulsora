//! CSV data ingestion and processing with ID-based row management
//!
//! This module handles parsing CSV data, validating against schemas,
//! and efficiently storing data in RocksDB with ID-based keys and time-series optimization.

use chrono::{DateTime, Utc};
use rocksdb::{WriteBatch, DB};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::error::{PulsoraError, Result};
use crate::storage::calculate_table_hash;
use crate::storage::columnar::ColumnBlock;
use crate::storage::id_manager::{IdManagerRegistry, RowId};
use crate::storage::schema::Schema;

pub fn parse_csv(csv_data: &str) -> Result<Vec<HashMap<String, String>>> {
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();
    let header_count = headers.len();
    let mut rows = Vec::new();

    for result in reader.records() {
        let record = result?;
        let mut row = HashMap::with_capacity(header_count); // Pre-allocate capacity

        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                row.insert(header.to_string(), field.to_string());
            }
        }

        if !row.is_empty() {
            rows.push(row);
        }
    }

    Ok(rows)
}

pub fn insert_rows(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    id_managers: &mut IdManagerRegistry,
    rows: Vec<HashMap<String, String>>,
    batch_size: usize,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Get ID manager for this table
    let id_manager = id_managers.get_or_create(table)?;

    // Process rows and assign IDs with pre-allocation
    let mut processed_rows = Vec::with_capacity(rows.len());
    for mut row in rows {
        // Handle ID assignment
        let row_id = if let Some(id_str) = row.get(&schema.id_column) {
            if id_str.trim().is_empty() {
                // Empty ID - auto assign
                RowId::Auto
            } else {
                // Parse user-provided ID
                let parsed_id = RowId::from_string(id_str)?;
                if let RowId::User(id) = parsed_id {
                    id_manager.register_user_id(id)?;
                }
                parsed_id
            }
        } else {
            // Missing ID column - auto assign
            RowId::Auto
        };

        // Resolve ID to actual value
        let actual_id = row_id.resolve(&id_manager);

        // Set the ID in the row data
        row.insert(schema.id_column.clone(), actual_id.to_string());

        // Validate the complete row
        schema.validate_row(&row)?;

        processed_rows.push((actual_id, row));
    }

    // Process rows in chunks for columnar storage
    let chunks: Vec<_> = processed_rows.chunks(batch_size).collect();
    let mut total_inserted = 0u64;

    // Process each chunk as a column block
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        // Create column block for this chunk
        let column_block = ColumnBlock::from_rows(chunk, schema)?;
        let serialized_block = column_block.serialize()?;

        let mut batch = WriteBatch::default();

        // Generate a unique block ID for this chunk
        let timestamp = Utc::now().timestamp_millis();
        let block_id = format!("_block_{}_{}_{}", table, timestamp, chunk_idx);

        // Store the compressed block ONCE with the block ID
        batch.put(block_id.as_bytes(), &serialized_block);

        // For each row, store references using dual key strategy
        for (row_idx, (id, row)) in chunk.iter().enumerate() {
            // Create reference data: [marker][block_id_len][block_id][row_idx]
            let mut ref_data = Vec::with_capacity(1 + 4 + block_id.len() + 4);
            ref_data.push(0xFF); // Reference marker
            ref_data.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
            ref_data.extend_from_slice(block_id.as_bytes());
            ref_data.extend_from_slice(&(row_idx as u32).to_le_bytes());

            // Primary key: [table_hash:u32][id:u64] - for direct ID lookups
            // For REPLACE semantics, we simply overwrite the existing key
            let id_key = generate_id_key(table, *id);
            batch.put(&id_key, &ref_data);

            // Time index key: [table_hash:u32][timestamp:i64][id:u64] - for time-range queries
            let time_key = generate_time_key(table, schema, row, *id)?;
            batch.put(&time_key, &ref_data);
        }

        db.write(batch)?;
        total_inserted += chunk.len() as u64;
        debug!("Wrote column block {} with {} rows", chunk_idx, chunk.len());
    }

    Ok(total_inserted)
}

/// Generate primary key for direct ID lookup: [table_hash:u32][id:u64]
fn generate_id_key(table: &str, id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(12); // 4 + 8 bytes

    // Table hash (4 bytes) - big-endian for correct ordering
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // ID (8 bytes) - big-endian for correct ordering
    key.extend_from_slice(&id.to_be_bytes());

    key
}

/// Generate time index key: [table_hash:u32][timestamp:i64][id:u64]
fn generate_time_key(
    table: &str,
    schema: &Schema,
    row: &HashMap<String, String>,
    id: u64,
) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(20); // 4 + 8 + 8 bytes

    // Table hash (4 bytes) - big-endian for correct ordering
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // Timestamp (8 bytes) - big-endian for correct ordering
    let timestamp = if let Some(ts_col) = schema.get_timestamp_column() {
        if let Some(ts_value) = row.get(ts_col) {
            parse_timestamp(ts_value)?
        } else {
            Utc::now().timestamp_millis()
        }
    } else {
        Utc::now().timestamp_millis()
    };
    key.extend_from_slice(&timestamp.to_be_bytes());

    // ID (8 bytes) - for uniqueness and ordering
    key.extend_from_slice(&id.to_be_bytes());

    Ok(key)
}

pub fn parse_timestamp(value: &str) -> Result<i64> {
    // Try parsing as Unix timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        // Check if it's in seconds (10 digits) or milliseconds (13 digits)
        if timestamp > 1_000_000_000 && timestamp < 10_000_000_000 {
            return Ok(timestamp * 1000); // Convert seconds to milliseconds
        } else if timestamp > 1_000_000_000_000 && timestamp < 10_000_000_000_000 {
            return Ok(timestamp); // Already in milliseconds
        }
    }

    // Try parsing as RFC3339 first (handles Z suffix)
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis());
    }

    // Try parsing as naive datetime and assume UTC
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Ok(naive_dt.and_utc().timestamp_millis());
    }

    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(naive_dt.and_utc().timestamp_millis());
    }

    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis());
    }

    Err(PulsoraError::InvalidData(format!(
        "Invalid timestamp format: {}",
        value
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use tempfile::TempDir;

    fn create_test_db() -> (Arc<DB>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(DB::open_default(temp_dir.path()).unwrap());
        (db, temp_dir)
    }

    fn create_test_schema() -> Schema {
        use crate::storage::schema::{Column, DataType};

        Schema {
            table_name: "test_table".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Id,
                    nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                Column {
                    name: "value".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                },
            ],
            column_order: vec![
                "id".to_string(),
                "name".to_string(),
                "value".to_string(),
                "timestamp".to_string(),
            ],
            timestamp_column: Some("timestamp".to_string()),
            id_column: "id".to_string(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_replace_semantics() {
        let (db, _temp) = create_test_db();
        let schema = create_test_schema();
        let mut id_managers = IdManagerRegistry::new(db.clone());

        // Insert initial row with ID 1
        let initial_rows = vec![[
            ("id".to_string(), "1".to_string()),
            ("name".to_string(), "Alice".to_string()),
            ("value".to_string(), "100".to_string()),
            ("timestamp".to_string(), "2024-01-01T00:00:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect()];

        let result = insert_rows(
            &db,
            "test_table",
            &schema,
            &mut id_managers,
            initial_rows,
            100,
        );
        if let Err(e) = &result {
            eprintln!("Error inserting initial rows: {:?}", e);
        }
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Replace row with same ID but different data
        let replacement_rows = vec![[
            ("id".to_string(), "1".to_string()),
            ("name".to_string(), "Bob".to_string()),
            ("value".to_string(), "200".to_string()),
            ("timestamp".to_string(), "2024-01-02T00:00:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect()];

        let result = insert_rows(
            &db,
            "test_table",
            &schema,
            &mut id_managers,
            replacement_rows,
            100,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Query should return the latest version (Bob, not Alice)
        use crate::storage::query::get_row_by_id;
        let row = get_row_by_id(&db, "test_table", &schema, 1).unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row.get("name").unwrap(), "Bob");
        assert_eq!(row.get("value").unwrap(), "200");
    }

    #[test]
    fn test_auto_increment_with_replace() {
        let (db, _temp) = create_test_db();
        let schema = create_test_schema();
        let mut id_managers = IdManagerRegistry::new(db.clone());

        // Insert rows with auto-increment IDs
        let rows = vec![
            [
                ("name".to_string(), "User1".to_string()),
                ("value".to_string(), "10".to_string()),
                ("timestamp".to_string(), "2024-01-01T00:00:00Z".to_string()),
            ]
            .iter()
            .cloned()
            .collect(),
            [
                ("name".to_string(), "User2".to_string()),
                ("value".to_string(), "20".to_string()),
                ("timestamp".to_string(), "2024-01-01T00:01:00Z".to_string()),
            ]
            .iter()
            .cloned()
            .collect(),
        ];

        let result = insert_rows(&db, "test_table", &schema, &mut id_managers, rows, 100);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Insert a row with known ID and verify replace works
        let replacement = vec![[
            ("id".to_string(), "999".to_string()),
            ("name".to_string(), "Known".to_string()),
            ("value".to_string(), "999".to_string()),
            ("timestamp".to_string(), "2024-01-01T00:02:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect()];

        insert_rows(
            &db,
            "test_table",
            &schema,
            &mut id_managers,
            replacement.clone(),
            100,
        )
        .unwrap();

        // Now replace it
        let replacement2 = vec![[
            ("id".to_string(), "999".to_string()),
            ("name".to_string(), "Replaced".to_string()),
            ("value".to_string(), "1000".to_string()),
            ("timestamp".to_string(), "2024-01-01T00:03:00Z".to_string()),
        ]
        .iter()
        .cloned()
        .collect()];

        insert_rows(
            &db,
            "test_table",
            &schema,
            &mut id_managers,
            replacement2,
            100,
        )
        .unwrap();

        use crate::storage::query::get_row_by_id;
        let row = get_row_by_id(&db, "test_table", &schema, 999)
            .unwrap()
            .unwrap();
        assert_eq!(row.get("name").unwrap(), "Replaced");
        assert_eq!(row.get("value").unwrap(), "1000");
    }

    #[test]
    fn test_concurrent_replace() {
        use std::sync::Arc;
        use std::thread;

        let (db, _temp) = create_test_db();
        let schema = Arc::new(create_test_schema());
        let id_managers = Arc::new(RwLock::new(IdManagerRegistry::new(db.clone())));

        // Spawn multiple threads that try to replace the same ID
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let db = db.clone();
                let schema = schema.clone();
                let id_managers = id_managers.clone();

                thread::spawn(move || {
                    let rows = vec![[
                        ("id".to_string(), "100".to_string()),
                        ("name".to_string(), format!("Thread{}", i)),
                        ("value".to_string(), i.to_string()),
                        (
                            "timestamp".to_string(),
                            format!("2024-01-01T00:00:{:02}Z", i),
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect()];

                    let mut managers = id_managers.write().unwrap();
                    insert_rows(&db, "test_table", &schema, &mut managers, rows, 100)
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        // The last write should win (append-only with latest version)
        use crate::storage::query::get_row_by_id;
        let row = get_row_by_id(&db, "test_table", &schema, 100).unwrap();
        assert!(row.is_some());
        // We can't predict which thread wins, but we should have exactly one result
    }

    #[test]
    fn test_parse_csv() {
        let csv_data = "timestamp,price,volume\n2024-01-01 10:00:00,100.5,1000\n2024-01-01 10:01:00,101.0,1500";
        let rows = parse_csv(csv_data).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("price"), Some(&"100.5".to_string()));
        assert_eq!(rows[1].get("volume"), Some(&"1500".to_string()));
    }

    #[test]
    fn test_parse_timestamp() {
        assert!(parse_timestamp("2024-01-01 10:00:00").is_ok());
        assert!(parse_timestamp("2024-01-01T10:00:00Z").is_ok());
        assert!(parse_timestamp("1704110400").is_ok()); // Unix timestamp
        assert!(parse_timestamp("invalid").is_err());
    }
}
