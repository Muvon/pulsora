//! CSV data ingestion and processing
//!
//! This module handles parsing CSV data, validating against schemas,
//! and efficiently storing data in RocksDB with time-series optimized keys.

use chrono::{DateTime, Utc};
use rocksdb::{WriteBatch, DB};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::error::{PulsoraError, Result};
use crate::storage::calculate_table_hash;
use crate::storage::columnar::ColumnBlock;
use crate::storage::schema::Schema;

pub fn parse_csv(csv_data: &str) -> Result<Vec<HashMap<String, String>>> {
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();
    let mut rows = Vec::new();

    for result in reader.records() {
        let record = result?;
        let mut row = HashMap::new();

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
    rows: Vec<HashMap<String, String>>,
    batch_size: usize,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Process rows in chunks for columnar storage
    let chunks: Vec<_> = rows.chunks(batch_size).collect();
    let mut total_inserted = 0u64;

    // Process each chunk as a column block
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        // Validate all rows in chunk
        for row in chunk.iter() {
            schema.validate_row(row)?;
        }

        // Create column block for this chunk
        let column_block = ColumnBlock::from_rows(chunk, schema)?;
        let serialized_block = column_block.serialize()?;

        let mut batch = WriteBatch::default();

        // Generate a unique block ID for this chunk
        let block_id = format!("_block_{}_{}", table, chunk_idx);

        // Store the compressed block ONCE with the block ID
        batch.put(block_id.as_bytes(), &serialized_block);

        // For each row, store just a small pointer to the block
        for (row_idx, row) in chunk.iter().enumerate() {
            let row_key = generate_key(table, schema, row)?;

            // Store a lightweight reference: [marker][block_id_len][block_id][row_idx]
            let mut ref_data = Vec::with_capacity(1 + 4 + block_id.len() + 4);
            ref_data.push(0xFF); // Reference marker
            ref_data.extend_from_slice(&(block_id.len() as u32).to_le_bytes());
            ref_data.extend_from_slice(block_id.as_bytes());
            ref_data.extend_from_slice(&(row_idx as u32).to_le_bytes());

            batch.put(&row_key, &ref_data);
        }

        db.write(batch)?;
        total_inserted += chunk.len() as u64;
        debug!("Wrote column block {} with {} rows", chunk_idx, chunk.len());
    }

    Ok(total_inserted)
}

fn generate_key(table: &str, schema: &Schema, row: &HashMap<String, String>) -> Result<Vec<u8>> {
    // Binary key format for maximum performance:
    // [table_hash:u32][timestamp:i64][row_id:u64]
    // This ensures time-ordered storage with minimal overhead

    let timestamp = if let Some(ts_col) = schema.get_timestamp_column() {
        if let Some(ts_value) = row.get(ts_col) {
            parse_timestamp(ts_value)?
        } else {
            Utc::now().timestamp_millis()
        }
    } else {
        Utc::now().timestamp_millis()
    };

    // Generate a unique row ID
    let row_id = generate_row_id();

    // Create binary key with fixed size for better performance
    let mut key = Vec::with_capacity(4 + 8 + 8); // 20 bytes total

    // Table hash (4 bytes) - using simple hash for table prefix
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // Timestamp (8 bytes) - big-endian for correct ordering
    key.extend_from_slice(&timestamp.to_be_bytes());

    // Row ID (8 bytes) - for uniqueness
    key.extend_from_slice(&row_id.to_be_bytes());

    Ok(key)
}

fn parse_timestamp(value: &str) -> Result<i64> {
    // Try parsing as Unix timestamp first
    if let Ok(timestamp) = value.parse::<i64>() {
        if timestamp > 1_000_000_000 && timestamp < 4_000_000_000 {
            return Ok(timestamp * 1000); // Convert to milliseconds
        }
    }

    // Try parsing as RFC3339 first (handles Z suffix)
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp_millis());
    }

    // Try common datetime formats
    let formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"];

    for format in &formats {
        if let Ok(dt) = DateTime::parse_from_str(value, format) {
            return Ok(dt.timestamp_millis());
        }
    }

    // Try parsing as naive datetime and assume UTC
    if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(naive_dt.and_utc().timestamp_millis());
    }

    Err(PulsoraError::InvalidData(format!(
        "Invalid timestamp format: {}",
        value
    )))
}

fn generate_row_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

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
