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
    let mut total_inserted = 0u64;
    let mut batch = WriteBatch::default();
    let mut batch_count = 0;

    for row in rows {
        // Validate row against schema
        schema.validate_row(&row)?;

        // Generate key and value
        let key = generate_key(table, schema, &row)?;
        let value = serialize_row(&row)?;

        batch.put(&key, &value);
        batch_count += 1;

        // Write batch when it reaches the configured size
        if batch_count >= batch_size {
            db.write(batch)?;
            total_inserted += batch_count as u64;
            batch = WriteBatch::default();
            batch_count = 0;
            debug!("Wrote batch of {} rows", batch_size);
        }
    }

    // Write remaining rows
    if batch_count > 0 {
        db.write(batch)?;
        total_inserted += batch_count as u64;
        debug!("Wrote final batch of {} rows", batch_count);
    }

    Ok(total_inserted)
}

fn generate_key(table: &str, schema: &Schema, row: &HashMap<String, String>) -> Result<Vec<u8>> {
    // Key format: {table}:{timestamp}:{row_id}
    // This ensures time-ordered storage for efficient range queries

    let timestamp = if let Some(ts_col) = schema.get_timestamp_column() {
        if let Some(ts_value) = row.get(ts_col) {
            parse_timestamp(ts_value)?
        } else {
            Utc::now().timestamp_millis()
        }
    } else {
        Utc::now().timestamp_millis()
    };

    // Generate a unique row ID (simple counter for MVP)
    let row_id = generate_row_id();

    let key = format!("{}:{}:{:016x}", table, timestamp, row_id);
    Ok(key.into_bytes())
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

fn serialize_row(row: &HashMap<String, String>) -> Result<Vec<u8>> {
    // Use bincode for efficient serialization
    Ok(bincode::serialize(row)?)
}

pub fn deserialize_row(data: &[u8]) -> Result<HashMap<String, String>> {
    Ok(bincode::deserialize(data)?)
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
