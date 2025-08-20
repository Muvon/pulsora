//! Query execution engine for time-series data
//!
//! This module handles time-range queries, pagination, and result formatting
//! for efficient retrieval of time-series data from RocksDB.

use chrono::DateTime;
use rocksdb::{Direction, IteratorMode, DB};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{PulsoraError, Result};
use crate::storage::columnar::ColumnBlock;
use crate::storage::schema::Schema;

pub fn execute_query(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<Value>> {
    let start_key = build_range_key(table, start.as_deref(), true)?;
    let end_key = build_range_key(table, end.as_deref(), false)?;

    let limit = limit.unwrap_or(1000).min(10000); // Cap at 10k rows for safety
    let offset = offset.unwrap_or(0);

    let mut results = Vec::new();
    let mut count = 0;
    let mut skipped = 0;

    // Group row requests by block ID for batch processing
    let mut block_requests: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    let mut request_order: Vec<(Vec<u8>, usize)> = Vec::new(); // Preserve order
    let mut direct_results: Vec<Value> = Vec::new();

    // Create iterator for range scan
    let table_hash = calculate_table_hash(table);
    let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));

    // First pass: collect and group row requests by block
    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };

        // Check if key belongs to our table (first 4 bytes = table hash)
        if key.len() < 4 {
            continue;
        }
        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }

        // Check end boundary if specified
        if !end_key.is_empty() && key.as_ref() > end_key.as_slice() {
            break;
        }

        // Apply offset
        if skipped < offset {
            skipped += 1;
            continue;
        }

        // Apply limit
        if count >= limit {
            break;
        }

        // Check if this is a reference or direct data
        if !value.is_empty() && value[0] == 0xFF {
            // This is a reference to a block
            let mut pos = 1;

            // Read block ID length
            if value.len() < pos + 4 {
                tracing::warn!("Invalid reference data");
                continue;
            }
            let block_id_len =
                u32::from_le_bytes([value[pos], value[pos + 1], value[pos + 2], value[pos + 3]])
                    as usize;
            pos += 4;

            // Read block ID
            if value.len() < pos + block_id_len {
                tracing::warn!("Invalid reference data");
                continue;
            }
            let block_id = value[pos..pos + block_id_len].to_vec();
            pos += block_id_len;

            // Read row index
            if value.len() < pos + 4 {
                tracing::warn!("Invalid reference data");
                continue;
            }
            let row_idx =
                u32::from_le_bytes([value[pos], value[pos + 1], value[pos + 2], value[pos + 3]])
                    as usize;

            // Group by block ID
            block_requests
                .entry(block_id.clone())
                .or_insert_with(Vec::new)
                .push(row_idx);
            request_order.push((block_id, row_idx));
            count += 1;
        } else {
            // Try as direct column block (shouldn't happen in normal flow)
            match ColumnBlock::deserialize(&value) {
                Ok(block) => match block.to_rows(schema) {
                    Ok(rows) => {
                        for row in rows {
                            if direct_results.len() >= limit - request_order.len() {
                                break;
                            }
                            let json_row = convert_row_to_json(&row, schema)?;
                            direct_results.push(json_row);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to deserialize column block: {}", e);
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!("Failed to deserialize data: {}", e);
                    continue;
                }
            }
        }
    }

    // Second pass: batch fetch blocks and extract rows
    let mut block_cache: HashMap<Vec<u8>, Vec<HashMap<String, String>>> = HashMap::new();

    // Fetch unique blocks in batch
    for (block_id, _row_indices) in &block_requests {
        if !block_cache.contains_key(block_id) {
            match db.get(block_id) {
                Ok(Some(block_data)) => match ColumnBlock::deserialize(&block_data) {
                    Ok(block) => match block.to_rows(schema) {
                        Ok(rows) => {
                            block_cache.insert(block_id.clone(), rows);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to deserialize column block: {}", e);
                        }
                    },
                    Err(e) => {
                        tracing::warn!("Failed to deserialize column block: {}", e);
                    }
                },
                Ok(None) => {
                    tracing::warn!("Column block not found");
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch column block: {}", e);
                }
            }
        }
    }

    // Third pass: extract rows in original order
    for (block_id, row_idx) in request_order {
        if let Some(rows) = block_cache.get(&block_id) {
            if row_idx < rows.len() {
                let json_row = convert_row_to_json(&rows[row_idx], schema)?;
                results.push(json_row);
            }
        }
    }

    // Add any direct results
    results.extend(direct_results);

    Ok(results)
}

fn build_range_key(table: &str, timestamp: Option<&str>, is_start: bool) -> Result<Vec<u8>> {
    // Binary key format matching ingestion
    let ts_millis = if let Some(ts) = timestamp {
        parse_query_timestamp(ts)?
    } else if is_start {
        0i64 // Beginning of time
    } else {
        i64::MAX // End of time for upper bound
    };

    let mut key = Vec::with_capacity(4 + 8 + 8);

    // Table hash (4 bytes)
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // Timestamp (8 bytes)
    key.extend_from_slice(&ts_millis.to_be_bytes());

    // Row ID (8 bytes) - min or max for range boundaries
    if is_start {
        key.extend_from_slice(&0u64.to_be_bytes());
    } else {
        key.extend_from_slice(&u64::MAX.to_be_bytes());
    }

    Ok(key)
}

fn calculate_table_hash(table: &str) -> u32 {
    // Same FNV-1a hash as in ingestion
    let mut hash = 2166136261u32;
    for byte in table.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

fn parse_query_timestamp(timestamp: &str) -> Result<i64> {
    // Try parsing as Unix timestamp first
    if let Ok(ts) = timestamp.parse::<i64>() {
        if ts > 1_000_000_000 && ts < 4_000_000_000 {
            return Ok(ts * 1000); // Convert to milliseconds
        }
        // Already in milliseconds
        if ts > 1_000_000_000_000 {
            return Ok(ts);
        }
    }

    // Try parsing as ISO datetime
    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp) {
        return Ok(dt.timestamp_millis());
    }

    // Try common formats
    let formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"];

    for format in &formats {
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp, format) {
            return Ok(naive_dt.and_utc().timestamp_millis());
        }
    }

    Err(PulsoraError::Query(format!(
        "Invalid timestamp format: {}",
        timestamp
    )))
}

fn convert_row_to_json(row: &HashMap<String, String>, schema: &Schema) -> Result<Value> {
    let mut json_obj = serde_json::Map::new();

    for (key, value) in row {
        if let Some(column) = schema.columns.iter().find(|c| c.name == *key) {
            let json_value = match column.data_type {
                crate::storage::schema::DataType::Integer => match value.parse::<i64>() {
                    Ok(i) => Value::Number(serde_json::Number::from(i)),
                    Err(_) => Value::String(value.clone()),
                },
                crate::storage::schema::DataType::Float => match value.parse::<f64>() {
                    Ok(f) => Value::Number(
                        serde_json::Number::from_f64(f)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                    Err(_) => Value::String(value.clone()),
                },
                crate::storage::schema::DataType::Boolean => match value.to_lowercase().as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    _ => Value::String(value.clone()),
                },
                crate::storage::schema::DataType::Timestamp => {
                    // Keep as string for now, could convert to ISO format
                    Value::String(value.clone())
                }
                crate::storage::schema::DataType::String => Value::String(value.clone()),
            };
            json_obj.insert(key.clone(), json_value);
        } else {
            // Column not in schema, treat as string
            json_obj.insert(key.clone(), Value::String(value.clone()));
        }
    }

    Ok(Value::Object(json_obj))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_timestamp() {
        // Unix timestamp (seconds)
        assert!(parse_query_timestamp("1704110400").is_ok());

        // Unix timestamp (milliseconds)
        assert!(parse_query_timestamp("1704110400000").is_ok());

        // ISO format
        assert!(parse_query_timestamp("2024-01-01T10:00:00Z").is_ok());

        // Common format
        assert!(parse_query_timestamp("2024-01-01 10:00:00").is_ok());

        // Invalid
        assert!(parse_query_timestamp("invalid").is_err());
    }

    #[test]
    fn test_build_range_key() {
        let start_key = build_range_key("test_table", Some("1704110400"), true).unwrap();
        // Binary key format: [table_hash:4][timestamp:8][row_id:8] = 20 bytes
        assert_eq!(start_key.len(), 20);

        // Check that table hash is consistent
        let table_hash = calculate_table_hash("test_table");
        let key_table_hash =
            u32::from_be_bytes([start_key[0], start_key[1], start_key[2], start_key[3]]);
        assert_eq!(table_hash, key_table_hash);
    }
}
