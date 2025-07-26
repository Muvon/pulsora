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
use crate::storage::ingestion::deserialize_row;
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

    // Create iterator for range scan
    let table_prefix = format!("{}:", table);
    let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));

    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };
        // Check if key belongs to our table
        let key_str = String::from_utf8_lossy(&key);
        if !key_str.starts_with(&table_prefix) {
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

        // Deserialize and convert to JSON
        match deserialize_row(&value) {
            Ok(row) => {
                let json_row = convert_row_to_json(&row, schema)?;
                results.push(json_row);
                count += 1;
            }
            Err(e) => {
                // Log error but continue processing
                tracing::warn!("Failed to deserialize row: {}", e);
                continue;
            }
        }
    }

    Ok(results)
}

fn build_range_key(table: &str, timestamp: Option<&str>, is_start: bool) -> Result<Vec<u8>> {
    let ts_millis = if let Some(ts) = timestamp {
        parse_query_timestamp(ts)?
    } else if is_start {
        0i64 // Beginning of time
    } else {
        return Ok(vec![]); // No end key
    };

    let key = if is_start {
        format!("{}:{:020}", table, ts_millis)
    } else {
        format!("{}:{:020}~", table, ts_millis) // ~ is after all possible row IDs
    };

    Ok(key.into_bytes())
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
        let key_str = String::from_utf8(start_key).unwrap();
        assert!(key_str.starts_with("test_table:"));
    }
}
