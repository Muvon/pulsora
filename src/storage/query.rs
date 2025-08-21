//! Query execution engine for time-series data
//!
//! This module handles time-range queries, pagination, and result formatting
//! for efficient retrieval of time-series data from RocksDB.

use chrono::DateTime;
use rocksdb::{Direction, IteratorMode, ReadOptions, DB};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{PulsoraError, Result};
use crate::storage::calculate_table_hash;
use crate::storage::columnar::ColumnBlock;
use crate::storage::schema::Schema;

/// Execute ID-based query to retrieve a single row by ID
pub fn get_row_by_id(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    id: u64,
) -> Result<Option<HashMap<String, String>>> {
    // Generate primary key for direct ID lookup
    let id_key = generate_id_key(table, id);

    // Use optimized read options for faster access
    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false); // Skip checksum verification for speed
    read_opts.fill_cache(true); // Use block cache

    // Try to find the row reference
    match db.get_opt(&id_key, &read_opts)? {
        Some(ref_data) => {
            // Parse the reference data to get block ID and row index
            let (block_id, row_idx) = parse_reference_data(&ref_data)?;

            // Load the block with optimized read
            let block_data = db
                .get_opt(&block_id, &read_opts)?
                .ok_or_else(|| PulsoraError::Query("Block not found".to_string()))?;

            // Deserialize the column block
            let column_block = ColumnBlock::deserialize(&block_data)?;

            // Convert to rows and return the specific row
            let rows = column_block.to_rows(schema)?;

            if row_idx < rows.len() {
                Ok(Some(rows[row_idx].clone()))
            } else {
                Err(PulsoraError::Query("Row index out of bounds".to_string()))
            }
        }
        None => Ok(None), // Row not found
    }
}

/// Generate primary key for ID-based lookup: [table_hash:u32][id:u64]
fn generate_id_key(table: &str, id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(12); // 4 + 8 bytes

    // Table hash (4 bytes) - big-endian for correct ordering
    let table_hash = calculate_table_hash(table);
    key.extend_from_slice(&table_hash.to_be_bytes());

    // ID (8 bytes) - big-endian for correct ordering
    key.extend_from_slice(&id.to_be_bytes());

    key
}

/// Parse reference data to extract block ID and row index
fn parse_reference_data(ref_data: &[u8]) -> Result<(Vec<u8>, usize)> {
    if ref_data.is_empty() || ref_data[0] != 0xFF {
        return Err(PulsoraError::Query("Invalid reference data".to_string()));
    }

    let mut pos = 1;

    // Read block ID length
    if ref_data.len() < pos + 4 {
        return Err(PulsoraError::Query(
            "Invalid reference data length".to_string(),
        ));
    }
    let block_id_len = u32::from_le_bytes([
        ref_data[pos],
        ref_data[pos + 1],
        ref_data[pos + 2],
        ref_data[pos + 3],
    ]) as usize;
    pos += 4;

    // Read block ID
    if ref_data.len() < pos + block_id_len {
        return Err(PulsoraError::Query("Invalid block ID length".to_string()));
    }
    let block_id = ref_data[pos..pos + block_id_len].to_vec();
    pos += block_id_len;

    // Read row index
    if ref_data.len() < pos + 4 {
        return Err(PulsoraError::Query("Invalid row index data".to_string()));
    }
    let row_idx = u32::from_le_bytes([
        ref_data[pos],
        ref_data[pos + 1],
        ref_data[pos + 2],
        ref_data[pos + 3],
    ]) as usize;

    Ok((block_id, row_idx))
}

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

    let limit = limit.unwrap_or(1000).min(10000);
    let offset = offset.unwrap_or(0);

    // Optimized read options for faster sequential scans
    let mut iter_opts = ReadOptions::default();
    iter_opts.set_verify_checksums(false);
    iter_opts.fill_cache(true);
    iter_opts.set_readahead_size(2 * 1024 * 1024);

    // Structure to hold block data and row indices
    struct BlockRequest {
        block_id: Vec<u8>,
        row_indices: Vec<usize>,
    }

    let mut blocks_to_fetch: HashMap<Vec<u8>, BlockRequest> = HashMap::new();
    let mut request_order: Vec<(Vec<u8>, usize)> = Vec::with_capacity(limit);

    let table_hash = calculate_table_hash(table);
    let iter = db.iterator_opt(
        IteratorMode::From(&start_key, Direction::Forward),
        iter_opts,
    );

    // First pass: collect row references
    let mut count = 0;
    let mut skipped = 0;

    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };

        // Fast table hash check
        if key.len() < 4 {
            continue;
        }
        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }

        // Check end boundary
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

        // Parse reference data inline for speed
        if !value.is_empty() && value[0] == 0xFF {
            if value.len() < 9 {
                continue;
            }

            let block_id_len =
                u32::from_le_bytes([value[1], value[2], value[3], value[4]]) as usize;
            if value.len() < 9 + block_id_len {
                continue;
            }

            let block_id = value[5..5 + block_id_len].to_vec();
            let row_idx = u32::from_le_bytes([
                value[5 + block_id_len],
                value[6 + block_id_len],
                value[7 + block_id_len],
                value[8 + block_id_len],
            ]) as usize;

            // Track request order and group by block
            request_order.push((block_id.clone(), row_idx));

            blocks_to_fetch
                .entry(block_id.clone())
                .or_insert_with(|| BlockRequest {
                    block_id: block_id.clone(),
                    row_indices: Vec::new(),
                })
                .row_indices
                .push(row_idx);

            count += 1;
        }
    }

    // Early return if no results
    if request_order.is_empty() {
        return Ok(Vec::new());
    }

    // Create read options for fetching blocks
    let mut fetch_opts = ReadOptions::default();
    fetch_opts.set_verify_checksums(false);
    fetch_opts.fill_cache(true);

    // Second pass: fetch and process blocks directly to JSON
    let mut block_json_cache: HashMap<Vec<u8>, Vec<Value>> = HashMap::new();

    // Determine if we should use parallel processing
    let use_parallel = blocks_to_fetch.len() > 2 && cfg!(feature = "parallel");

    if use_parallel {
        #[cfg(feature = "parallel")]
        {
            use rayon::prelude::*;

            let json_blocks: Vec<_> = blocks_to_fetch
                .values()
                .par_bridge()
                .filter_map(|req| {
                    // Create read options per thread
                    let mut thread_opts = ReadOptions::default();
                    thread_opts.set_verify_checksums(false);
                    thread_opts.fill_cache(true);

                    match db.get_opt(&req.block_id, &thread_opts) {
                        Ok(Some(block_data)) => {
                            match ColumnBlock::deserialize(&block_data) {
                                Ok(block) => {
                                    // Use the new optimized direct-to-JSON method
                                    match block.to_json_values(schema) {
                                        Ok(json_values) => {
                                            Some((req.block_id.clone(), json_values))
                                        }
                                        Err(_) => None,
                                    }
                                }
                                Err(_) => None,
                            }
                        }
                        _ => None,
                    }
                })
                .collect();

            for (block_id, json_values) in json_blocks {
                block_json_cache.insert(block_id, json_values);
            }
        }
    } else {
        // Sequential processing for small queries
        for req in blocks_to_fetch.values() {
            if let Ok(Some(block_data)) = db.get_opt(&req.block_id, &fetch_opts) {
                if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                    // Use the new optimized direct-to-JSON method
                    if let Ok(json_values) = block.to_json_values(schema) {
                        block_json_cache.insert(req.block_id.clone(), json_values);
                    }
                }
            }
        }
    }

    // Third pass: extract results in original order
    let mut results = Vec::with_capacity(request_order.len());

    for (block_id, row_idx) in request_order {
        if let Some(json_values) = block_json_cache.get(&block_id) {
            if row_idx < json_values.len() {
                results.push(json_values[row_idx].clone());
            }
        }
    }

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

// Keep the original function name for compatibility
pub fn convert_row_to_json(row: &HashMap<String, String>, schema: &Schema) -> Result<Value> {
    let mut json_obj = serde_json::Map::with_capacity(row.len());

    for (key, value) in row {
        // Linear search is fine for small schemas, binary search doesn't help much
        let column = schema.columns.iter().find(|c| c.name == *key);

        if let Some(column) = column {
            let json_value = match column.data_type {
                crate::storage::schema::DataType::Id => {
                    // Use fast parsing from encoding module
                    if let Some(id) = crate::storage::encoding::fast_parse_u64(value) {
                        Value::Number(serde_json::Number::from(id))
                    } else {
                        Value::String(value.clone())
                    }
                }
                crate::storage::schema::DataType::Integer => {
                    // Use fast parsing from encoding module
                    if let Some(i) = crate::storage::encoding::fast_parse_i64(value) {
                        Value::Number(serde_json::Number::from(i))
                    } else {
                        Value::String(value.clone())
                    }
                }
                crate::storage::schema::DataType::Float => match value.parse::<f64>() {
                    Ok(f) => Value::Number(
                        serde_json::Number::from_f64(f)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                    Err(_) => Value::String(value.clone()),
                },
                crate::storage::schema::DataType::Boolean => match value.as_bytes() {
                    b"true" => Value::Bool(true),
                    b"false" => Value::Bool(false),
                    _ => Value::String(value.clone()),
                },
                crate::storage::schema::DataType::Timestamp => {
                    if let Some(millis) = crate::storage::encoding::fast_parse_i64(value) {
                        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(millis) {
                            Value::String(datetime.to_rfc3339())
                        } else {
                            Value::String(value.clone())
                        }
                    } else {
                        Value::String(value.clone())
                    }
                }
                crate::storage::schema::DataType::String => Value::String(value.clone()),
            };
            json_obj.insert(key.clone(), json_value);
        } else {
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
