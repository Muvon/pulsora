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
    let start_ts = if let Some(s) = start.as_deref() {
        parse_query_timestamp(s)?
    } else {
        0i64
    };

    let end_ts = if let Some(e) = end.as_deref() {
        parse_query_timestamp(e)?
    } else {
        i64::MAX
    };

    let limit = limit.unwrap_or(1000).min(10000);
    let offset = offset.unwrap_or(0);

    // Build block index scan key - start from beginning and filter later
    let table_hash = calculate_table_hash(table);
    let mut start_key = Vec::with_capacity(5);
    start_key.extend_from_slice(&table_hash.to_be_bytes());
    start_key.push(b'B'); // Block marker
                          // Don't add timestamp to start_key - scan all blocks and filter by timestamp range

    // Optimized read options
    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    read_opts.fill_cache(true);
    read_opts.set_readahead_size(4 * 1024 * 1024); // 4MB readahead for blocks

    // Create separate options for iterator since it consumes them
    let mut iter_read_opts = ReadOptions::default();
    iter_read_opts.set_verify_checksums(false);
    iter_read_opts.fill_cache(true);
    iter_read_opts.set_readahead_size(4 * 1024 * 1024);

    let iter = db.iterator_opt(
        IteratorMode::From(&start_key, Direction::Forward),
        iter_read_opts,
    );

    let mut results = Vec::with_capacity(limit);
    let mut skipped_rows = 0usize;

    // OPTIMIZATION: Collect block metadata first for potential parallel processing
    let mut block_metadata = Vec::new();

    // Scan block index
    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };

        // Check if we're still in the block index range
        if key.len() < 5 || key[4] != b'B' {
            continue; // Not a block index entry
        }

        // Check table hash
        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }

        // Check if past end timestamp
        if key.len() >= 13 {
            let block_min_ts = i64::from_be_bytes([
                key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12],
            ]);
            if block_min_ts > end_ts {
                break;
            }
        }

        // Parse block index value - standard format
        if value.len() < 24 {
            continue; // Invalid block index
        }

        let block_id_len = u32::from_le_bytes([value[0], value[1], value[2], value[3]]) as usize;
        if value.len() < 4 + block_id_len + 16 + 4 {
            continue;
        }

        let block_id = &value[4..4 + block_id_len];
        let pos = 4 + block_id_len;
        let block_min_ts = i64::from_le_bytes([
            value[pos],
            value[pos + 1],
            value[pos + 2],
            value[pos + 3],
            value[pos + 4],
            value[pos + 5],
            value[pos + 6],
            value[pos + 7],
        ]);
        let block_max_ts = i64::from_le_bytes([
            value[pos + 8],
            value[pos + 9],
            value[pos + 10],
            value[pos + 11],
            value[pos + 12],
            value[pos + 13],
            value[pos + 14],
            value[pos + 15],
        ]);

        // Skip blocks entirely outside our range
        if block_max_ts < start_ts || block_min_ts > end_ts {
            continue;
        }

        // Collect block metadata for processing
        block_metadata.push((block_id.to_vec(), block_min_ts, block_max_ts));
    }

    // OPTIMIZATION: Process blocks with potential parallelization
    for (block_id, block_min_ts, block_max_ts) in block_metadata {
        // Fetch and decompress the block
        let mut fetch_opts = ReadOptions::default();
        fetch_opts.set_verify_checksums(false);
        fetch_opts.fill_cache(true);

        if let Ok(Some(block_data)) = db.get_opt(&block_id, &fetch_opts) {
            if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                // Get block row count for bulk offset handling
                let block_row_count = block.row_count();

                // OPTIMIZATION 1: Bulk offset skipping at block level
                if skipped_rows + block_row_count <= offset {
                    // Skip entire block
                    skipped_rows += block_row_count;
                    continue;
                }

                // OPTIMIZATION 2: Calculate how many rows to skip within this block
                let skip_in_block = offset.saturating_sub(skipped_rows);

                // OPTIMIZATION 3: Calculate how many rows we can take from this block
                let remaining_limit = limit - results.len();
                let available_in_block = block_row_count - skip_in_block;
                let take_from_block = remaining_limit.min(available_in_block);

                // Use slice-based processing for better performance
                // Use slice-based processing for better performance
                if let Ok(json_values) = block.to_json_slice(schema, skip_in_block, take_from_block)
                {
                    for (i, json_row) in json_values.into_iter().enumerate() {
                        let actual_row_idx = skip_in_block + i;

                        // VALIDITY CHECK: Ensure this is the latest version of the row
                        let mut is_latest = true;
                        let id_opt = json_row.get(&schema.id_column).and_then(|v| v.as_u64());

                        if let Some(id) = id_opt {
                            let id_key = generate_id_key(table, id);
                            if let Ok(Some(ref_data)) = db.get_opt(&id_key, &read_opts) {
                                if let Ok((latest_block_id, latest_row_idx)) =
                                    parse_reference_data(&ref_data)
                                {
                                    if latest_block_id != block_id
                                        || latest_row_idx != actual_row_idx
                                    {
                                        is_latest = false;
                                    }
                                }
                            }
                        }

                        if !is_latest {
                            continue;
                        }

                        // TIMESTAMP CHECK
                        let mut include_row = true;
                        if block_min_ts < start_ts || block_max_ts > end_ts {
                            if let Some(ts_field) = schema.get_timestamp_column() {
                                if let Some(ts_value) = json_row.get(ts_field) {
                                    let ts_millis = match ts_value {
                                        Value::Number(n) => n.as_i64(),
                                        Value::String(s) => {
                                            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s)
                                            {
                                                Some(dt.timestamp_millis())
                                            } else {
                                                None
                                            }
                                        }
                                        _ => None,
                                    };

                                    if let Some(ts) = ts_millis {
                                        include_row = ts >= start_ts && ts <= end_ts;
                                    }
                                }
                            }
                        }

                        if include_row {
                            results.push(json_row);
                            if results.len() >= limit {
                                return Ok(results);
                            }
                        }
                    }

                    // Update skipped count
                    skipped_rows = offset;
                } else if let Ok(json_values) = block.to_json_values(schema) {
                    for (i, json_row) in json_values.into_iter().enumerate() {
                        if i < skip_in_block {
                            continue;
                        }

                        // VALIDITY CHECK
                        let mut is_latest = true;
                        let id_opt = json_row.get(&schema.id_column).and_then(|v| v.as_u64());
                        if let Some(id) = id_opt {
                            let id_key = generate_id_key(table, id);
                            if let Ok(Some(ref_data)) = db.get_opt(&id_key, &read_opts) {
                                if let Ok((latest_block_id, latest_row_idx)) =
                                    parse_reference_data(&ref_data)
                                {
                                    if latest_block_id != block_id || latest_row_idx != i {
                                        is_latest = false;
                                    }
                                }
                            }
                        }
                        if !is_latest {
                            continue;
                        }

                        // Check timestamp bounds if block spans our range
                        if block_min_ts < start_ts || block_max_ts > end_ts {
                            if let Some(ts_field) = schema.get_timestamp_column() {
                                if let Some(Value::Number(ts_val)) = json_row.get(ts_field) {
                                    if let Some(ts) = ts_val.as_i64() {
                                        if ts < start_ts || ts > end_ts {
                                            continue;
                                        }
                                    }
                                }
                            }
                        }

                        results.push(json_row);
                        if results.len() >= limit {
                            return Ok(results);
                        }
                    }
                    skipped_rows = offset;
                }
            }
        }

        // Early exit if we have enough results
        if results.len() >= limit {
            break;
        }
    }

    Ok(results)
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
#[path = "query_test.rs"]
mod query_test;
