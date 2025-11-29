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

//! Query execution engine for time-series data
//!
//! This module handles time-range queries, pagination, and result formatting
//! for efficient retrieval of time-series data from RocksDB.

use arrow::record_batch::RecordBatch;
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

#[allow(clippy::too_many_arguments)]
pub fn execute_query(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    query_threads: usize,
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

    let limit = limit.unwrap_or(1000);
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

    // OPTIMIZATION: Collect block metadata first for potential parallel processing
    let mut block_metadata: Vec<(Vec<u8>, i64, i64, usize)> = Vec::new();

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

        let row_count = u32::from_le_bytes([
            value[pos + 16],
            value[pos + 17],
            value[pos + 18],
            value[pos + 19],
        ]) as usize;

        // Skip blocks entirely outside our range
        if block_max_ts < start_ts || block_min_ts > end_ts {
            continue;
        }

        // Collect block metadata for processing
        block_metadata.push((block_id.to_vec(), block_min_ts, block_max_ts, row_count));
    }

    // OPTIMIZATION: Process blocks with potential parallelization
    // println!("DEBUG: Found {} blocks in index", block_metadata.len());

    // Calculate global offsets and prepare tasks
    let mut tasks = Vec::with_capacity(block_metadata.len());
    let mut current_skipped = 0;
    let mut current_taken = 0;

    for (block_id, block_min_ts, block_max_ts, row_count) in block_metadata {
        // If we haven't reached the offset yet
        if current_skipped + row_count <= offset {
            current_skipped += row_count;
            continue;
        }

        // If we have enough results
        if current_taken >= limit {
            break;
        }

        let skip_in_block = offset.saturating_sub(current_skipped);
        let remaining_limit = limit - current_taken;
        let available_in_block = row_count - skip_in_block;
        let take_from_block = remaining_limit.min(available_in_block);

        if take_from_block > 0 {
            tasks.push((
                block_id,
                block_min_ts,
                block_max_ts,
                skip_in_block,
                take_from_block,
            ));
            current_taken += take_from_block;
        }

        current_skipped += row_count;
    }

    use rayon::prelude::*;

    // ADAPTIVE PARALLELISM: Only use parallel processing when beneficial
    // - Small task counts (< 4 blocks) have too much overhead
    // - Single-threaded config should use sequential processing
    let use_parallel = tasks.len() >= 4 && query_threads > 1;

    let results_batches: Result<Vec<Vec<Value>>> = if use_parallel {
        tasks
            .par_iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    process_block_filtered(
                        db,
                        table,
                        schema,
                        block_id,
                        *block_min_ts,
                        *block_max_ts,
                        *skip_in_block,
                        *take_from_block,
                        start_ts,
                        end_ts,
                    )
                },
            )
            .collect()
    } else {
        // Sequential processing for small workloads or single-threaded config
        tasks
            .iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    process_block_filtered(
                        db,
                        table,
                        schema,
                        block_id,
                        *block_min_ts,
                        *block_max_ts,
                        *skip_in_block,
                        *take_from_block,
                        start_ts,
                        end_ts,
                    )
                },
            )
            .collect()
    };

    let mut results = Vec::with_capacity(limit);
    for batch in results_batches? {
        results.extend(batch);
        if results.len() >= limit {
            results.truncate(limit);
            break;
        }
    }

    Ok(results)
}

pub fn parse_query_timestamp(timestamp: &str) -> Result<i64> {
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

/// Process a single block with optimized filtering and JSON conversion
/// Generate primary key from hash for ID-based lookup: [table_hash:u32][id:u64]
/// Returns stack-allocated array to avoid heap allocation
fn generate_id_key_from_hash(table_hash: u32, id: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[0..4].copy_from_slice(&table_hash.to_be_bytes());
    key[4..12].copy_from_slice(&id.to_be_bytes());
    key
}

/// Process a single block with optimized filtering and JSON conversion
#[allow(clippy::too_many_arguments)]
fn process_block_filtered(
    db: &DB,
    table: &str,
    schema: &Schema,
    block_id: &[u8],
    block_min_ts: i64,
    block_max_ts: i64,
    skip_in_block: usize,
    take_from_block: usize,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<Value>> {
    let mut fetch_opts = ReadOptions::default();
    fetch_opts.set_verify_checksums(false);
    fetch_opts.fill_cache(true);

    if let Ok(Some(block_data)) = db.get_opt(block_id, &fetch_opts) {
        if let Ok(block) = ColumnBlock::deserialize(&block_data) {
            // OPTIMIZATION: Calculate hash once
            let table_hash = calculate_table_hash(table);

            // Check if we need filtering
            if block_min_ts < start_ts || block_max_ts > end_ts {
                // Filtered path
                let indices = block.filter_by_timestamp(schema, start_ts, end_ts)?;
                let relevant_indices: Vec<usize> = indices
                    .into_iter()
                    .filter(|&idx| idx >= skip_in_block && idx < skip_in_block + take_from_block)
                    .collect();

                if relevant_indices.is_empty() {
                    return Ok(Vec::new());
                }

                let json_values = block.to_json_filtered(schema, &relevant_indices)?;

                // Validity Check
                let mut read_opts = ReadOptions::default();
                read_opts.set_verify_checksums(false);
                read_opts.fill_cache(true);

                let mut keys = Vec::with_capacity(json_values.len());
                let mut key_indices = Vec::with_capacity(json_values.len());

                for (i, json_row) in json_values.iter().enumerate() {
                    if let Some(id) = json_row.get(&schema.id_column).and_then(|v| v.as_u64()) {
                        keys.push(generate_id_key_from_hash(table_hash, id));
                        key_indices.push(i);
                    }
                }

                let ref_results = db.multi_get(&keys);
                let mut is_valid = vec![true; json_values.len()];

                for (k_idx, result) in ref_results.into_iter().enumerate() {
                    let json_idx = key_indices[k_idx];
                    let actual_row_idx = relevant_indices[json_idx];

                    if let Ok(Some(ref_data)) = result {
                        if let Ok((latest_block_id, latest_row_idx)) =
                            parse_reference_data(&ref_data)
                        {
                            if latest_block_id != block_id || latest_row_idx != actual_row_idx {
                                is_valid[json_idx] = false;
                            }
                        }
                    }
                }

                let mut batch_results = Vec::with_capacity(json_values.len());
                for (i, json_row) in json_values.into_iter().enumerate() {
                    if is_valid[i] {
                        batch_results.push(json_row);
                    }
                }
                return Ok(batch_results);
            } else {
                // Full slice path (faster for full scans)
                let json_values = block.to_json_slice(schema, skip_in_block, take_from_block)?;

                // Validity Check
                let mut read_opts = ReadOptions::default();
                read_opts.set_verify_checksums(false);
                read_opts.fill_cache(true);

                let mut keys = Vec::with_capacity(json_values.len());
                let mut key_indices = Vec::with_capacity(json_values.len());

                for (i, json_row) in json_values.iter().enumerate() {
                    if let Some(id) = json_row.get(&schema.id_column).and_then(|v| v.as_u64()) {
                        keys.push(generate_id_key_from_hash(table_hash, id));
                        key_indices.push(i);
                    }
                }

                let ref_results = db.multi_get(&keys);
                let mut is_valid = vec![true; json_values.len()];

                for (k_idx, result) in ref_results.into_iter().enumerate() {
                    let json_idx = key_indices[k_idx];
                    let actual_row_idx = skip_in_block + json_idx;

                    if let Ok(Some(ref_data)) = result {
                        if let Ok((latest_block_id, latest_row_idx)) =
                            parse_reference_data(&ref_data)
                        {
                            if latest_block_id != block_id || latest_row_idx != actual_row_idx {
                                is_valid[json_idx] = false;
                            }
                        }
                    }
                }

                let mut batch_results = Vec::with_capacity(json_values.len());
                for (i, json_row) in json_values.into_iter().enumerate() {
                    if is_valid[i] {
                        batch_results.push(json_row);
                    }
                }
                return Ok(batch_results);
            }
        }
    }
    Ok(Vec::new())
}

#[allow(clippy::too_many_arguments)]
pub fn execute_query_csv(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    query_threads: usize,
) -> Result<Vec<String>> {
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

    let limit = limit.unwrap_or(1000);
    let offset = offset.unwrap_or(0);

    // Build block index scan key
    let table_hash = calculate_table_hash(table);
    let mut start_key = Vec::with_capacity(5);
    start_key.extend_from_slice(&table_hash.to_be_bytes());
    start_key.push(b'B');

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    read_opts.fill_cache(true);
    read_opts.set_readahead_size(4 * 1024 * 1024);

    let mut iter_read_opts = ReadOptions::default();
    iter_read_opts.set_verify_checksums(false);
    iter_read_opts.fill_cache(true);
    iter_read_opts.set_readahead_size(4 * 1024 * 1024);

    let iter = db.iterator_opt(
        IteratorMode::From(&start_key, Direction::Forward),
        iter_read_opts,
    );

    let mut block_metadata: Vec<(Vec<u8>, i64, i64, usize)> = Vec::new();

    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };

        if key.len() < 5 || key[4] != b'B' {
            continue;
        }

        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }

        if key.len() >= 13 {
            let block_min_ts = i64::from_be_bytes([
                key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12],
            ]);
            if block_min_ts > end_ts {
                break;
            }
        }

        if value.len() < 24 {
            continue;
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

        let row_count = u32::from_le_bytes([
            value[pos + 16],
            value[pos + 17],
            value[pos + 18],
            value[pos + 19],
        ]) as usize;

        if block_max_ts < start_ts || block_min_ts > end_ts {
            continue;
        }

        block_metadata.push((block_id.to_vec(), block_min_ts, block_max_ts, row_count));
    }

    let mut tasks = Vec::with_capacity(block_metadata.len());
    let mut current_skipped = 0;
    let mut current_taken = 0;

    for (block_id, block_min_ts, block_max_ts, row_count) in block_metadata {
        if current_skipped + row_count <= offset {
            current_skipped += row_count;
            continue;
        }

        if current_taken >= limit {
            break;
        }

        let skip_in_block = offset.saturating_sub(current_skipped);
        let remaining_limit = limit - current_taken;
        let available_in_block = row_count - skip_in_block;
        let take_from_block = remaining_limit.min(available_in_block);

        if take_from_block > 0 {
            tasks.push((
                block_id,
                block_min_ts,
                block_max_ts,
                skip_in_block,
                take_from_block,
            ));
            current_taken += take_from_block;
        }

        current_skipped += row_count;
    }

    use rayon::prelude::*;
    let use_parallel = tasks.len() >= 4 && query_threads > 1;

    let results_batches: Result<Vec<String>> = if use_parallel {
        tasks
            .par_iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    let skip_in_block = *skip_in_block;
                    let take_from_block = *take_from_block;
                    let block_min_ts = *block_min_ts;
                    let block_max_ts = *block_max_ts;

                    let mut fetch_opts = ReadOptions::default();
                    fetch_opts.set_verify_checksums(false);
                    fetch_opts.fill_cache(true);

                    if let Ok(Some(block_data)) = db.get_opt(block_id, &fetch_opts) {
                        if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                            if block_min_ts < start_ts || block_max_ts > end_ts {
                                let indices =
                                    block.filter_by_timestamp(schema, start_ts, end_ts)?;
                                let relevant_indices: Vec<usize> = indices
                                    .into_iter()
                                    .filter(|&idx| {
                                        idx >= skip_in_block
                                            && idx < skip_in_block + take_from_block
                                    })
                                    .collect();
                                return block.to_csv_filtered(schema, &relevant_indices);
                            } else {
                                return block.to_csv(schema, skip_in_block, take_from_block);
                            }
                        }
                    }
                    Ok(String::new())
                },
            )
            .collect()
    } else {
        tasks
            .iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    let skip_in_block = *skip_in_block;
                    let take_from_block = *take_from_block;
                    let block_min_ts = *block_min_ts;
                    let block_max_ts = *block_max_ts;

                    let mut fetch_opts = ReadOptions::default();
                    fetch_opts.set_verify_checksums(false);
                    fetch_opts.fill_cache(true);

                    if let Ok(Some(block_data)) = db.get_opt(block_id, &fetch_opts) {
                        if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                            if block_min_ts < start_ts || block_max_ts > end_ts {
                                let indices =
                                    block.filter_by_timestamp(schema, start_ts, end_ts)?;
                                let relevant_indices: Vec<usize> = indices
                                    .into_iter()
                                    .filter(|&idx| {
                                        idx >= skip_in_block
                                            && idx < skip_in_block + take_from_block
                                    })
                                    .collect();
                                return block.to_csv_filtered(schema, &relevant_indices);
                            } else {
                                return block.to_csv(schema, skip_in_block, take_from_block);
                            }
                        }
                    }
                    Ok(String::new())
                },
            )
            .collect()
    };

    results_batches
}

#[allow(clippy::too_many_arguments)]
pub fn execute_query_arrow(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    query_threads: usize,
) -> Result<Vec<RecordBatch>> {
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

    let limit = limit.unwrap_or(1000);
    let offset = offset.unwrap_or(0);

    // Build block index scan key
    let table_hash = calculate_table_hash(table);
    let mut start_key = Vec::with_capacity(5);
    start_key.extend_from_slice(&table_hash.to_be_bytes());
    start_key.push(b'B');

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    read_opts.fill_cache(true);
    read_opts.set_readahead_size(4 * 1024 * 1024);

    let mut iter_read_opts = ReadOptions::default();
    iter_read_opts.set_verify_checksums(false);
    iter_read_opts.fill_cache(true);
    iter_read_opts.set_readahead_size(4 * 1024 * 1024);

    let iter = db.iterator_opt(
        IteratorMode::From(&start_key, Direction::Forward),
        iter_read_opts,
    );

    let mut block_metadata: Vec<(Vec<u8>, i64, i64, usize)> = Vec::new();

    for item in iter {
        let (key, value) = match item {
            Ok((k, v)) => (k, v),
            Err(_) => continue,
        };

        if key.len() < 5 || key[4] != b'B' {
            continue;
        }

        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }

        if key.len() >= 13 {
            let block_min_ts = i64::from_be_bytes([
                key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12],
            ]);
            if block_min_ts > end_ts {
                break;
            }
        }

        if value.len() < 24 {
            continue;
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

        let row_count = u32::from_le_bytes([
            value[pos + 16],
            value[pos + 17],
            value[pos + 18],
            value[pos + 19],
        ]) as usize;

        if block_max_ts < start_ts || block_min_ts > end_ts {
            continue;
        }

        block_metadata.push((block_id.to_vec(), block_min_ts, block_max_ts, row_count));
    }

    let mut tasks = Vec::with_capacity(block_metadata.len());
    let mut current_skipped = 0;
    let mut current_taken = 0;

    for (block_id, block_min_ts, block_max_ts, row_count) in block_metadata {
        if current_skipped + row_count <= offset {
            current_skipped += row_count;
            continue;
        }

        if current_taken >= limit {
            break;
        }

        let skip_in_block = offset.saturating_sub(current_skipped);
        let remaining_limit = limit - current_taken;
        let available_in_block = row_count - skip_in_block;
        let take_from_block = remaining_limit.min(available_in_block);

        if take_from_block > 0 {
            tasks.push((
                block_id,
                block_min_ts,
                block_max_ts,
                skip_in_block,
                take_from_block,
            ));
            current_taken += take_from_block;
        }

        current_skipped += row_count;
    }

    use rayon::prelude::*;
    let use_parallel = tasks.len() >= 4 && query_threads > 1;

    let results_batches: Result<Vec<Option<RecordBatch>>> = if use_parallel {
        tasks
            .par_iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    let skip_in_block = *skip_in_block;
                    let take_from_block = *take_from_block;
                    let block_min_ts = *block_min_ts;
                    let block_max_ts = *block_max_ts;

                    let mut fetch_opts = ReadOptions::default();
                    fetch_opts.set_verify_checksums(false);
                    fetch_opts.fill_cache(true);

                    if let Ok(Some(block_data)) = db.get_opt(block_id, &fetch_opts) {
                        if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                            if block_min_ts < start_ts || block_max_ts > end_ts {
                                let indices =
                                    block.filter_by_timestamp(schema, start_ts, end_ts)?;
                                let relevant_indices: Vec<usize> = indices
                                    .into_iter()
                                    .filter(|&idx| {
                                        idx >= skip_in_block
                                            && idx < skip_in_block + take_from_block
                                    })
                                    .collect();
                                return block
                                    .to_arrow_filtered(schema, &relevant_indices)
                                    .map(Some);
                            } else {
                                return block
                                    .to_arrow(schema, skip_in_block, take_from_block)
                                    .map(Some);
                            }
                        }
                    }
                    Ok(None)
                },
            )
            .collect()
    } else {
        tasks
            .iter()
            .map(
                |(block_id, block_min_ts, block_max_ts, skip_in_block, take_from_block)| {
                    let skip_in_block = *skip_in_block;
                    let take_from_block = *take_from_block;
                    let block_min_ts = *block_min_ts;
                    let block_max_ts = *block_max_ts;

                    let mut fetch_opts = ReadOptions::default();
                    fetch_opts.set_verify_checksums(false);
                    fetch_opts.fill_cache(true);

                    if let Ok(Some(block_data)) = db.get_opt(block_id, &fetch_opts) {
                        if let Ok(block) = ColumnBlock::deserialize(&block_data) {
                            if block_min_ts < start_ts || block_max_ts > end_ts {
                                let indices =
                                    block.filter_by_timestamp(schema, start_ts, end_ts)?;
                                let relevant_indices: Vec<usize> = indices
                                    .into_iter()
                                    .filter(|&idx| {
                                        idx >= skip_in_block
                                            && idx < skip_in_block + take_from_block
                                    })
                                    .collect();
                                return block
                                    .to_arrow_filtered(schema, &relevant_indices)
                                    .map(Some);
                            } else {
                                return block
                                    .to_arrow(schema, skip_in_block, take_from_block)
                                    .map(Some);
                            }
                        }
                    }
                    Ok(None)
                },
            )
            .collect()
    };

    let mut batches = Vec::with_capacity(tasks.len());
    for batch in (results_batches?).into_iter().flatten() {
        batches.push(batch);
    }

    Ok(batches)
}
#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;
