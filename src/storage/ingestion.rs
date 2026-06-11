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
use crate::storage::id_manager::{IdManager, IdManagerRegistry, RowId};
use crate::storage::refs;
use crate::storage::schema::Schema;
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::ipc::reader::StreamReader;
use prost::Message;
use rayon::prelude::*;
use std::io::Cursor;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoRow {
    #[prost(map = "string, string", tag = "1")]
    pub values: HashMap<String, String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoBatch {
    #[prost(message, repeated, tag = "1")]
    pub rows: Vec<ProtoRow>,
}

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

fn process_single_row(
    mut row: HashMap<String, String>,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<(u64, HashMap<String, String>)> {
    // ID Assignment
    let row_id = if let Some(id_str) = row.get(&schema.id_column) {
        if id_str.trim().is_empty() {
            RowId::Auto
        } else {
            let parsed_id = RowId::from_string(id_str)?;
            if let RowId::User(id) = parsed_id {
                id_manager.register_user_id(id)?;
            }
            parsed_id
        }
    } else {
        RowId::Auto
    };

    let actual_id = row_id.resolve(id_manager);
    row.insert(schema.id_column.clone(), actual_id.to_string());

    // Validation
    schema.validate_row(&row)?;

    Ok((actual_id, row))
}

pub fn process_rows_parallel(
    rows: Vec<HashMap<String, String>>,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<Vec<(u64, HashMap<String, String>)>> {
    // ADAPTIVE PARALLELISM: Only use parallel processing when beneficial
    if rows.len() >= 100 && rayon::current_num_threads() > 1 {
        rows.into_par_iter()
            .map(|row| process_single_row(row, schema, id_manager))
            .collect()
    } else {
        // Sequential processing for small batches or single-threaded config
        rows.into_iter()
            .map(|row| process_single_row(row, schema, id_manager))
            .collect()
    }
}

pub fn parse_arrow(arrow_data: &[u8]) -> Result<Vec<HashMap<String, String>>> {
    let cursor = Cursor::new(arrow_data);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        PulsoraError::InvalidData(format!("Failed to create Arrow stream reader: {}", e))
    })?;

    let mut all_rows = Vec::new();

    for batch in reader {
        let batch = batch
            .map_err(|e| PulsoraError::InvalidData(format!("Failed to read Arrow batch: {}", e)))?;

        let schema_ref = batch.schema();
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut row_map = HashMap::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let field = schema_ref.field(col_idx);
                let col_name = field.name();
                let column = batch.column(col_idx);

                if column.is_null(row_idx) {
                    continue;
                }

                let value_str = match column.data_type() {
                    ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Int64 => {
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    ArrowDataType::Boolean => {
                        let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        array.value(row_idx).to_string()
                    }
                    _ => {
                        // Fallback for other types: try to cast to string or just skip/error
                        // For now, let's skip unsupported types or try debug format
                        // A robust implementation would handle all types
                        continue;
                    }
                };
                row_map.insert(col_name.clone(), value_str);
            }
            if !row_map.is_empty() {
                all_rows.push(row_map);
            }
        }
    }
    Ok(all_rows)
}

pub fn parse_protobuf(proto_data: &[u8]) -> Result<Vec<HashMap<String, String>>> {
    let batch = ProtoBatch::decode(proto_data)
        .map_err(|e| PulsoraError::InvalidData(format!("Failed to decode Protobuf data: {}", e)))?;
    Ok(batch.rows.into_iter().map(|r| r.values).collect())
}

#[allow(clippy::type_complexity)]
pub fn process_csv_bulk(
    csv_data: &str,
    schema: &Schema,
    id_manager: &Arc<IdManager>,
) -> Result<(ColumnBlock, Vec<u64>, i64, i64)> {
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());
    let headers = reader.headers()?.clone();

    // Map Schema Column Name -> CSV Index
    let mut schema_to_csv_idx = HashMap::with_capacity(schema.columns.len());
    for (i, header) in headers.iter().enumerate() {
        schema_to_csv_idx.insert(header, i);
    }

    // Initialize column vectors
    let est_rows = csv_data.len() / 50; // Rough estimate
    let mut id_col_vals = Vec::with_capacity(est_rows);
    let mut int_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut float_cols: HashMap<String, Vec<f64>> = HashMap::new();
    let mut string_cols: HashMap<String, Vec<String>> = HashMap::new();
    let mut bool_cols: HashMap<String, Vec<bool>> = HashMap::new();
    let mut ts_cols: HashMap<String, Vec<i64>> = HashMap::new();
    let mut null_bitmaps: HashMap<String, Vec<u8>> = HashMap::new();

    // Initialize vectors for each column in schema
    for col in &schema.columns {
        match col.data_type {
            crate::storage::schema::DataType::Id => {} // Handled separately
            crate::storage::schema::DataType::Integer => {
                int_cols.insert(col.name.clone(), Vec::with_capacity(est_rows));
            }
            crate::storage::schema::DataType::Float => {
                float_cols.insert(col.name.clone(), Vec::with_capacity(est_rows));
            }
            crate::storage::schema::DataType::String => {
                string_cols.insert(col.name.clone(), Vec::with_capacity(est_rows));
            }
            crate::storage::schema::DataType::Boolean => {
                bool_cols.insert(col.name.clone(), Vec::with_capacity(est_rows));
            }
            crate::storage::schema::DataType::Timestamp => {
                ts_cols.insert(col.name.clone(), Vec::with_capacity(est_rows));
            }
        }
        null_bitmaps.insert(col.name.clone(), Vec::new());
    }

    let mut row_count = 0;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let ts_col_name = schema.get_timestamp_column();

    for result in reader.records() {
        let record = result?;
        row_count += 1;

        // Handle ID
        let id = if let Some(&idx) = schema_to_csv_idx.get(schema.id_column.as_str()) {
            let val = record.get(idx).unwrap_or("");
            if val.is_empty() {
                id_manager.next_auto_id()
            } else if let Ok(uid) = val.parse::<u64>() {
                id_manager.register_user_id(uid)?;
                uid
            } else {
                id_manager.next_auto_id()
            }
        } else {
            id_manager.next_auto_id()
        };
        id_col_vals.push(id);

        // Handle other columns
        for col in &schema.columns {
            if col.name == schema.id_column {
                continue;
            }

            let val_str = if let Some(&idx) = schema_to_csv_idx.get(col.name.as_str()) {
                record.get(idx)
            } else {
                None
            };

            let is_null = val_str.is_none() || val_str.unwrap().is_empty();

            // Update null bitmap
            let byte_idx = (row_count - 1) >> 3;
            let bit_idx = (row_count - 1) & 7;
            let bitmap = null_bitmaps.get_mut(&col.name).unwrap();
            if bitmap.len() <= byte_idx {
                bitmap.push(0);
            }
            if is_null {
                bitmap[byte_idx] |= 1 << bit_idx;
            }

            match col.data_type {
                crate::storage::schema::DataType::Id => {} // Already handled
                crate::storage::schema::DataType::Integer => {
                    let val = if is_null {
                        0
                    } else {
                        val_str.unwrap().parse::<i64>().unwrap_or(0)
                    };
                    int_cols.get_mut(&col.name).unwrap().push(val);

                    if Some(col.name.as_str()) == ts_col_name {
                        min_ts = min_ts.min(val);
                        max_ts = max_ts.max(val);
                    }
                }
                crate::storage::schema::DataType::Float => {
                    let val = if is_null {
                        0.0
                    } else {
                        val_str.unwrap().parse::<f64>().unwrap_or(0.0)
                    };
                    float_cols.get_mut(&col.name).unwrap().push(val);
                }
                crate::storage::schema::DataType::String => {
                    let val = if is_null {
                        String::new()
                    } else {
                        val_str.unwrap().to_string()
                    };
                    string_cols.get_mut(&col.name).unwrap().push(val);
                }
                crate::storage::schema::DataType::Boolean => {
                    let val = if is_null {
                        false
                    } else {
                        val_str.unwrap().parse::<bool>().unwrap_or(false)
                    };
                    bool_cols.get_mut(&col.name).unwrap().push(val);
                }
                crate::storage::schema::DataType::Timestamp => {
                    let val = if is_null {
                        chrono::Utc::now().timestamp_millis()
                    } else {
                        parse_timestamp(val_str.unwrap())
                            .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis())
                    };
                    ts_cols.get_mut(&col.name).unwrap().push(val);

                    if Some(col.name.as_str()) == ts_col_name {
                        min_ts = min_ts.min(val);
                        max_ts = max_ts.max(val);
                    }
                }
            }
        }
    }

    if row_count == 0 {
        return Ok((
            ColumnBlock {
                row_count: 0,
                columns: HashMap::new(),
                null_bitmaps: HashMap::new(),
            },
            Vec::new(),
            0,
            0,
        ));
    }

    // Compress columns
    let mut columns = HashMap::new();

    // ID Column
    columns.insert(
        schema.id_column.clone(),
        crate::storage::columnar::compress_id_column(&id_col_vals)?,
    );

    for (name, vals) in int_cols {
        columns.insert(name, crate::storage::columnar::compress_int_column(&vals)?);
    }
    for (name, vals) in float_cols {
        columns.insert(
            name,
            crate::storage::columnar::compress_float_column(&vals)?,
        );
    }
    for (name, vals) in string_cols {
        columns.insert(
            name,
            crate::storage::columnar::compress_string_column(&vals)?,
        );
    }
    for (name, vals) in bool_cols {
        columns.insert(name, crate::storage::columnar::compress_bool_column(&vals)?);
    }
    for (name, vals) in &ts_cols {
        columns.insert(
            name.clone(),
            crate::storage::columnar::compress_timestamp_column(vals)?,
        );
    }

    Ok((
        ColumnBlock {
            row_count,
            columns,
            null_bitmaps,
        },
        id_col_vals,
        min_ts,
        max_ts,
    ))
}

pub fn write_column_block_to_rocksdb(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    column_block: &ColumnBlock,
    ids: &[u64],
    min_timestamp: i64,
    max_timestamp: i64,
) -> Result<u64> {
    let serialized_block = column_block.serialize()?;
    write_block_core(
        db,
        table,
        schema,
        &serialized_block,
        ids,
        min_timestamp,
        max_timestamp,
    )
}

pub fn write_batch_to_rocksdb(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    rows: &[(u64, HashMap<String, String>)],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let column_block = ColumnBlock::from_rows(rows, schema)?;
    let serialized_block = column_block.serialize()?;
    let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();

    // Min and max timestamps for the block index; current time when the
    // schema has no timestamp column.
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    for (_, row) in rows.iter() {
        if let Some(ts_col) = schema.get_timestamp_column() {
            if let Some(ts_value) = row.get(ts_col) {
                if let Ok(ts) = parse_timestamp(ts_value) {
                    min_timestamp = min_timestamp.min(ts);
                    max_timestamp = max_timestamp.max(ts);
                }
            }
        }
    }
    if min_timestamp == i64::MAX {
        let now = Utc::now().timestamp_millis();
        min_timestamp = now;
        max_timestamp = now;
    }

    write_block_core(
        db,
        table,
        schema,
        &serialized_block,
        &ids,
        min_timestamp,
        max_timestamp,
    )
}

/// Scan the table's block index and return metadata for every block.
/// Ingest-side REPLACE detection needs the id ranges; the scan is one
/// sequential pass over hot, prefix-compressed index keys.
fn scan_table_blocks(db: &DB, table_hash: u32) -> Vec<refs::BlockMeta> {
    use rocksdb::{Direction, IteratorMode};
    let mut start_key = Vec::with_capacity(5);
    start_key.extend_from_slice(&table_hash.to_be_bytes());
    start_key.push(b'B');

    let iter = db.iterator(IteratorMode::From(&start_key, Direction::Forward));
    let mut out = Vec::new();
    for item in iter {
        let Ok((key, value)) = item else { break };
        if key.len() < 4 {
            continue;
        }
        if u32::from_be_bytes([key[0], key[1], key[2], key[3]]) != table_hash {
            break;
        }
        // Markers sort 'B' < 'D' < 'O' and the scan starts at the 'B' range:
        // the first non-'B' key ends it. `continue` here would walk the whole
        // 'D' range, materializing every serialized block along the way.
        if key.len() < 5 || key[4] != b'B' {
            break;
        }
        if let Ok(meta) = refs::parse_block_index_value(&value) {
            out.push(meta);
        }
    }
    out
}

/// Write one immutable block plus the bookkeeping that keeps REPLACE
/// semantics without any per-row index entries:
///
/// 1. In-batch duplicate ids: only the LAST occurrence is live; earlier
///    positions go into the new block's own override set.
/// 2. Cross-block REPLACE: every existing block whose id range overlaps the
///    batch is checked (one cheap id-column decode); positions holding
///    re-ingested ids are merged into that block's override set.
///
/// Everything lands in ONE WriteBatch, so a crash can never leave a new copy
/// live without its predecessors dead. Override updates use `merge` (set
/// union), so concurrent batches replacing rows of the same old block cannot
/// lose each other's updates; callers serialize ingests per table so the
/// duplicate DETECTION itself is race-free.
fn write_block_core(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    serialized_block: &[u8],
    ids: &[u64],
    min_timestamp: i64,
    max_timestamp: i64,
) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let table_hash = calculate_table_hash(table);

    // In-batch duplicate ids: keep the last occurrence, override the rest.
    let mut last_pos: HashMap<u64, u32> = HashMap::with_capacity(ids.len());
    for (pos, &id) in ids.iter().enumerate() {
        last_pos.insert(id, pos as u32);
    }
    let mut self_overrides: Vec<u32> = Vec::new();
    if last_pos.len() != ids.len() {
        for (pos, &id) in ids.iter().enumerate() {
            if last_pos[&id] != pos as u32 {
                self_overrides.push(pos as u32);
            }
        }
    }

    let batch_min_id = *ids.iter().min().unwrap();
    let batch_max_id = *ids.iter().max().unwrap();

    // Cross-block REPLACE detection: only blocks whose id range overlaps the
    // batch can hold previous copies. Append-style workloads (monotonic ids)
    // overlap at most the newest block; REPLACE-heavy tables stay small.
    let mut old_overrides: Vec<(u64, Vec<u32>)> = Vec::new();
    for meta in scan_table_blocks(db, table_hash) {
        if meta.min_id > batch_max_id || meta.max_id < batch_min_id {
            continue;
        }
        let Some(old_block) = read_block(db, table_hash, meta.block)? else {
            continue;
        };
        let old_ids = old_block.get_id_values(schema)?;
        let dead: Vec<u32> = old_ids
            .iter()
            .enumerate()
            .filter(|(_, oid)| last_pos.contains_key(oid))
            .map(|(pos, _)| pos as u32)
            .collect();
        if !dead.is_empty() {
            old_overrides.push((meta.block, dead));
        }
    }

    let block_id = refs::allocate_block_id(db)?;
    let mut batch = WriteBatch::default();
    batch.put(refs::block_data_key(table_hash, block_id), serialized_block);
    batch.put(
        refs::block_index_key(table_hash, min_timestamp, block_id),
        refs::encode_block_index_value(&refs::BlockMeta {
            block: block_id,
            min_ts: min_timestamp,
            max_ts: max_timestamp,
            rows: ids.len() as u32,
            min_id: batch_min_id,
            max_id: batch_max_id,
        }),
    );
    if !self_overrides.is_empty() {
        batch.merge(
            refs::override_key(table_hash, block_id),
            refs::encode_override_positions(&self_overrides),
        );
    }
    for (old_block, dead) in &old_overrides {
        batch.merge(
            refs::override_key(table_hash, *old_block),
            refs::encode_override_positions(dead),
        );
    }

    // Synced write: callers truncate the application-level buffer WAL right
    // after this returns, so the RocksDB WAL must be on disk first — an
    // unsynced write would open a crash window where BOTH logs lose the
    // batch. One fsync per block (not per row) is negligible against the
    // block payload itself.
    let mut write_opts = rocksdb::WriteOptions::default();
    write_opts.set_sync(true);
    db.write_opt(batch, &write_opts)?;
    debug!("Wrote column block with {} rows", ids.len());
    Ok(ids.len() as u64)
}

/// Fetch and deserialize one block by id (ingest-side helper).
fn read_block(db: &DB, table_hash: u32, block_id: u64) -> Result<Option<ColumnBlock>> {
    match db.get(refs::block_data_key(table_hash, block_id))? {
        Some(data) => Ok(Some(ColumnBlock::deserialize(&data)?)),
        None => Ok(None),
    }
}

#[allow(dead_code)]
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

    // Duplicate ids must not straddle chunk boundaries: chunks are written in
    // parallel and cross-chunk copies of one id would race REPLACE detection.
    // Keep the LAST occurrence (REPLACE semantics), preserving arrival order.
    let mut keep_pos: HashMap<u64, usize> = HashMap::with_capacity(processed_rows.len());
    for (pos, (id, _)) in processed_rows.iter().enumerate() {
        keep_pos.insert(*id, pos);
    }
    if keep_pos.len() != processed_rows.len() {
        let mut deduped = Vec::with_capacity(keep_pos.len());
        for (pos, pair) in processed_rows.into_iter().enumerate() {
            if keep_pos[&pair.0] == pos {
                deduped.push(pair);
            }
        }
        processed_rows = deduped;
    }

    // Process rows in chunks for columnar storage
    let chunks: Vec<_> = processed_rows.chunks(batch_size).collect();

    // Chunks are written SEQUENTIALLY: block writes run REPLACE detection
    // against the blocks already on disk, and parallel chunk writes would
    // race that detection (a chunk cannot see a sibling's in-flight block).
    let mut total_inserted = 0u64;
    for chunk in chunks {
        total_inserted += write_batch_to_rocksdb(db, table, schema, chunk)?;
    }

    Ok(total_inserted)
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
#[path = "ingestion_test.rs"]
mod ingestion_test;
