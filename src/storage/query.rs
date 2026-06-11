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
use crate::storage::refs;
use crate::storage::schema::Schema;

/// Retrieve a single row by id.
///
/// There are no per-row index entries: candidate blocks are those whose
/// stored id range covers `id` (block index scan), checked NEWEST first —
/// block ids are strictly increasing with write time, so the first
/// non-overridden hit is the live copy.
pub fn get_row_by_id(
    db: &Arc<DB>,
    table: &str,
    schema: &Schema,
    id: u64,
) -> Result<Option<HashMap<String, String>>> {
    let table_hash = calculate_table_hash(table);
    // One snapshot covers the index scan AND the block/override reads: a
    // concurrent REPLACE commits {new block + overrides} atomically, and a
    // mixed view (old candidate set + new overrides) would serve nothing.
    let snap = db.snapshot();
    let mut candidates: Vec<refs::BlockMeta> =
        scan_block_metadata(&snap, table_hash, i64::MIN, i64::MAX)
            .into_iter()
            .filter(|meta| meta.min_id <= id && id <= meta.max_id)
            .collect();
    candidates.sort_unstable_by(|a, b| b.block.cmp(&a.block));

    for meta in candidates {
        let Some(block) = fetch_block(&snap, table_hash, meta.block)? else {
            continue;
        };
        let ids = block.get_id_values(schema)?;
        // rposition: a block can hold the same id more than once (in-batch
        // duplicates), with every position except the LAST self-overridden —
        // only the last occurrence can be the live copy.
        let Some(row_idx) = ids.iter().rposition(|&row_id| row_id == id) else {
            continue;
        };
        let overrides = load_overrides(&snap, table_hash, meta.block)?;
        if overrides.contains(&(row_idx as u32)) {
            // Dead copy; any live copy sits in a NEWER block, which this
            // newest-first scan has already visited — keep going only to
            // honor older candidates of the same id (also dead) cleanly.
            continue;
        }
        let rows = block.to_rows(schema)?;
        return Ok(rows.get(row_idx).cloned());
    }
    Ok(None)
}

/// Load a block's override set (dead row positions). Absent key = all live.
fn load_overrides(
    snap: &rocksdb::Snapshot<'_>,
    table_hash: u32,
    block_id: u64,
) -> Result<std::collections::HashSet<u32>> {
    let mut opts = ReadOptions::default();
    opts.set_verify_checksums(false);
    opts.fill_cache(true);
    match snap.get_opt(refs::override_key(table_hash, block_id), opts)? {
        Some(data) => Ok(refs::decode_override_positions(&data)),
        None => Ok(std::collections::HashSet::new()),
    }
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
    let start_ts = start
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(0);
    let end_ts = end
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(i64::MAX);
    let limit = limit.unwrap_or(1000);
    let offset = offset.unwrap_or(0);
    let needed = offset.saturating_add(limit);

    let table_hash = calculate_table_hash(table);
    // One snapshot for the whole query: the index scan and every block /
    // override read must observe the same state, or a concurrent REPLACE
    // (atomic {new block + overrides} batch) transiently hides the row.
    let snap = db.snapshot();
    // Ascending by block min_ts from the index scan
    let metadata = scan_block_metadata(&snap, table_hash, start_ts, end_ts);

    use rayon::prelude::*;
    let ts_col = schema.get_timestamp_column().map(str::to_string);

    // Row timestamp in milliseconds for the early-exit bound. Values render
    // either as epoch numbers or formatted strings depending on column type.
    let row_ts = |row: &Value| -> i64 {
        let Some(ts_col) = ts_col.as_deref() else {
            return 0;
        };
        let ts = match row.get(ts_col) {
            Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
            Some(Value::String(s)) => crate::storage::ingestion::parse_timestamp(s).unwrap_or(0),
            _ => 0,
        };
        // Normalize plain epoch seconds to milliseconds (block keys are ms)
        if ts > 0 && ts < 100_000_000_000 {
            ts * 1000
        } else {
            ts
        }
    };

    // Each in-range block is processed independently — fetch, validity-filter,
    // serialize the live rows to JSON. Blocks are consumed in chunks with an
    // EARLY EXIT once enough valid rows are collected AND no unprocessed block
    // can still contribute to the first `needed` ranks: a `limit=1` probe must
    // not decompress the whole table. Only valid (non-stale) rows count toward
    // the exit, so REPLACE-stale rows cannot starve later blocks; overlapping
    // block time ranges are handled by bounding on collected row timestamps.
    let chunk_size = if query_threads > 1 {
        (query_threads * 2).max(4)
    } else {
        4
    };

    let mut all: Vec<Value> = Vec::new();
    // Early-exit bound: the timestamp of the `needed`-th smallest collected
    // row (a bounded max-heap over the best `needed` candidates). Bounding on
    // the max of ALL collected rows would never fire on contiguous data —
    // consecutive blocks share boundary seconds, so the running max always
    // reaches into the next block. The k-th rank stays anchored instead.
    let mut top_ts: std::collections::BinaryHeap<i64> = std::collections::BinaryHeap::new();

    let mut idx = 0;
    while idx < metadata.len() {
        // Adaptive chunk: just enough blocks (by stored row counts — an upper
        // bound on valid rows) to plausibly satisfy `needed`, capped at
        // `chunk_size` for memory. A limit=1 probe then touches one block
        // instead of a full parallel sweep.
        let mut chunk_end = idx;
        let mut rows_estimate = 0usize;
        while chunk_end < metadata.len() && (chunk_end - idx) < chunk_size {
            rows_estimate += metadata[chunk_end].rows as usize;
            chunk_end += 1;
            if rows_estimate >= needed.saturating_mul(2) {
                break;
            }
        }
        let chunk = &metadata[idx..chunk_end];
        idx = chunk_end;

        if top_ts.len() >= needed {
            // Blocks are ordered by min_ts: a block starting strictly past the
            // needed-th ranked row cannot change the first `needed` rows, and
            // neither can any block after it.
            let bound = top_ts.peek().copied().unwrap_or(i64::MAX);
            let next_can_contribute = ts_col.is_some()
                && chunk
                    .first()
                    .map(|meta| meta.min_ts <= bound)
                    .unwrap_or(false);
            if !next_can_contribute {
                break;
            }
        }

        let use_parallel = chunk.len() >= 4 && query_threads > 1;
        let per_block: Result<Vec<Vec<Value>>> = if use_parallel {
            chunk
                .par_iter()
                .map(|meta| {
                    block_to_json(BlockQuery {
                        snap: &snap,
                        schema,
                        table_hash,
                        block_id: meta.block,
                        block_min_ts: meta.min_ts,
                        block_max_ts: meta.max_ts,
                        start_ts,
                        end_ts,
                    })
                })
                .collect()
        } else {
            chunk
                .iter()
                .map(|meta| {
                    block_to_json(BlockQuery {
                        snap: &snap,
                        schema,
                        table_hash,
                        block_id: meta.block,
                        block_min_ts: meta.min_ts,
                        block_max_ts: meta.max_ts,
                        start_ts,
                        end_ts,
                    })
                })
                .collect()
        };

        for batch in per_block? {
            for row in batch {
                let ts = row_ts(&row);
                if top_ts.len() < needed {
                    top_ts.push(ts);
                } else if let Some(mut worst) = top_ts.peek_mut() {
                    if ts < *worst {
                        *worst = ts;
                    }
                }
                all.push(row);
            }
        }
    }

    // Cut to the requested window in TOTAL order (ts, id). Sorting by ts alone
    // leaves same-second rows in nondeterministic relative order, which makes
    // offset pagination drop/duplicate tied rows across requests.
    let id_col = schema.id_column.clone();
    let row_id = move |row: &Value| -> u64 {
        match row.get(&id_col) {
            Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
            Some(Value::String(s)) => s.parse().unwrap_or(0),
            _ => 0,
        }
    };
    all.sort_by_cached_key(|row| (row_ts(row), row_id(row)));

    Ok(all.into_iter().skip(offset).take(limit).collect())
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

    // Try common datetime formats (with time component).
    let datetime_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"];

    for format in &datetime_formats {
        if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(timestamp, format) {
            return Ok(naive_dt.and_utc().timestamp_millis());
        }
    }

    // Date-only ("%Y-%m-%d") must go through NaiveDate; NaiveDateTime rejects
    // it because it has no time component. Mirrors ingestion::parse_timestamp
    // so query-time and ingest-time accept the same surface.
    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(timestamp, "%Y-%m-%d") {
        return Ok(naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis());
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

/// Walk the block index for `table_hash` and collect metadata for every
/// block whose stored \[min_ts, max_ts\] overlaps the requested range.
///
/// Block-index keys are laid out as `[table_hash:u32][b'B'][min_ts:i64][block_id]`
/// in big-endian; iteration in `Forward` direction therefore yields blocks
/// in ascending `min_ts` order, and we can short-circuit as soon as a key's
/// `min_ts` exceeds the requested `end_ts`.
fn scan_block_metadata(
    snap: &rocksdb::Snapshot<'_>,
    table_hash: u32,
    start_ts: i64,
    end_ts: i64,
) -> Vec<refs::BlockMeta> {
    let mut start_key = Vec::with_capacity(5);
    start_key.extend_from_slice(&table_hash.to_be_bytes());
    start_key.push(b'B');

    let mut opts = ReadOptions::default();
    opts.set_verify_checksums(false);
    opts.fill_cache(true);
    opts.set_readahead_size(4 * 1024 * 1024);

    let iter = snap.iterator_opt(IteratorMode::From(&start_key, Direction::Forward), opts);
    let mut result: Vec<refs::BlockMeta> = Vec::new();

    for item in iter {
        let (key, value) = match item {
            Ok(p) => p,
            Err(_) => continue,
        };
        // Table-hash boundary FIRST: keys are hash-prefixed, so the first key
        // of another table ends this table's index range. Checking the 'B'
        // marker first would `continue` past foreign tables' keys and walk
        // the entire remaining keyspace on unbounded-end queries.
        if key.len() < 4 {
            continue;
        }
        let key_table_hash = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if key_table_hash != table_hash {
            break;
        }
        // Markers sort 'B' < 'D' < 'O' and the scan starts at the 'B' range:
        // the first non-'B' key ends it. `continue` here would walk the whole
        // 'D' range, materializing every serialized block along the way.
        if key.len() < 5 || key[4] != b'B' {
            break;
        }

        // The key carries the block's min_ts; once that's past end_ts, every
        // subsequent block in the iterator is also out of range.
        if key.len() >= 13 {
            let key_min_ts = i64::from_be_bytes([
                key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12],
            ]);
            if key_min_ts > end_ts {
                break;
            }
        }

        let Ok(meta) = refs::parse_block_index_value(&value) else {
            continue;
        };

        if meta.max_ts < start_ts || meta.min_ts > end_ts {
            continue;
        }

        result.push(meta);
    }

    result
}

struct BlockQuery<'a> {
    snap: &'a rocksdb::Snapshot<'a>,
    schema: &'a Schema,
    table_hash: u32,
    block_id: u64,
    block_min_ts: i64,
    block_max_ts: i64,
    start_ts: i64,
    end_ts: i64,
}

/// Fetch and deserialize a block by its numeric id.
fn fetch_block(
    snap: &rocksdb::Snapshot<'_>,
    table_hash: u32,
    block_id: u64,
) -> Result<Option<ColumnBlock>> {
    let mut opts = ReadOptions::default();
    opts.set_verify_checksums(false);
    opts.fill_cache(true);
    match snap.get_opt(refs::block_data_key(table_hash, block_id), opts)? {
        Some(data) => Ok(Some(ColumnBlock::deserialize(&data)?)),
        None => Ok(None),
    }
}

/// Compute the per-row validity bitmap for `block`.
///
/// A row is live unless its position appears in the block's override set —
/// the (usually absent) record of positions superseded by later REPLACE
/// ingests. ONE point read per block, no per-row lookups: this is what makes
/// large range scans cheap.
fn block_validity_bitmap(
    snap: &rocksdb::Snapshot<'_>,
    table_hash: u32,
    block: &ColumnBlock,
    block_id: u64,
) -> Result<Vec<bool>> {
    if block.row_count == 0 {
        return Ok(Vec::new());
    }
    let overrides = load_overrides(snap, table_hash, block_id)?;
    let mut valid = vec![true; block.row_count];
    for pos in overrides {
        if let Some(slot) = valid.get_mut(pos as usize) {
            *slot = false;
        }
    }
    Ok(valid)
}

/// Materialize the indices selected by both the timestamp range and the
/// validity bitmap. Used by paths that need an `&[usize]` (`to_*_filtered`).
fn intersect_indices(
    block: &ColumnBlock,
    schema: &Schema,
    validity: &[bool],
    needs_ts_filter: bool,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<usize>> {
    if needs_ts_filter {
        let ts_candidates = block.filter_by_timestamp(schema, start_ts, end_ts)?;
        Ok(ts_candidates.into_iter().filter(|&i| validity[i]).collect())
    } else {
        Ok((0..block.row_count).filter(|&i| validity[i]).collect())
    }
}

/// Per-block JSON serializer used by `execute_query`.
///
/// Two paths, both running on a precomputed validity bitmap:
///   * **slice fast path** — block lies fully inside the timestamp range
///     AND every row is the live one. `to_json_slice` walks columns
///     contiguously without per-row index indirection.
///   * **filtered path** — anything else; the surviving indices are
///     materialized once and passed to `to_json_filtered`.
fn block_to_json(query: BlockQuery<'_>) -> Result<Vec<Value>> {
    let block = match fetch_block(query.snap, query.table_hash, query.block_id)? {
        Some(b) => b,
        None => return Ok(Vec::new()),
    };
    let validity = block_validity_bitmap(query.snap, query.table_hash, &block, query.block_id)?;
    let valid_count = validity.iter().filter(|&&v| v).count();
    if valid_count == 0 {
        return Ok(Vec::new());
    }

    let needs_ts_filter = query.block_min_ts < query.start_ts || query.block_max_ts > query.end_ts;
    if !needs_ts_filter && valid_count == block.row_count {
        return block.to_json_slice(query.schema, 0, block.row_count);
    }

    let indices = intersect_indices(
        &block,
        query.schema,
        &validity,
        needs_ts_filter,
        query.start_ts,
        query.end_ts,
    )?;
    if indices.is_empty() {
        return Ok(Vec::new());
    }
    block.to_json_filtered(query.schema, &indices)
}

/// Per-block CSV serializer used by `execute_query_csv`. Three paths, each
/// doing **exactly one** full-column decompression:
///
///   1. `to_csv` slice — block fully in range AND every row live.
///   2. `to_csv_filtered` — block fully in range, some rows stale.
///   3. `to_csv_query` single-pass — timestamp range trims the block;
///      walks rows once with inline `validity[i] && ts in [start,end]`
///      checks, avoiding a separate `filter_by_timestamp` pre-scan.
fn block_to_csv(query: BlockQuery<'_>) -> Result<String> {
    let block = match fetch_block(query.snap, query.table_hash, query.block_id)? {
        Some(b) => b,
        None => return Ok(String::new()),
    };
    let validity = block_validity_bitmap(query.snap, query.table_hash, &block, query.block_id)?;
    let valid_count = validity.iter().filter(|&&v| v).count();
    if valid_count == 0 {
        return Ok(String::new());
    }

    let needs_ts_filter = query.block_min_ts < query.start_ts || query.block_max_ts > query.end_ts;

    if !needs_ts_filter {
        if valid_count == block.row_count {
            return block.to_csv(query.schema, 0, block.row_count);
        }
        let indices: Vec<usize> = (0..block.row_count).filter(|&i| validity[i]).collect();
        return block.to_csv_filtered(query.schema, &indices);
    }

    block.to_csv_query(query.schema, query.start_ts, query.end_ts, &validity)
}

/// Per-block Arrow serializer used by `execute_query_arrow`. Same
/// three-path dispatch as `block_to_csv`.
fn block_to_arrow(query: BlockQuery<'_>) -> Result<Option<RecordBatch>> {
    let block = match fetch_block(query.snap, query.table_hash, query.block_id)? {
        Some(b) => b,
        None => return Ok(None),
    };
    let validity = block_validity_bitmap(query.snap, query.table_hash, &block, query.block_id)?;
    let valid_count = validity.iter().filter(|&&v| v).count();
    if valid_count == 0 {
        return Ok(None);
    }

    let needs_ts_filter = query.block_min_ts < query.start_ts || query.block_max_ts > query.end_ts;

    let batch = if !needs_ts_filter {
        if valid_count == block.row_count {
            block.to_arrow(query.schema, 0, block.row_count)?
        } else {
            let indices: Vec<usize> = (0..block.row_count).filter(|&i| validity[i]).collect();
            block.to_arrow_filtered(query.schema, &indices)?
        }
    } else {
        block.to_arrow_query(query.schema, query.start_ts, query.end_ts, &validity)?
    };

    if batch.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(batch))
    }
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
    let start_ts = start
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(0);
    let end_ts = end
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(i64::MAX);
    let limit = limit.unwrap_or(1000);
    let offset = offset.unwrap_or(0);

    let table_hash = calculate_table_hash(table);
    // One snapshot for the whole query — see execute_query for rationale.
    let snap = db.snapshot();
    let metadata = scan_block_metadata(&snap, table_hash, start_ts, end_ts);

    use rayon::prelude::*;
    let use_parallel = metadata.len() >= 4 && query_threads > 1;

    let per_block: Result<Vec<String>> = if use_parallel {
        metadata
            .par_iter()
            .map(|meta| {
                block_to_csv(BlockQuery {
                    snap: &snap,
                    schema,
                    table_hash,
                    block_id: meta.block,
                    block_min_ts: meta.min_ts,
                    block_max_ts: meta.max_ts,
                    start_ts,
                    end_ts,
                })
            })
            .collect()
    } else {
        metadata
            .iter()
            .map(|meta| {
                block_to_csv(BlockQuery {
                    snap: &snap,
                    schema,
                    table_hash,
                    block_id: meta.block,
                    block_min_ts: meta.min_ts,
                    block_max_ts: meta.max_ts,
                    start_ts,
                    end_ts,
                })
            })
            .collect()
    };

    // Each per-block chunk is rows joined by '\n' (trailing '\n'). For
    // no-pagination the cheapest thing is to return the chunks unchanged
    // and let the caller concatenate them with the header. With pagination
    // we re-emit only the requested row window.
    let chunks = per_block?;
    if offset == 0 && limit == usize::MAX {
        return Ok(chunks);
    }

    let combined: String = chunks.into_iter().collect();
    let mut out = String::with_capacity(combined.len());
    let take_end = offset.saturating_add(limit);
    for (seen, line) in combined.lines().enumerate() {
        if seen >= take_end {
            break;
        }
        if seen >= offset {
            out.push_str(line);
            out.push('\n');
        }
    }
    Ok(vec![out])
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
    let start_ts = start
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(0);
    let end_ts = end
        .as_deref()
        .map(parse_query_timestamp)
        .transpose()?
        .unwrap_or(i64::MAX);
    let limit = limit.unwrap_or(1000);
    let offset = offset.unwrap_or(0);

    let table_hash = calculate_table_hash(table);
    // One snapshot for the whole query — see execute_query for rationale.
    let snap = db.snapshot();
    let metadata = scan_block_metadata(&snap, table_hash, start_ts, end_ts);

    use rayon::prelude::*;
    let use_parallel = metadata.len() >= 4 && query_threads > 1;

    let per_block: Result<Vec<Option<RecordBatch>>> = if use_parallel {
        metadata
            .par_iter()
            .map(|meta| {
                block_to_arrow(BlockQuery {
                    snap: &snap,
                    schema,
                    table_hash,
                    block_id: meta.block,
                    block_min_ts: meta.min_ts,
                    block_max_ts: meta.max_ts,
                    start_ts,
                    end_ts,
                })
            })
            .collect()
    } else {
        metadata
            .iter()
            .map(|meta| {
                block_to_arrow(BlockQuery {
                    snap: &snap,
                    schema,
                    table_hash,
                    block_id: meta.block,
                    block_min_ts: meta.min_ts,
                    block_max_ts: meta.max_ts,
                    start_ts,
                    end_ts,
                })
            })
            .collect()
    };

    let batches: Vec<RecordBatch> = per_block?.into_iter().flatten().collect();
    if batches.is_empty() || (offset == 0 && limit == usize::MAX) {
        return Ok(batches);
    }

    // Apply global skip/take across batches. RecordBatch::slice is zero-copy
    // — it adjusts offsets without copying buffers — so paged Arrow output
    // stays cheap.
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut to_skip = offset;
    let mut to_take = limit;
    for batch in batches {
        if to_take == 0 {
            break;
        }
        let rows = batch.num_rows();
        if to_skip >= rows {
            to_skip -= rows;
            continue;
        }
        let take = (rows - to_skip).min(to_take);
        if to_skip == 0 && take == rows {
            out.push(batch);
        } else {
            out.push(batch.slice(to_skip, take));
        }
        to_take -= take;
        to_skip = 0;
    }
    Ok(out)
}
#[cfg(test)]
#[path = "query_test.rs"]
mod query_test;
