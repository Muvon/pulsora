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

//! Regression tests for limited queries across many blocks.
//!
//! The original query path materialized EVERY block before applying
//! limit/offset, so a `limit=1` probe on a large table decompressed the whole
//! table (and starved readers during heavy ingestion). These tests pin the
//! correctness contract of the early-exit implementation — including
//! REPLACE-stale rows and overlapping block time ranges — and the client-side
//! tail-read pattern (count, then offset/limit, reverse on the client), which
//! is exactly the territory the original suite did not cover.

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use tempfile::TempDir;

const BASE_TS: i64 = 1704067200000; // 2024-01-01 00:00:00 UTC (ms)

async fn create_test_engine() -> (StorageEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp_dir)
}

/// Ingest one batch of rows (id, ts ms, value) and flush it into its own block.
async fn ingest_block(engine: &StorageEngine, table: &str, rows: &[(u64, i64, i64)]) {
    let mut csv = String::from("id,timestamp,value\n");
    for (id, ts, value) in rows {
        csv.push_str(&format!("{},{},{}\n", id, ts, value));
    }
    engine.ingest_csv(table, csv).await.unwrap();
    engine.flush_table(table).await.unwrap();
}

fn ids(results: &[serde_json::Value]) -> Vec<u64> {
    results
        .iter()
        .map(|r| r.get("id").unwrap().as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn test_limited_query_correct_across_many_blocks() {
    let (engine, _temp) = create_test_engine().await;
    let table = "many_blocks";

    // 20 flushed blocks of 50 rows each, strictly increasing timestamps
    for block in 0..20i64 {
        let rows: Vec<(u64, i64, i64)> = (1..=50)
            .map(|i| {
                let id = (block * 50 + i) as u64;
                (id, BASE_TS + (block * 50 + i) * 1000, i)
            })
            .collect();
        ingest_block(&engine, table, &rows).await;
    }

    // A small limit must return exactly the globally-first rows
    let results = engine
        .query(table, None, None, Some(3), None)
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![1, 2, 3]);

    // Offset + limit deep into later blocks
    let results = engine
        .query(table, None, None, Some(5), Some(997))
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![998, 999, 1000]);
}

#[tokio::test]
async fn test_limited_query_with_replace_stale_rows() {
    let (engine, _temp) = create_test_engine().await;
    let table = "replace_stale";

    // Block 1: ids 1..=50
    let rows: Vec<(u64, i64, i64)> = (1..=50)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 1))
        .collect();
    ingest_block(&engine, table, &rows).await;

    // Block 2: REPLACE ids 1..=10 (same timestamps — stale rows stay in block 1)
    let rows: Vec<(u64, i64, i64)> = (1..=10)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 2))
        .collect();
    ingest_block(&engine, table, &rows).await;

    // The first 10 rows must all be the replaced (value=2) versions, exactly once
    let results = engine
        .query(table, None, None, Some(10), None)
        .await
        .unwrap();
    assert_eq!(ids(&results), (1..=10).collect::<Vec<u64>>());
    for row in &results {
        assert_eq!(row.get("value").unwrap().as_i64().unwrap(), 2);
    }

    // Total live rows unchanged
    let results = engine
        .query(table, None, None, Some(1000), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 50);
}

#[tokio::test]
async fn test_limited_query_with_overlapping_blocks() {
    let (engine, _temp) = create_test_engine().await;
    let table = "overlap_blocks";

    // Block 1: newer rows first (ids 100.., later timestamps)
    let rows: Vec<(u64, i64, i64)> = (1..=50)
        .map(|i| (100 + i, BASE_TS + (100 + i as i64) * 1000, 1))
        .collect();
    ingest_block(&engine, table, &rows).await;

    // Block 2: OLDER rows ingested later (gap-fill pattern) — this block's
    // time range sits entirely before block 1 in the index order by min_ts,
    // but arrives second. Early exit must still see them.
    let rows: Vec<(u64, i64, i64)> = (1..=50)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 2))
        .collect();
    ingest_block(&engine, table, &rows).await;

    // Globally-first rows are the old gap-filled ones
    let results = engine
        .query(table, None, None, Some(3), None)
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![1, 2, 3]);
}

#[tokio::test]
async fn test_tail_read_via_count_and_offset() {
    let (engine, _temp) = create_test_engine().await;
    let table = "tail_read";

    for block in 0..10i64 {
        let rows: Vec<(u64, i64, i64)> = (1..=50)
            .map(|i| {
                let id = (block * 50 + i) as u64;
                (id, BASE_TS + (block * 50 + i) * 1000, i)
            })
            .collect();
        ingest_block(&engine, table, &rows).await;
    }

    // The client pattern for "newest N rows": count, then offset = count - N,
    // ascending query, reverse client-side.
    let count = engine.get_table_count(table).await.unwrap() as usize;
    assert_eq!(count, 500);

    let n = 3;
    let results = engine
        .query(table, None, None, Some(n), Some(count - n))
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![498, 499, 500]);
}

#[tokio::test]
async fn test_count_is_live_rows_under_replace() {
    let (engine, _temp) = create_test_engine().await;
    let table = "count_replace";

    // 50 rows, then REPLACE 10 of them — count must NOT include stale copies,
    // otherwise the count-then-offset tail pattern lands on the wrong rows.
    let rows: Vec<(u64, i64, i64)> = (1..=50)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 1))
        .collect();
    ingest_block(&engine, table, &rows).await;
    let rows: Vec<(u64, i64, i64)> = (1..=10)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 2))
        .collect();
    ingest_block(&engine, table, &rows).await;

    let count = engine.get_table_count(table).await.unwrap() as usize;
    assert_eq!(count, 50);

    // Tail read stays exact after replaces
    let results = engine
        .query(table, None, None, Some(2), Some(count - 2))
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![49, 50]);
}

#[tokio::test]
async fn test_pagination_deterministic_with_tied_timestamps() {
    let (engine, _temp) = create_test_engine().await;
    let table = "tied_ts";

    // Real trade shape: MANY rows share the same second, spread across blocks.
    // Pagination across separate requests must neither drop nor duplicate
    // tied rows — requires a total (ts, id) order, not ts-only.
    for block in 0..10i64 {
        let rows: Vec<(u64, i64, i64)> = (1..=50)
            .map(|i| {
                let id = (block * 50 + i) as u64;
                // Only ~5 distinct seconds across 500 rows
                (id, BASE_TS + (id as i64 / 100) * 1000, i)
            })
            .collect();
        ingest_block(&engine, table, &rows).await;
    }

    // Page through with a small page size and collect all ids
    let mut seen = Vec::new();
    let mut offset = 0;
    loop {
        let page = engine
            .query(table, None, None, Some(37), Some(offset))
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        offset += page.len();
        seen.extend(ids(&page));
    }

    let mut sorted = seen.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(seen.len(), 500, "pages dropped or duplicated tied rows");
    assert_eq!(sorted.len(), 500, "duplicate ids across page boundaries");
    assert_eq!(sorted, (1..=500).collect::<Vec<u64>>());
}

#[tokio::test]
async fn test_limited_query_with_contiguous_block_boundaries() {
    let (engine, _temp) = create_test_engine().await;
    let table = "contiguous";

    // Blocks share boundary seconds (flushes split mid-second) — the
    // early-exit bound must anchor on the needed-th rank, not the max of all
    // collected rows, or it degrades to a full scan on contiguous data.
    for block in 0..20i64 {
        let rows: Vec<(u64, i64, i64)> = (1..=50)
            .map(|i| {
                let id = (block * 50 + i) as u64;
                // 10 rows per second, seconds run continuously across blocks
                (id, BASE_TS + (id as i64 / 10) * 1000, i)
            })
            .collect();
        ingest_block(&engine, table, &rows).await;
    }

    let results = engine
        .query(table, None, None, Some(1), None)
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![1]);

    let results = engine
        .query(table, None, None, Some(4), Some(3))
        .await
        .unwrap();
    assert_eq!(ids(&results), vec![4, 5, 6, 7]);
}

#[tokio::test]
async fn test_no_per_row_index_entries() {
    let (engine, _temp) = create_test_engine().await;
    let table = "compact_refs";

    let rows: Vec<(u64, i64, i64)> = (1..=50)
        .map(|i| (i, BASE_TS + i as i64 * 1000, 1))
        .collect();
    ingest_block(&engine, table, &rows).await;

    // The whole point of the block-level design: NO per-row index entries.
    // The only allowed entries are per-block ('B' index, 'D' data, 'O'
    // overrides), schema/meta keys, and the block sequence counter.
    let mut blocks = 0;
    for item in engine.db.iterator(rocksdb::IteratorMode::Start) {
        let (key, _value) = item.unwrap();
        // Per-block keys ('B'/'D'/'O') are 13 or 21 bytes and meta keys
        // start with '_' — any other 12-byte key is a per-row entry.
        if key.len() == 12 && key[0] != b'_' {
            panic!("unexpected per-row index entry: {:?}", key);
        }
        if key.len() == 21 && key[4] == b'B' {
            blocks += 1;
        }
    }
    assert!(blocks >= 1, "expected at least one block index entry");
}
