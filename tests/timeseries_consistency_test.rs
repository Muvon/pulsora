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

//! Time-series data consistency suite.
//!
//! Every test maintains a ground-truth oracle `HashMap<id, OracleRow>` and,
//! after each manipulation sequence, asserts that *all five* read paths
//! agree with the oracle:
//!
//!   1. `query()`                  — JSON via DB + buffer merge
//!   2. `query_csv()`              — columnar CSV (flushes first)
//!   3. `query_arrow()`            — columnar Arrow IPC (flushes first)
//!   4. `get_row_by_id_json()`     — per-row probe of buffer then DB
//!   5. `get_table_count()`        — logical row count
//!
//! Any disagreement between these paths is a consistency bug regardless of
//! whether the data values themselves are correct, so the assertion is
//! strong: same set of IDs, same values for each, identical between paths.

#![allow(clippy::needless_range_loop)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use prost::Message;
use pulsora::config::Config;
use pulsora::storage::ingestion::{ProtoBatch, ProtoRow};
use pulsora::storage::StorageEngine;
use tempfile::TempDir;

// --------------------------------------------------------------------------
// Oracle model
// --------------------------------------------------------------------------

/// Schema used by every test in this file:
///   id        — Id (auto or user)
///   timestamp — Integer (ms since epoch; named "timestamp" so the engine
///               sets it as the table's timestamp column)
///   value     — Integer
///   tag       — String
#[derive(Clone, PartialEq, Eq, Debug)]
struct OracleRow {
    timestamp: i64,
    value: i64,
    tag: String,
}

type Oracle = HashMap<u64, OracleRow>;

// --------------------------------------------------------------------------
// Engine helpers
// --------------------------------------------------------------------------

fn default_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_string_lossy().to_string();
    // Deterministic: no time-based background flush.
    config.storage.flush_interval_ms = 0;
    config
}

async fn fresh_engine() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let engine = StorageEngine::new(&default_config(&temp)).await.unwrap();
    (engine, temp)
}

// --------------------------------------------------------------------------
// CSV construction
// --------------------------------------------------------------------------

fn build_csv(ids: &[u64], oracle: &Oracle) -> String {
    let mut csv = String::from("id,timestamp,value,tag\n");
    for id in ids {
        let r = oracle.get(id).expect("oracle has id");
        csv.push_str(&format!("{},{},{},{}\n", id, r.timestamp, r.value, r.tag));
    }
    csv
}

// --------------------------------------------------------------------------
// JSON / value coercion
// --------------------------------------------------------------------------

fn json_id(v: &serde_json::Value) -> u64 {
    v.get("id").expect("id field").as_u64().expect("id is u64")
}

/// Pull an integer-shaped field that may come back as JSON Number OR
/// as an RFC3339 string (the columnar-to-JSON path emits Timestamp-typed
/// columns as strings). Our `timestamp` column is Integer-typed, so this
/// is just defensive — but tests pass either way.
fn json_i64(v: &serde_json::Value, field: &str) -> i64 {
    let val = v
        .get(field)
        .unwrap_or_else(|| panic!("missing field {field}"));
    if let Some(n) = val.as_i64() {
        return n;
    }
    if let Some(s) = val.as_str() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return dt.timestamp_millis();
        }
        if let Ok(n) = s.parse::<i64>() {
            return n;
        }
    }
    panic!("field {field} not int-coercible: {:?}", val);
}

fn json_str<'a>(v: &'a serde_json::Value, field: &str) -> &'a str {
    v.get(field)
        .unwrap_or_else(|| panic!("missing field {field}"))
        .as_str()
        .unwrap_or_else(|| panic!("field {field} not str: {:?}", v.get(field)))
}

// --------------------------------------------------------------------------
// Cross-read-path assertion
// --------------------------------------------------------------------------

/// The crown jewel: assert every read path returns exactly the oracle.
async fn assert_oracle(engine: &StorageEngine, table: &str, oracle: &Oracle) {
    let n = oracle.len();
    let limit = n + 100;

    // ------------------------------------------------------------------
    // 1. query() JSON — primary path (DB + buffer merge)
    // ------------------------------------------------------------------
    let json = engine
        .query(table, None, None, Some(limit), None)
        .await
        .expect("query");
    assert_eq!(
        json.len(),
        n,
        "query() returned {} rows, oracle has {}",
        json.len(),
        n
    );
    let json_by_id: HashMap<u64, &serde_json::Value> =
        json.iter().map(|r| (json_id(r), r)).collect();
    assert_eq!(
        json_by_id.len(),
        n,
        "query() produced duplicate IDs (got {} unique of {} rows)",
        json_by_id.len(),
        json.len()
    );
    for (id, expected) in oracle {
        let row = json_by_id
            .get(id)
            .unwrap_or_else(|| panic!("query() missing id={}", id));
        assert_eq!(
            json_i64(row, "timestamp"),
            expected.timestamp,
            "query() timestamp mismatch id={}",
            id
        );
        assert_eq!(
            json_i64(row, "value"),
            expected.value,
            "query() value mismatch id={}",
            id
        );
        assert_eq!(
            json_str(row, "tag"),
            expected.tag,
            "query() tag mismatch id={}",
            id
        );
    }

    // ------------------------------------------------------------------
    // 2. get_table_count() — must match the oracle exactly
    // ------------------------------------------------------------------
    let count = engine.get_table_count(table).await.unwrap();
    assert_eq!(
        count as usize, n,
        "get_table_count() = {}, oracle has {}",
        count, n
    );

    // ------------------------------------------------------------------
    // 3. get_row_by_id_json — per-ID probe
    // ------------------------------------------------------------------
    for (id, expected) in oracle {
        let row = engine
            .get_row_by_id_json(table, *id)
            .await
            .expect("get_row_by_id_json call")
            .unwrap_or_else(|| panic!("get_row_by_id_json: id={} not retrievable", id));
        assert_eq!(json_id(&row), *id, "get_row_by_id returned wrong id");
        assert_eq!(
            json_i64(&row, "timestamp"),
            expected.timestamp,
            "get_row_by_id timestamp mismatch id={}",
            id
        );
        assert_eq!(
            json_i64(&row, "value"),
            expected.value,
            "get_row_by_id value mismatch id={}",
            id
        );
        assert_eq!(
            json_str(&row, "tag"),
            expected.tag,
            "get_row_by_id tag mismatch id={}",
            id
        );
    }

    // ------------------------------------------------------------------
    // 4. query_csv — parse and compare
    // ------------------------------------------------------------------
    let csv = engine
        .query_csv(table, None, None, Some(limit), None)
        .await
        .unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(
        lines.len(),
        n + 1,
        "query_csv: expected {} lines (header + {} rows), got {}",
        n + 1,
        n,
        lines.len()
    );

    let header: Vec<&str> = lines[0].split(',').collect();
    let id_pos = header.iter().position(|c| *c == "id").expect("csv id col");
    let ts_pos = header
        .iter()
        .position(|c| *c == "timestamp")
        .expect("csv timestamp col");
    let val_pos = header
        .iter()
        .position(|c| *c == "value")
        .expect("csv value col");
    let tag_pos = header
        .iter()
        .position(|c| *c == "tag")
        .expect("csv tag col");

    let mut csv_by_id: HashMap<u64, (i64, i64, String)> = HashMap::new();
    for line in &lines[1..] {
        let cols: Vec<&str> = line.split(',').collect();
        let id: u64 = cols[id_pos].parse().expect("csv id parse");
        let ts: i64 = cols[ts_pos].parse().expect("csv ts parse");
        let v: i64 = cols[val_pos].parse().expect("csv value parse");
        let tag = cols[tag_pos].to_string();
        csv_by_id.insert(id, (ts, v, tag));
    }
    assert_eq!(
        csv_by_id.len(),
        n,
        "query_csv: distinct IDs {} != oracle {}",
        csv_by_id.len(),
        n
    );
    for (id, expected) in oracle {
        let (ts, v, t) = csv_by_id
            .get(id)
            .unwrap_or_else(|| panic!("query_csv missing id={}", id));
        assert_eq!(*ts, expected.timestamp, "csv ts mismatch id={}", id);
        assert_eq!(*v, expected.value, "csv value mismatch id={}", id);
        assert_eq!(t.as_str(), expected.tag, "csv tag mismatch id={}", id);
    }

    // ------------------------------------------------------------------
    // 5. query_arrow — decode IPC stream and compare
    // ------------------------------------------------------------------
    let arrow_bytes = engine
        .query_arrow(table, None, None, Some(limit), None)
        .await
        .unwrap();
    let cursor = std::io::Cursor::new(&arrow_bytes);
    let reader = StreamReader::try_new(cursor, None).expect("arrow stream");
    let mut arrow_by_id: HashMap<u64, (i64, i64, String)> = HashMap::new();
    for batch in reader {
        let batch = batch.unwrap();
        let id_col = batch.column_by_name("id").expect("arrow id col");
        let ts_col = batch
            .column_by_name("timestamp")
            .expect("arrow timestamp col");
        let val_col = batch.column_by_name("value").expect("arrow value col");
        let tag_col = batch.column_by_name("tag").expect("arrow tag col");

        let ids: Vec<u64> = if let Some(arr) = id_col.as_any().downcast_ref::<UInt64Array>() {
            arr.values().to_vec()
        } else if let Some(arr) = id_col.as_any().downcast_ref::<Int64Array>() {
            arr.values().iter().map(|v| *v as u64).collect()
        } else {
            panic!("arrow id column type unexpected");
        };
        let tss: Vec<i64> = ts_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("arrow ts is Int64")
            .values()
            .to_vec();
        let vals: Vec<i64> = val_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("arrow value is Int64")
            .values()
            .to_vec();
        let tag_arr = tag_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arrow tag is Utf8");

        for i in 0..batch.num_rows() {
            arrow_by_id.insert(ids[i], (tss[i], vals[i], tag_arr.value(i).to_string()));
        }
    }
    assert_eq!(
        arrow_by_id.len(),
        n,
        "query_arrow rows {} != oracle {}",
        arrow_by_id.len(),
        n
    );
    for (id, expected) in oracle {
        let (ts, v, t) = arrow_by_id
            .get(id)
            .unwrap_or_else(|| panic!("query_arrow missing id={}", id));
        assert_eq!(*ts, expected.timestamp, "arrow ts mismatch id={}", id);
        assert_eq!(*v, expected.value, "arrow value mismatch id={}", id);
        assert_eq!(t.as_str(), expected.tag, "arrow tag mismatch id={}", id);
    }
}

// --------------------------------------------------------------------------
// Deterministic RNG (LCG) — no external dep
// --------------------------------------------------------------------------

struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + (self.next() % (hi - lo))
    }
}

// ==========================================================================
// 1. Sustained append-only — the bread-and-butter TS workload
// ==========================================================================

#[tokio::test]
async fn ts_consistency_append_only_sustained() {
    let (engine, _temp) = fresh_engine().await;
    let table = "append_only";
    let mut oracle = Oracle::new();

    // 10 batches × 100 monotonically-timestamped appends = 1000 rows
    for batch in 0..10_u64 {
        let mut ids = Vec::new();
        for i in 0..100_u64 {
            let id = batch * 100 + i + 1;
            oracle.insert(
                id,
                OracleRow {
                    timestamp: 1_704_067_200_000 + (id as i64) * 1000,
                    value: id as i64 * 7,
                    tag: format!("b{}r{}", batch, i),
                },
            );
            ids.push(id);
        }
        engine
            .ingest_csv(table, build_csv(&ids, &oracle))
            .await
            .unwrap();
    }

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 2. Late-arriving data — newer ingest carries older timestamps
// ==========================================================================

#[tokio::test]
async fn ts_consistency_late_arriving_data() {
    let (engine, _temp) = fresh_engine().await;
    let table = "late_arriving";
    let mut oracle = Oracle::new();

    // First wave: "recent" timestamps (later epoch range)
    for i in 1..=50_u64 {
        oracle.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_300_000 + (i as i64) * 1000,
                value: i as i64,
                tag: "recent".to_string(),
            },
        );
    }
    let ids: Vec<u64> = (1..=50).collect();
    engine
        .ingest_csv(table, build_csv(&ids, &oracle))
        .await
        .unwrap();

    // Second wave: "late" timestamps (older than the first wave)
    for i in 51..=100_u64 {
        oracle.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_200_000 + (i as i64 - 51) * 1000,
                value: i as i64,
                tag: "late".to_string(),
            },
        );
    }
    let ids: Vec<u64> = (51..=100).collect();
    engine
        .ingest_csv(table, build_csv(&ids, &oracle))
        .await
        .unwrap();

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 3. Bursty REPLACE on recent rows — typical correction workload
// ==========================================================================

#[tokio::test]
async fn ts_consistency_bursty_replace_recent() {
    let (engine, _temp) = fresh_engine().await;
    let table = "bursty_replace";
    let mut oracle = Oracle::new();

    // Initial dataset: 100 rows
    for i in 1..=100_u64 {
        oracle.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_200_000 + (i as i64) * 1000,
                value: 0,
                tag: "init".to_string(),
            },
        );
    }
    let ids: Vec<u64> = (1..=100).collect();
    engine
        .ingest_csv(table, build_csv(&ids, &oracle))
        .await
        .unwrap();

    // Apply 10 rounds of corrections to the last 20 rows (id 81..=100).
    for round in 1..=10_i64 {
        let mut update_ids = Vec::new();
        for i in 81..=100_u64 {
            let r = oracle.get_mut(&i).unwrap();
            r.value = round * 100 + i as i64;
            r.tag = format!("upd{}", round);
            update_ids.push(i);
        }
        engine
            .ingest_csv(table, build_csv(&update_ids, &oracle))
            .await
            .unwrap();
    }

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 4. High-cardinality series — 1000 distinct IDs, sparse per series
// ==========================================================================

#[tokio::test]
async fn ts_consistency_high_cardinality_series() {
    let (engine, _temp) = fresh_engine().await;
    let table = "high_card";
    let mut oracle = Oracle::new();

    // 1000 IDs in one shot. Tags repeat every 50 IDs to exercise the
    // string-dictionary compression path.
    for i in 1..=1000_u64 {
        oracle.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_200_000 + (i as i64) * 100,
                value: i as i64 * 13,
                tag: format!("series_{}", i % 50),
            },
        );
    }
    let ids: Vec<u64> = (1..=1000).collect();
    engine
        .ingest_csv(table, build_csv(&ids, &oracle))
        .await
        .unwrap();

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 5. Sustained REPLACE on the same ID — latest-value semantics
// ==========================================================================

#[tokio::test]
async fn ts_consistency_sustained_same_id_replace() {
    let (engine, _temp) = fresh_engine().await;
    let table = "same_id_replace";
    let mut oracle = Oracle::new();
    let id: u64 = 42;

    for round in 1..=50_i64 {
        let row = OracleRow {
            timestamp: 1_704_067_200_000 + round * 1000,
            value: round * 100,
            tag: format!("v{}", round),
        };
        oracle.insert(id, row.clone());
        let csv = format!(
            "id,timestamp,value,tag\n{},{},{},{}\n",
            id, row.timestamp, row.value, row.tag
        );
        engine.ingest_csv(table, csv).await.unwrap();
    }

    // Oracle is exactly 1 row containing the latest update.
    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 6. Restart, then continue — durability + identity preservation
// ==========================================================================

#[tokio::test]
async fn ts_consistency_restart_then_continue() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_string_lossy().to_string();
    let table = "restart_continue";
    let mut oracle = Oracle::new();

    // Phase 1 — insert 100, flush, drop.
    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir.clone();
        config.storage.flush_interval_ms = 0;
        let engine = StorageEngine::new(&config).await.unwrap();
        for i in 1..=100_u64 {
            oracle.insert(
                i,
                OracleRow {
                    timestamp: 1_704_067_200_000 + (i as i64),
                    value: i as i64,
                    tag: "phase1".to_string(),
                },
            );
        }
        let ids: Vec<u64> = (1..=100).collect();
        engine
            .ingest_csv(table, build_csv(&ids, &oracle))
            .await
            .unwrap();
        engine.flush_table(table).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(engine);
    }

    // Phase 2 — restart, update half, add 100 new, verify.
    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.flush_interval_ms = 0;
        let engine = StorageEngine::new(&config).await.unwrap();

        for i in 1..=50_u64 {
            let r = oracle.get_mut(&i).unwrap();
            r.value = i as i64 * 100;
            r.tag = "phase2_upd".to_string();
        }
        for i in 101..=200_u64 {
            oracle.insert(
                i,
                OracleRow {
                    timestamp: 1_704_067_300_000 + (i as i64),
                    value: i as i64,
                    tag: "phase2_new".to_string(),
                },
            );
        }
        let mut ids: Vec<u64> = (1..=50).collect();
        ids.extend(101..=200);
        engine
            .ingest_csv(table, build_csv(&ids, &oracle))
            .await
            .unwrap();
        engine.flush_table(table).await.unwrap();

        assert_oracle(&engine, table, &oracle).await;
    }
}

// ==========================================================================
// 7. WAL recovery of mixed flushed + buffered rows
// ==========================================================================

#[tokio::test]
async fn ts_consistency_wal_preserves_mixed_state() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_string_lossy().to_string();
    let table = "wal_mixed";
    let mut oracle = Oracle::new();

    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir.clone();
        config.storage.wal_enabled = true;
        config.storage.buffer_size = 100_000; // no overflow flush
        config.storage.flush_interval_ms = 0;
        let engine = StorageEngine::new(&config).await.unwrap();

        // Wave 1: CSV slow path → buffer + WAL → explicit flush_table moves
        // the rows into RocksDB and truncates the WAL. After this, the DB
        // holds 50 rows on disk and the WAL is empty.
        for i in 1..=50_u64 {
            oracle.insert(
                i,
                OracleRow {
                    timestamp: 1_704_067_200_000 + (i as i64),
                    value: i as i64,
                    tag: "flushed".to_string(),
                },
            );
        }
        let ids: Vec<u64> = (1..=50).collect();
        engine
            .ingest_csv(table, build_csv(&ids, &oracle))
            .await
            .unwrap();
        engine.flush_table(table).await.unwrap();

        // Wave 2: Protobuf always uses the slow path (buffer + WAL) even when
        // a schema already exists. We deliberately do NOT flush; the rows live
        // only in the in-memory buffer and the WAL file on disk. A crash now
        // can only recover them via WAL replay, so this is the path that
        // actually exercises mixed flushed + WAL-only state.
        for i in 51..=100_u64 {
            oracle.insert(
                i,
                OracleRow {
                    timestamp: 1_704_067_200_000 + (i as i64),
                    value: i as i64,
                    tag: "buffered".to_string(),
                },
            );
        }
        let mut rows = Vec::new();
        for i in 51..=100_u64 {
            let r = &oracle[&i];
            let mut values = HashMap::new();
            values.insert("id".to_string(), i.to_string());
            values.insert("timestamp".to_string(), r.timestamp.to_string());
            values.insert("value".to_string(), r.value.to_string());
            values.insert("tag".to_string(), r.tag.clone());
            rows.push(ProtoRow { values });
        }
        let mut buf = Vec::new();
        ProtoBatch { rows }.encode(&mut buf).unwrap();
        engine.ingest_protobuf(table, buf).await.unwrap();

        // Give the async WAL writer time to fsync before drop.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        drop(engine);
    }

    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.wal_enabled = true;
        config.storage.buffer_size = 100_000;
        config.storage.flush_interval_ms = 0;
        let engine = StorageEngine::new(&config).await.unwrap();

        // At this point: 50 rows live in RocksDB, 50 rows live in the buffer
        // (restored by WAL replay during StorageEngine::new). assert_oracle
        // exercises read paths that merge them (query JSON, get_row_by_id)
        // and read paths that flush before scanning (query_csv, query_arrow).
        // All five must report the same 100 rows.
        assert_oracle(&engine, table, &oracle).await;
    }
}

// ==========================================================================
// 8. Concurrent disjoint writers
// ==========================================================================

#[tokio::test]
async fn ts_consistency_concurrent_disjoint_writers() {
    let (engine, _temp) = fresh_engine().await;
    let table = "concurrent_disjoint";

    // Establish schema deterministically.
    engine
        .ingest_csv(
            table,
            "id,timestamp,value,tag\n1,1704067200000,1,seed\n".to_string(),
        )
        .await
        .unwrap();

    let mut oracle = Oracle::new();
    oracle.insert(
        1,
        OracleRow {
            timestamp: 1_704_067_200_000,
            value: 1,
            tag: "seed".to_string(),
        },
    );

    let writers: u64 = 6;
    let per_writer: u64 = 50;

    let mut handles = Vec::new();
    for w in 0..writers {
        let engine = engine.clone();
        let table = table.to_string();
        let base = 1000 + w * per_writer; // disjoint ID ranges
        handles.push(tokio::spawn(async move {
            let mut csv = String::from("id,timestamp,value,tag\n");
            for i in 0..per_writer {
                let id = base + i;
                csv.push_str(&format!(
                    "{},{},{},w{}\n",
                    id,
                    1_704_067_300_000 + (id as i64),
                    id,
                    w
                ));
            }
            engine.ingest_csv(&table, csv).await.unwrap();
        }));
    }

    for w in 0..writers {
        let base = 1000 + w * per_writer;
        for i in 0..per_writer {
            let id = base + i;
            oracle.insert(
                id,
                OracleRow {
                    timestamp: 1_704_067_300_000 + (id as i64),
                    value: id as i64,
                    tag: format!("w{}", w),
                },
            );
        }
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 9. Concurrent overlapping writers — same IDs, last-write-wins, no torn rows
// ==========================================================================

#[tokio::test]
async fn ts_consistency_concurrent_overlapping_no_torn_rows() {
    let (engine, _temp) = fresh_engine().await;
    let table = "concurrent_overlap";

    // Seed with the IDs all writers will contend over.
    let n_ids: u64 = 50;
    let mut seed_csv = String::from("id,timestamp,value,tag\n");
    for id in 1..=n_ids {
        seed_csv.push_str(&format!(
            "{},{},0,seed\n",
            id,
            1_704_067_200_000 + (id as i64)
        ));
    }
    engine.ingest_csv(table, seed_csv).await.unwrap();

    let writers: u64 = 4;
    let rounds: u64 = 5;

    let mut handles = Vec::new();
    for w in 0..writers {
        let engine = engine.clone();
        let table = table.to_string();
        handles.push(tokio::spawn(async move {
            for round in 0..rounds {
                let mut csv = String::from("id,timestamp,value,tag\n");
                for id in 1..=n_ids {
                    csv.push_str(&format!(
                        "{},{},{},w{}r{}\n",
                        id,
                        1_704_067_300_000 + (w * 1000 + round) as i64,
                        w * 1000 + round + id * 100,
                        w,
                        round,
                    ));
                }
                engine.ingest_csv(&table, csv).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Build the set of all possible (value, tag) tuples per ID across all
    // writers and rounds. Each ID's observed final state must be one of them
    // (and the same value across every read path).
    let mut allowed: HashMap<u64, HashSet<(i64, String)>> = HashMap::new();
    // The seed write is also a possible final state if no writer happened
    // to write that id after the seed (theoretically — in practice every
    // writer rewrites every id, but we model the seed for robustness).
    for id in 1..=n_ids {
        let mut set = HashSet::new();
        set.insert((0, "seed".to_string()));
        for w in 0..writers {
            for round in 0..rounds {
                let value = (w * 1000 + round + id * 100) as i64;
                let tag = format!("w{}r{}", w, round);
                set.insert((value, tag));
            }
        }
        allowed.insert(id, set);
    }

    // 1. query() JSON
    let json = engine
        .query(table, None, None, Some((n_ids + 100) as usize), None)
        .await
        .unwrap();
    assert_eq!(
        json.len(),
        n_ids as usize,
        "expected {} rows, got {}",
        n_ids,
        json.len()
    );
    let mut final_state: HashMap<u64, (i64, String)> = HashMap::new();
    for r in &json {
        let id = json_id(r);
        let v = json_i64(r, "value");
        let t = json_str(r, "tag").to_string();
        assert!(
            allowed[&id].contains(&(v, t.clone())),
            "id={} got ({}, {:?}) which is not from any writer",
            id,
            v,
            t
        );
        assert!(final_state.insert(id, (v, t)).is_none(), "duplicate id");
    }

    // 2. get_table_count
    let count = engine.get_table_count(table).await.unwrap();
    assert_eq!(count, n_ids, "count mismatch");

    // 3. get_row_by_id agrees with query()
    for (id, (v, t)) in &final_state {
        let row = engine
            .get_row_by_id_json(table, *id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("get_row_by_id missing id={}", id));
        assert_eq!(json_i64(&row, "value"), *v, "get_row_by_id value id={}", id);
        assert_eq!(json_str(&row, "tag"), t, "get_row_by_id tag id={}", id);
    }

    // 4. query_csv agrees with query()
    let csv = engine
        .query_csv(table, None, None, Some((n_ids + 100) as usize), None)
        .await
        .unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines.len() - 1, n_ids as usize, "csv row count");
    let header: Vec<&str> = lines[0].split(',').collect();
    let id_pos = header.iter().position(|c| *c == "id").unwrap();
    let val_pos = header.iter().position(|c| *c == "value").unwrap();
    let tag_pos = header.iter().position(|c| *c == "tag").unwrap();
    for line in &lines[1..] {
        let cols: Vec<&str> = line.split(',').collect();
        let id: u64 = cols[id_pos].parse().unwrap();
        let v: i64 = cols[val_pos].parse().unwrap();
        let t = cols[tag_pos];
        let expected = &final_state[&id];
        assert_eq!(v, expected.0, "csv value mismatch id={}", id);
        assert_eq!(t, expected.1, "csv tag mismatch id={}", id);
    }

    // 5. query_arrow agrees with query()
    let arrow_bytes = engine
        .query_arrow(table, None, None, Some((n_ids + 100) as usize), None)
        .await
        .unwrap();
    let reader = StreamReader::try_new(std::io::Cursor::new(&arrow_bytes), None).unwrap();
    let mut arrow_seen = 0_usize;
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("arrow id UInt64")
            .values()
            .to_vec();
        let vals = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("arrow value Int64")
            .values()
            .to_vec();
        let tags = batch
            .column_by_name("tag")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("arrow tag Utf8");
        for i in 0..batch.num_rows() {
            let id = ids[i];
            let v = vals[i];
            let t = tags.value(i);
            let expected = &final_state[&id];
            assert_eq!(v, expected.0, "arrow value mismatch id={}", id);
            assert_eq!(t, expected.1, "arrow tag mismatch id={}", id);
            arrow_seen += 1;
        }
    }
    assert_eq!(arrow_seen, n_ids as usize, "arrow row count");
}

// ==========================================================================
// 10. Cross-format manipulation chain — CSV → Protobuf → Arrow → CSV
// ==========================================================================

#[tokio::test]
async fn ts_consistency_cross_format_chain() {
    let (engine, _temp) = fresh_engine().await;
    let table = "cross_chain";
    let mut oracle = Oracle::new();

    // Step 1 — CSV insert: ids 1..=3
    for i in 1..=3_u64 {
        oracle.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_200_000 + (i as i64),
                value: i as i64,
                tag: "csv1".to_string(),
            },
        );
    }
    let ids: Vec<u64> = (1..=3).collect();
    engine
        .ingest_csv(table, build_csv(&ids, &oracle))
        .await
        .unwrap();

    // Step 2 — Protobuf update id=1 + insert id=4
    oracle.insert(
        1,
        OracleRow {
            timestamp: 1_704_067_300_000,
            value: 100,
            tag: "proto".to_string(),
        },
    );
    oracle.insert(
        4,
        OracleRow {
            timestamp: 1_704_067_300_004,
            value: 400,
            tag: "proto".to_string(),
        },
    );
    let mut rows = Vec::new();
    for id in [1_u64, 4] {
        let r = &oracle[&id];
        let mut values = HashMap::new();
        values.insert("id".to_string(), id.to_string());
        values.insert("timestamp".to_string(), r.timestamp.to_string());
        values.insert("value".to_string(), r.value.to_string());
        values.insert("tag".to_string(), r.tag.clone());
        rows.push(ProtoRow { values });
    }
    let mut buf = Vec::new();
    ProtoBatch { rows }.encode(&mut buf).unwrap();
    engine.ingest_protobuf(table, buf).await.unwrap();

    // Step 3 — Arrow update id=2 + insert id=5
    oracle.insert(
        2,
        OracleRow {
            timestamp: 1_704_067_400_000,
            value: 200,
            tag: "arrow".to_string(),
        },
    );
    oracle.insert(
        5,
        OracleRow {
            timestamp: 1_704_067_400_005,
            value: 500,
            tag: "arrow".to_string(),
        },
    );
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("timestamp", ArrowDataType::Int64, false),
        Field::new("value", ArrowDataType::Int64, false),
        Field::new("tag", ArrowDataType::Utf8, false),
    ]));
    let ids = Int64Array::from(vec![2_i64, 5]);
    let tss = Int64Array::from(vec![1_704_067_400_000, 1_704_067_400_005]);
    let vals = Int64Array::from(vec![200_i64, 500]);
    let tags = StringArray::from(vec!["arrow", "arrow"]);
    let arrow_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(ids), Arc::new(tss), Arc::new(vals), Arc::new(tags)],
    )
    .unwrap();
    let mut arrow_buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut arrow_buf, &arrow_schema).unwrap();
        w.write(&arrow_batch).unwrap();
        w.finish().unwrap();
    }
    engine.ingest_arrow(table, arrow_buf).await.unwrap();

    // Step 4 — CSV finishes off id=3
    oracle.insert(
        3,
        OracleRow {
            timestamp: 1_704_067_500_000,
            value: 300,
            tag: "csv2".to_string(),
        },
    );
    engine
        .ingest_csv(
            table,
            format!(
                "id,timestamp,value,tag\n3,{},300,csv2\n",
                1_704_067_500_000_i64
            ),
        )
        .await
        .unwrap();

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 11. Idempotent re-insert — same row N times → 1 logical row
// ==========================================================================

#[tokio::test]
async fn ts_consistency_idempotent_reinsert() {
    let (engine, _temp) = fresh_engine().await;
    let table = "idempotent";
    let mut oracle = Oracle::new();
    oracle.insert(
        42,
        OracleRow {
            timestamp: 1_704_067_200_000,
            value: 999,
            tag: "stable".to_string(),
        },
    );
    let csv = "id,timestamp,value,tag\n42,1704067200000,999,stable\n".to_string();
    for _ in 0..20 {
        engine.ingest_csv(table, csv.clone()).await.unwrap();
    }
    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 12. Many small blocks then heavy update — block-fragmentation correctness
// ==========================================================================

#[tokio::test]
async fn ts_consistency_many_blocks_with_updates() {
    let (engine, _temp) = fresh_engine().await;
    let table = "frag_updates";
    let mut oracle = Oracle::new();

    // Phase 1: 50 ingests × 5 rows = 50 blocks, 250 distinct rows
    for batch in 0..50_u64 {
        let mut ids = Vec::new();
        for i in 0..5_u64 {
            let id = batch * 5 + i + 1;
            oracle.insert(
                id,
                OracleRow {
                    timestamp: 1_704_067_200_000 + (id as i64) * 100,
                    value: id as i64,
                    tag: format!("b{}", batch),
                },
            );
            ids.push(id);
        }
        engine
            .ingest_csv(table, build_csv(&ids, &oracle))
            .await
            .unwrap();
    }

    // Phase 2: 5 rounds of updates to every 10th row
    for round in 1..=5_i64 {
        let mut update_ids = Vec::new();
        for i in (10..=250_u64).step_by(10) {
            let r = oracle.get_mut(&i).unwrap();
            r.value = round * 1000 + i as i64;
            r.tag = format!("upd{}", round);
            update_ids.push(i);
        }
        engine
            .ingest_csv(table, build_csv(&update_ids, &oracle))
            .await
            .unwrap();
    }

    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 13. Range query reflects updated timestamps — validity-check correctness
// ==========================================================================

#[tokio::test]
async fn ts_consistency_range_after_ts_update() {
    let (engine, _temp) = fresh_engine().await;
    let table = "range_after_update";

    // Three rows with distinct timestamps.
    engine
        .ingest_csv(
            table,
            "id,timestamp,value,tag\n\
             1,1704067200000,10,initial\n\
             2,1704067210000,20,initial\n\
             3,1704067220000,30,initial\n"
                .to_string(),
        )
        .await
        .unwrap();

    // Update id=1 to a much later timestamp; the original block at ts=200000
    // still physically exists but its id_key now points to the new block.
    engine
        .ingest_csv(
            table,
            "id,timestamp,value,tag\n1,1704067290000,11,updated\n".to_string(),
        )
        .await
        .unwrap();

    // Range [200000, 215000] should NOT include id=1 — the validity check
    // must filter out the stale entry whose id_key no longer points back.
    let r1 = engine
        .query(
            table,
            Some("1704067200000".to_string()),
            Some("1704067215000".to_string()),
            Some(100),
            None,
        )
        .await
        .unwrap();
    let ids1: HashSet<u64> = r1.iter().map(json_id).collect();
    assert_eq!(
        ids1,
        [2_u64].into_iter().collect(),
        "stale entry must be filtered; got {:?}",
        ids1
    );

    // Range covering the new ts must include exactly the updated row.
    let r2 = engine
        .query(
            table,
            Some("1704067285000".to_string()),
            Some("1704067295000".to_string()),
            Some(100),
            None,
        )
        .await
        .unwrap();
    assert_eq!(r2.len(), 1, "new-ts range expected 1 row, got {}", r2.len());
    assert_eq!(json_id(&r2[0]), 1);
    assert_eq!(json_i64(&r2[0], "value"), 11);
    assert_eq!(json_str(&r2[0], "tag"), "updated");

    // Full query: 3 logical rows, id=1 with latest values, the rest unchanged.
    let mut oracle = Oracle::new();
    oracle.insert(
        1,
        OracleRow {
            timestamp: 1_704_067_290_000,
            value: 11,
            tag: "updated".to_string(),
        },
    );
    oracle.insert(
        2,
        OracleRow {
            timestamp: 1_704_067_210_000,
            value: 20,
            tag: "initial".to_string(),
        },
    );
    oracle.insert(
        3,
        OracleRow {
            timestamp: 1_704_067_220_000,
            value: 30,
            tag: "initial".to_string(),
        },
    );
    assert_oracle(&engine, table, &oracle).await;
}

// ==========================================================================
// 14. Multi-table isolation — operations on table A don't bleed into table B
// ==========================================================================

#[tokio::test]
async fn ts_consistency_multi_table_isolation() {
    let (engine, _temp) = fresh_engine().await;
    let table_a = "iso_a";
    let table_b = "iso_b";

    let mut oracle_a = Oracle::new();
    let mut oracle_b = Oracle::new();

    // Populate table A.
    for i in 1..=100_u64 {
        oracle_a.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_200_000 + (i as i64),
                value: i as i64,
                tag: "table_a".to_string(),
            },
        );
    }
    let ids: Vec<u64> = (1..=100).collect();
    engine
        .ingest_csv(table_a, build_csv(&ids, &oracle_a))
        .await
        .unwrap();

    // Populate table B with overlapping IDs (1..=100 again, totally
    // different values/tags). They must not see each other.
    for i in 1..=100_u64 {
        oracle_b.insert(
            i,
            OracleRow {
                timestamp: 1_704_067_300_000 + (i as i64),
                value: i as i64 * 1000,
                tag: "table_b".to_string(),
            },
        );
    }
    engine
        .ingest_csv(table_b, build_csv(&ids, &oracle_b))
        .await
        .unwrap();

    // Update one row in each table; the other must remain pristine.
    let mut update_csv_a = String::from("id,timestamp,value,tag\n");
    let r = oracle_a.get_mut(&50).unwrap();
    r.value = 5555;
    r.tag = "a_updated".to_string();
    update_csv_a.push_str(&format!("50,{},5555,a_updated\n", r.timestamp));
    engine.ingest_csv(table_a, update_csv_a).await.unwrap();

    let mut update_csv_b = String::from("id,timestamp,value,tag\n");
    let r = oracle_b.get_mut(&50).unwrap();
    r.value = 9999;
    r.tag = "b_updated".to_string();
    update_csv_b.push_str(&format!("50,{},9999,b_updated\n", r.timestamp));
    engine.ingest_csv(table_b, update_csv_b).await.unwrap();

    assert_oracle(&engine, table_a, &oracle_a).await;
    assert_oracle(&engine, table_b, &oracle_b).await;
}

// ==========================================================================
// 15. Randomized workload — N ops, oracle tracks every change, all paths agree
// ==========================================================================

#[tokio::test]
async fn ts_consistency_random_workload_oracle_agreement() {
    let (engine, _temp) = fresh_engine().await;
    let table = "random_workload";
    let mut oracle = Oracle::new();
    let mut rng = Lcg::new(0xABCDEF0123456789);

    // Seed with one row to fix the schema.
    oracle.insert(
        1,
        OracleRow {
            timestamp: 1_704_067_200_000,
            value: 1,
            tag: "seed".to_string(),
        },
    );
    engine
        .ingest_csv(
            table,
            "id,timestamp,value,tag\n1,1704067200000,1,seed\n".to_string(),
        )
        .await
        .unwrap();

    // 300 random ops. Each op is a small batch (1..=8 rows) of inserts
    // and/or updates against the ID space 1..=200.
    let id_space: u64 = 200;
    for op in 0..300_u64 {
        let batch_size = rng.range(1, 9) as usize;
        let mut batch_ids: Vec<u64> = Vec::with_capacity(batch_size);
        let mut seen_in_batch: HashSet<u64> = HashSet::new();
        for _ in 0..batch_size {
            // Pick an ID; allow re-use across the batch deliberately to
            // exercise within-batch REPLACE.
            let id = rng.range(1, id_space + 1);
            if !seen_in_batch.insert(id) {
                continue; // skip dup-within-batch (same-batch REPLACE has
                          // ill-defined order); avoids unrelated noise.
            }
            let row = OracleRow {
                timestamp: 1_704_067_300_000 + (op as i64) * 1000 + (id as i64),
                value: (op as i64) * 10_000 + id as i64,
                tag: format!("op{}_id{}", op, id),
            };
            oracle.insert(id, row);
            batch_ids.push(id);
        }
        if batch_ids.is_empty() {
            continue;
        }
        engine
            .ingest_csv(table, build_csv(&batch_ids, &oracle))
            .await
            .unwrap();
    }

    assert_oracle(&engine, table, &oracle).await;
}
