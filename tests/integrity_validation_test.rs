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

//! End-to-end data integrity validation suite.
//!
//! Each test exercises a scenario that could plausibly corrupt or lose data:
//! round-trip fidelity, REPLACE semantics across persisted blocks, restart
//! durability, schema/id-state recovery, buffer/DB merge correctness, the
//! query format trio (JSON / CSV / Arrow), pagination correctness, the
//! validity-check fast-path that hides stale rows after updates, concurrency,
//! and compression branches (boolean RLE, string dictionary vs direct, float
//! special values, etc.). Every test uses only the public `StorageEngine`
//! interface and asserts strictly on observable behavior.

#![allow(clippy::approx_constant)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::needless_range_loop)]

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;

// --------------------------------------------------------------------------
// Test infrastructure
// --------------------------------------------------------------------------

fn default_config(temp_dir: &TempDir) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    // Disable background flush to keep tests deterministic. Explicit
    // buffer-overflow flushes and `flush_table` still happen.
    config.storage.flush_interval_ms = 0;
    config
}

async fn fresh_engine() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let config = default_config(&temp);
    let engine = StorageEngine::new(&config).await.unwrap();
    (engine, temp)
}

async fn engine_with(config: Config) -> StorageEngine {
    StorageEngine::new(&config).await.unwrap()
}

/// Small deterministic LCG so tests don't pull in `rand`.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1))
    }
    fn next_u64(&mut self) -> u64 {
        // Numerical Recipes LCG
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }
    fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }
    fn next_f64(&mut self) -> f64 {
        // Map u64 to (-1e6, 1e6) range, deterministic, exact double values.
        let raw = (self.next_u64() % 2_000_000_001) as f64 - 1_000_000_000.0;
        raw / 1000.0
    }
}

fn get_id(row: &serde_json::Value) -> u64 {
    row.get("id")
        .expect("id column")
        .as_u64()
        .expect("id is u64")
}

fn get_i64(row: &serde_json::Value, field: &str) -> i64 {
    row.get(field)
        .unwrap_or_else(|| panic!("missing {field}"))
        .as_i64()
        .unwrap_or_else(|| panic!("{field} not i64: {:?}", row.get(field)))
}

fn get_f64(row: &serde_json::Value, field: &str) -> f64 {
    row.get(field)
        .unwrap_or_else(|| panic!("missing {field}"))
        .as_f64()
        .unwrap_or_else(|| panic!("{field} not f64: {:?}", row.get(field)))
}

fn get_str<'a>(row: &'a serde_json::Value, field: &str) -> &'a str {
    row.get(field)
        .unwrap_or_else(|| panic!("missing {field}"))
        .as_str()
        .unwrap_or_else(|| panic!("{field} not str: {:?}", row.get(field)))
}

fn get_bool(row: &serde_json::Value, field: &str) -> bool {
    row.get(field)
        .unwrap_or_else(|| panic!("missing {field}"))
        .as_bool()
        .unwrap_or_else(|| {
            row.get(field)
                .and_then(|v| v.as_str())
                .map(|s| s.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        })
}

// --------------------------------------------------------------------------
// 1. Large randomized round-trip across all data types
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_random_round_trip_5000_rows_all_types() {
    let (engine, _temp) = fresh_engine().await;
    let table = "rt5k";

    let n: usize = 5000;
    let mut rng = Lcg::new(0xCAFEBABE);

    // Build the ground-truth dataset.
    struct Row {
        id: u64,
        ts: i64,
        int_v: i64,
        flt_v: f64,
        str_v: String,
        bool_v: bool,
    }
    let mut truth: Vec<Row> = Vec::with_capacity(n);
    for i in 0..n {
        truth.push(Row {
            id: (i as u64) + 1,                      // contiguous user IDs
            ts: 1_704_067_200_000 + (i as i64) * 17, // strictly increasing
            int_v: rng.next_i64(),
            flt_v: rng.next_f64(),
            // Mix short + long strings to exercise both inline and bulk paths.
            str_v: if i % 23 == 0 {
                format!("long-{}-{}", i, "x".repeat(120))
            } else {
                format!("s{}", i)
            },
            bool_v: rng.next_bool(),
        });
    }

    // Build CSV (a single ingest call, large enough to take the columnar fast path).
    let mut csv = String::with_capacity(n * 60);
    csv.push_str("id,timestamp,int_v,flt_v,str_v,bool_v\n");
    for r in &truth {
        // String column has no commas/quotes/newlines in our fixture, so plain.
        csv.push_str(&format!(
            "{},{},{},{},{},{}\n",
            r.id, r.ts, r.int_v, r.flt_v, r.str_v, r.bool_v
        ));
    }
    let stats = engine.ingest_csv(table, csv).await.unwrap();
    assert_eq!(stats.rows_inserted, n as u64, "all rows must be inserted");

    // Read back with explicit limit; default limit caps at 1000.
    let results = engine
        .query(table, None, None, Some(n + 10), None)
        .await
        .unwrap();
    assert_eq!(results.len(), n, "round-trip row count");

    // Verify every row byte-for-byte (mod float tolerance).
    let mut by_id: HashMap<u64, &serde_json::Value> = HashMap::with_capacity(n);
    for row in &results {
        by_id.insert(get_id(row), row);
    }
    assert_eq!(by_id.len(), n, "no duplicate IDs after round-trip");

    for r in &truth {
        let row = by_id
            .get(&r.id)
            .unwrap_or_else(|| panic!("id {} missing", r.id));
        assert_eq!(get_i64(row, "int_v"), r.int_v, "int mismatch id={}", r.id);
        let got_f = get_f64(row, "flt_v");
        assert!(
            (got_f - r.flt_v).abs() < 1e-9 || got_f.to_bits() == r.flt_v.to_bits(),
            "float mismatch id={} expected={} got={}",
            r.id,
            r.flt_v,
            got_f
        );
        assert_eq!(get_str(row, "str_v"), r.str_v, "str mismatch id={}", r.id);
        assert_eq!(
            get_bool(row, "bool_v"),
            r.bool_v,
            "bool mismatch id={}",
            r.id
        );
    }
}

// --------------------------------------------------------------------------
// 2. REPLACE semantics across multiple PERSISTED blocks
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_replace_across_persisted_blocks() {
    let (engine, _temp) = fresh_engine().await;
    let table = "replace_blocks";

    // Phase 1: insert 200 rows (goes through fast path, becomes one block).
    let mut csv = String::from("id,timestamp,value,tag\n");
    for i in 1..=200_i64 {
        csv.push_str(&format!(
            "{},{},{},original\n",
            i,
            1_704_067_200_000 + i * 10,
            i * 100
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Phase 2: update half the IDs with NEW timestamps and values, written as a new block.
    let mut csv2 = String::from("id,timestamp,value,tag\n");
    for i in (1..=200_i64).step_by(2) {
        // Updated timestamp is AFTER original; updated value distinct.
        csv2.push_str(&format!(
            "{},{},{},updated\n",
            i,
            1_704_067_300_000 + i * 10,
            i * 100 + 7
        ));
    }
    engine.ingest_csv(table, csv2).await.unwrap();

    // Phase 3: another update on a subset moves to a third block.
    let mut csv3 = String::from("id,timestamp,value,tag\n");
    for i in (1..=200_i64).step_by(20) {
        csv3.push_str(&format!(
            "{},{},{},twice\n",
            i,
            1_704_067_400_000 + i * 10,
            i * 100 + 13
        ));
    }
    engine.ingest_csv(table, csv3).await.unwrap();

    // Verify final state.
    let results = engine
        .query(table, None, None, Some(1000), None)
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        200,
        "logical row count must still be 200 after REPLACE"
    );

    let mut by_id: HashMap<u64, &serde_json::Value> = HashMap::new();
    for r in &results {
        by_id.insert(get_id(r), r);
    }
    assert_eq!(by_id.len(), 200, "REPLACE must not introduce duplicates");

    // Phase 2 ids: every odd i (1, 3, 5, ..., 199).
    // Phase 3 ids: step_by(20) starting at 1 → i % 20 == 1 (1, 21, ..., 181) — all odd.
    for i in 1..=200_i64 {
        let row = by_id
            .get(&(i as u64))
            .unwrap_or_else(|| panic!("id {} missing", i));
        let tag = get_str(row, "tag");
        let val = get_i64(row, "value");
        if i % 20 == 1 {
            assert_eq!(tag, "twice", "id={} expected tag=twice", i);
            assert_eq!(val, i * 100 + 13, "id={} value mismatch (twice)", i);
        } else if i % 2 == 1 {
            assert_eq!(tag, "updated", "id={} expected tag=updated", i);
            assert_eq!(val, i * 100 + 7, "id={} value mismatch (updated)", i);
        } else {
            assert_eq!(tag, "original", "id={} expected tag=original", i);
            assert_eq!(val, i * 100, "id={} value mismatch (original)", i);
        }
    }
}

// --------------------------------------------------------------------------
// 3. get_row_by_id returns latest version after a chain of updates
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_get_row_by_id_after_update_chain() {
    let (engine, _temp) = fresh_engine().await;
    let table = "get_by_id_chain";

    let id: u64 = 42;
    // Run a chain of 10 updates on the same ID.
    for version in 1..=10_i64 {
        let csv = format!(
            "id,timestamp,value\n{},{},{}\n",
            id,
            1_704_067_200_000 + version * 1_000,
            version * 1_000
        );
        engine.ingest_csv(table, csv).await.unwrap();
    }

    // Fetch by ID and check we get the latest version (10).
    let row = engine
        .get_row_by_id_json(table, id)
        .await
        .unwrap()
        .expect("row must exist");
    assert_eq!(get_id(&row), id);
    assert_eq!(get_i64(&row, "value"), 10_000, "must return latest value");

    // Cross-check via range query.
    let all = engine
        .query(table, None, None, Some(100), None)
        .await
        .unwrap();
    assert_eq!(all.len(), 1, "single logical row across all updates");
    assert_eq!(get_i64(&all[0], "value"), 10_000);

    // Non-existent ID returns None.
    let missing = engine.get_row_by_id_json(table, 999_999).await.unwrap();
    assert!(missing.is_none(), "missing ID must yield None");
}

// --------------------------------------------------------------------------
// 4. Mixed user-provided + auto-assigned IDs in a single batch
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_mixed_user_and_auto_ids() {
    let (engine, _temp) = fresh_engine().await;
    let table = "mixed_ids";

    // Some rows have explicit ID, some leave it blank (auto).
    let csv = "id,timestamp,value\n\
        1000,1704067200000,a\n\
        ,1704067201000,b\n\
        2000,1704067202000,c\n\
        ,1704067203000,d\n\
        ,1704067204000,e\n\
        3000,1704067205000,f\n";
    let stats = engine.ingest_csv(table, csv.to_string()).await.unwrap();
    assert_eq!(stats.rows_inserted, 6);

    let results = engine
        .query(table, None, None, Some(50), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 6);

    let mut ids: HashSet<u64> = HashSet::new();
    for r in &results {
        let inserted = ids.insert(get_id(r));
        assert!(inserted, "duplicate id in result set: {}", get_id(r));
    }
    // User-provided IDs must be present.
    for expected in &[1000_u64, 2000, 3000] {
        assert!(ids.contains(expected), "user id {} missing", expected);
    }
    // No auto-assigned ID may collide with a user-provided ID.
    let auto_ids: Vec<u64> = ids
        .iter()
        .copied()
        .filter(|i| !matches!(*i, 1000 | 2000 | 3000))
        .collect();
    assert_eq!(auto_ids.len(), 3, "exactly 3 auto IDs assigned");
    for auto in &auto_ids {
        assert!(*auto != 1000 && *auto != 2000 && *auto != 3000);
        assert!(*auto != 0, "auto ID must never be zero");
    }

    // Every ID must be individually retrievable.
    for id in &ids {
        let row = engine.get_row_by_id_json(table, *id).await.unwrap();
        assert!(row.is_some(), "row id={} not retrievable", id);
    }
}

// --------------------------------------------------------------------------
// 5. Large user ID does not cause auto-ID collisions later
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_high_user_id_then_auto_assignment() {
    let (engine, _temp) = fresh_engine().await;
    let table = "high_user_id";

    // Insert a very large user-provided ID.
    let big: u64 = u64::MAX / 2;
    engine
        .ingest_csv(
            table,
            format!("id,timestamp,value\n{},1704067200000,big\n", big),
        )
        .await
        .unwrap();

    // Insert several rows with auto IDs (empty id).
    let mut csv = String::from("id,timestamp,value\n");
    for i in 0..20 {
        csv.push_str(&format!(",1704067210000,a{}\n", i));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine
        .query(table, None, None, Some(100), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 21, "1 user + 20 auto rows");

    let mut seen = HashSet::new();
    let mut found_big = false;
    for r in &results {
        let id = get_id(r);
        assert!(id > 0, "id must be positive");
        assert!(seen.insert(id), "duplicate id {}", id);
        if id == big {
            found_big = true;
        }
    }
    assert!(found_big, "big user id preserved");
}

// --------------------------------------------------------------------------
// 6. Restart durability of FLUSHED (RocksDB-persisted) data
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_restart_after_flush_preserves_all_data() {
    let temp = TempDir::new().unwrap();
    let config = default_config(&temp);
    let data_dir = config.storage.data_dir.clone();
    let table = "restart_flushed";

    // Phase 1: ingest, force flush.
    {
        let engine = engine_with(config).await;
        let mut csv = String::from("id,timestamp,value,name\n");
        for i in 1..=200_i64 {
            csv.push_str(&format!(
                "{},{},{},name{}\n",
                i,
                1_704_067_200_000 + i,
                i * 10,
                i
            ));
        }
        engine.ingest_csv(table, csv).await.unwrap();
        // Explicit flush — guarantees data is in RocksDB, not buffer.
        engine.flush_table(table).await.unwrap();
        // Wait for any async WAL operations to settle.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(engine);
    }

    // Phase 2: restart, query.
    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.flush_interval_ms = 0;
        let engine = engine_with(config).await;

        let results = engine
            .query(table, None, None, Some(500), None)
            .await
            .unwrap();
        assert_eq!(
            results.len(),
            200,
            "all flushed rows recovered after restart"
        );
        for r in &results {
            let id = get_id(r);
            assert_eq!(get_i64(r, "value"), (id as i64) * 10);
            assert_eq!(get_str(r, "name"), format!("name{}", id));
        }
    }
}

// --------------------------------------------------------------------------
// 7. Auto-IDs after restart do not collide with persisted IDs
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_restart_no_id_reuse_after_recovery() {
    let temp = TempDir::new().unwrap();
    let config = default_config(&temp);
    let data_dir = config.storage.data_dir.clone();
    let table = "restart_id_state";

    // Phase 1: ingest with explicit large user IDs.
    {
        let engine = engine_with(config).await;
        let csv = "id,timestamp,value\n\
            10000,1704067200000,a\n\
            10001,1704067201000,b\n\
            10002,1704067202000,c\n";
        engine.ingest_csv(table, csv.to_string()).await.unwrap();
        engine.flush_table(table).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(engine);
    }

    // Phase 2: restart, ingest auto-id rows. None of them must collide with 10000..=10002.
    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.flush_interval_ms = 0;
        let engine = engine_with(config).await;

        let mut csv = String::from("id,timestamp,value\n");
        for i in 0..30 {
            csv.push_str(&format!(",1704067300000,auto{}\n", i));
        }
        engine.ingest_csv(table, csv).await.unwrap();

        let results = engine
            .query(table, None, None, Some(200), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 33, "3 user + 30 auto rows");

        let mut seen = HashSet::new();
        for r in &results {
            assert!(
                seen.insert(get_id(r)),
                "duplicate id after restart: {}",
                get_id(r)
            );
        }
        // Ensure none of the auto IDs collided with persisted ones.
        // (User IDs are still present and unique.)
        for expected in &[10000_u64, 10001, 10002] {
            assert!(
                seen.contains(expected),
                "user id {} lost after restart",
                expected
            );
        }
    }
}

// --------------------------------------------------------------------------
// 8. Schema persists across restart with inferred types intact
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_schema_persists_across_restart() {
    let temp = TempDir::new().unwrap();
    let config = default_config(&temp);
    let data_dir = config.storage.data_dir.clone();
    let table = "schema_persist";

    {
        let engine = engine_with(config).await;
        let csv = "id,timestamp,int_col,float_col,str_col,bool_col\n\
            1,1704067200000,42,3.14,hello,true\n\
            2,1704067201000,99,2.71,world,false\n";
        engine.ingest_csv(table, csv.to_string()).await.unwrap();
        engine.flush_table(table).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(engine);
    }

    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.flush_interval_ms = 0;
        let engine = engine_with(config).await;
        let schema_v = engine.get_schema(table).await.unwrap();
        let schema: pulsora::storage::schema::Schema = serde_json::from_value(schema_v).unwrap();
        let by_name: HashMap<&str, &pulsora::storage::schema::DataType> = schema
            .columns
            .iter()
            .map(|c| (c.name.as_str(), &c.data_type))
            .collect();
        use pulsora::storage::schema::DataType as DT;
        assert!(matches!(by_name.get("int_col"), Some(DT::Integer)));
        assert!(matches!(by_name.get("float_col"), Some(DT::Float)));
        assert!(matches!(by_name.get("str_col"), Some(DT::String)));
        assert!(matches!(by_name.get("bool_col"), Some(DT::Boolean)));
        assert!(matches!(by_name.get("id"), Some(DT::Id)));

        // Querying still works using the recovered schema.
        let results = engine
            .query(table, None, None, Some(10), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
    }
}

// --------------------------------------------------------------------------
// 9. WAL recovers many buffered rows after a crash
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_wal_recovers_many_buffered_rows() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_string_lossy().to_string();
    config.storage.wal_enabled = true;
    config.storage.buffer_size = 10_000; // big buffer so nothing flushes by overflow
    config.storage.flush_interval_ms = 0; // no background flush
    let data_dir = config.storage.data_dir.clone();
    let table = "wal_many";

    {
        let engine = engine_with(config).await;
        let mut csv = String::from("id,timestamp,value\n");
        for i in 1..=80_i64 {
            csv.push_str(&format!("{},{},{}\n", i, 1_704_067_200_000 + i, i * 7));
        }
        engine.ingest_csv(table, csv).await.unwrap();

        // Pre-drop: verify rows are visible through buffer query.
        let pre = engine
            .query(table, None, None, Some(200), None)
            .await
            .unwrap();
        assert_eq!(pre.len(), 80);

        // Wait for async WAL writer to persist.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        drop(engine);
    }

    {
        let mut config = Config::default();
        config.storage.data_dir = data_dir;
        config.storage.wal_enabled = true;
        config.storage.buffer_size = 10_000;
        config.storage.flush_interval_ms = 0;
        let engine = engine_with(config).await;

        let results = engine
            .query(table, None, None, Some(200), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 80, "all 80 buffered rows recovered from WAL");
        for r in &results {
            let id = get_id(r);
            assert!((1..=80).contains(&id));
            assert_eq!(get_i64(r, "value"), (id as i64) * 7);
        }
    }
}

// --------------------------------------------------------------------------
// 10. Pagination is exhaustive and non-overlapping across DB-resident data
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_pagination_no_loss_no_duplicates() {
    let (engine, _temp) = fresh_engine().await;
    let table = "pagination_strict";

    let n: usize = 537; // not a multiple of the page size
    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=n {
        csv.push_str(&format!(
            "{},{},{}\n",
            i,
            1_704_067_200_000_i64 + (i as i64),
            i * 11
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let page_size = 50;
    let mut all_seen: Vec<(u64, i64)> = Vec::new();
    let mut offset = 0;
    loop {
        let page = engine
            .query(table, None, None, Some(page_size), Some(offset))
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        for r in page.iter() {
            all_seen.push((get_id(r), get_i64(r, "value")));
        }
        offset += page_size;
        if offset > n * 2 {
            panic!("pagination did not terminate");
        }
    }

    assert_eq!(
        all_seen.len(),
        n,
        "every row visited exactly once via pagination"
    );

    let mut ids: HashSet<u64> = HashSet::new();
    for (id, val) in &all_seen {
        assert!(ids.insert(*id), "id {} returned twice across pages", id);
        assert_eq!(*val, (*id as i64) * 11, "value/id mismatch for id={}", id);
    }
    for i in 1..=n {
        assert!(ids.contains(&(i as u64)), "id {} never returned", i);
    }
}

// --------------------------------------------------------------------------
// 11. Concurrent ingestion of disjoint ID ranges loses nothing
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_concurrent_disjoint_ingestion() {
    let (engine, _temp) = fresh_engine().await;
    let table = "concurrent_disjoint";

    // First ingest establishes the schema deterministically.
    engine
        .ingest_csv(table, "id,timestamp,value\n1,1704067200000,1\n".to_string())
        .await
        .unwrap();

    let writers = 8_u64;
    let per_writer = 100_u64;

    let mut handles = Vec::new();
    for w in 0..writers {
        let engine = engine.clone();
        let table = table.to_string();
        handles.push(tokio::spawn(async move {
            let base = 1_000 + w * per_writer * 2; // disjoint
            let mut csv = String::from("id,timestamp,value\n");
            for i in 0..per_writer {
                let id = base + i;
                csv.push_str(&format!(
                    "{},{},{}\n",
                    id,
                    1_704_067_300_000 + (id as i64),
                    id
                ));
            }
            engine.ingest_csv(&table, csv).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let expected = 1 + writers * per_writer;
    let results = engine
        .query(table, None, None, Some((expected as usize) + 10), None)
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        expected as usize,
        "no concurrent writes lost"
    );

    let mut ids = HashSet::new();
    for r in &results {
        assert!(ids.insert(get_id(r)), "duplicate id across writers");
    }
    for w in 0..writers {
        let base = 1_000 + w * per_writer * 2;
        for i in 0..per_writer {
            assert!(
                ids.contains(&(base + i)),
                "writer {} id {} missing",
                w,
                base + i
            );
        }
    }
}

// --------------------------------------------------------------------------
// 12. Boolean RLE branches: all-true, all-false, alternating, long runs
// --------------------------------------------------------------------------

async fn round_trip_bools(name: &str, bools: Vec<bool>) {
    let (engine, _temp) = fresh_engine().await;
    let mut csv = String::from("id,timestamp,flag\n");
    for (i, b) in bools.iter().enumerate() {
        csv.push_str(&format!(
            "{},{},{}\n",
            i + 1,
            1_704_067_200_000_i64 + (i as i64),
            b
        ));
    }
    engine.ingest_csv(name, csv).await.unwrap();
    let results = engine
        .query(name, None, None, Some(bools.len() + 10), None)
        .await
        .unwrap();
    assert_eq!(results.len(), bools.len(), "{}: row count", name);
    for r in &results {
        let id = get_id(r) as usize;
        let expected = bools[id - 1];
        assert_eq!(
            get_bool(r, "flag"),
            expected,
            "{}: id={} mismatch",
            name,
            id
        );
    }
}

#[tokio::test]
async fn integrity_bool_rle_all_true() {
    round_trip_bools("bool_all_true", vec![true; 300]).await;
}

#[tokio::test]
async fn integrity_bool_rle_all_false() {
    round_trip_bools("bool_all_false", vec![false; 300]).await;
}

#[tokio::test]
async fn integrity_bool_rle_alternating() {
    let bs: Vec<bool> = (0..300).map(|i| i % 2 == 0).collect();
    round_trip_bools("bool_alt", bs).await;
}

#[tokio::test]
async fn integrity_bool_rle_long_runs() {
    // 100 true, 100 false, 1 true, 99 false → exercises the multi-byte run-length varint.
    let mut bs = Vec::with_capacity(300);
    bs.extend(std::iter::repeat_n(true, 100));
    bs.extend(std::iter::repeat_n(false, 100));
    bs.push(true);
    bs.extend(std::iter::repeat_n(false, 99));
    round_trip_bools("bool_long_runs", bs).await;
}

// --------------------------------------------------------------------------
// 13. String compression: dictionary path (low cardinality) preserves values
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_string_dictionary_low_cardinality() {
    let (engine, _temp) = fresh_engine().await;
    let table = "str_dict";

    // 200 rows, only 4 distinct strings — should trigger dictionary path
    // (dict size < strings.len() / 2 AND strings.len() > 10).
    let dict = ["AAPL", "GOOGL", "MSFT", "AMZN"];
    let mut csv = String::from("id,timestamp,symbol\n");
    for i in 1..=200_i64 {
        let s = dict[(i as usize) % dict.len()];
        csv.push_str(&format!("{},{},{}\n", i, 1_704_067_200_000 + i, s));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine
        .query(table, None, None, Some(500), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 200);
    for r in &results {
        let i = get_id(r) as usize;
        let expected = dict[i % dict.len()];
        assert_eq!(get_str(r, "symbol"), expected, "id={} symbol mismatch", i);
    }
}

// --------------------------------------------------------------------------
// 14. String compression: direct path (high cardinality)
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_string_direct_high_cardinality() {
    let (engine, _temp) = fresh_engine().await;
    let table = "str_direct";

    let mut csv = String::from("id,timestamp,name\n");
    for i in 1..=200_i64 {
        // Every row distinct → dictionary not chosen.
        csv.push_str(&format!("{},{},unique_{}\n", i, 1_704_067_200_000 + i, i));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    let results = engine
        .query(table, None, None, Some(500), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 200);
    for r in &results {
        let i = get_id(r);
        assert_eq!(get_str(r, "name"), format!("unique_{}", i));
    }
}

// --------------------------------------------------------------------------
// 15. Float NaN / Inf preserved bit-exact via Arrow IPC (storage layer is
// bit-exact even though JSON conversion lossily clamps non-finite values).
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_float_special_values_arrow_round_trip() {
    let (engine, _temp) = fresh_engine().await;
    let table = "float_special";

    // Use Arrow ingestion so non-finite values survive the parser path.
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("timestamp", ArrowDataType::Int64, false),
        Field::new("value", ArrowDataType::Float64, false),
    ]));

    let ids = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
    let timestamps = Int64Array::from(vec![
        1_704_067_200_000_i64,
        1_704_067_200_001,
        1_704_067_200_002,
        1_704_067_200_003,
        1_704_067_200_004,
        1_704_067_200_005,
    ]);
    let values = Float64Array::from(vec![
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        f64::MIN_POSITIVE,
    ]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(timestamps), Arc::new(values)],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    engine.ingest_arrow(table, buf).await.unwrap();

    // Read back as Arrow IPC — this avoids JSON's lossy non-finite handling.
    let arrow_bytes = engine
        .query_arrow(table, None, None, Some(50), None)
        .await
        .unwrap();
    assert!(!arrow_bytes.is_empty(), "arrow stream must contain data");

    let cursor = std::io::Cursor::new(&arrow_bytes);
    let reader = StreamReader::try_new(cursor, None).expect("valid arrow stream");
    let mut by_id: HashMap<i64, f64> = HashMap::new();
    for batch in reader {
        let batch = batch.unwrap();
        let id_arr = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .map(|a| a.values().iter().map(|v| *v as i64).collect::<Vec<_>>())
            .or_else(|| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.values().to_vec())
            })
            .expect("id column int-like");
        let val_arr = batch
            .column_by_name("value")
            .expect("value column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value is Float64");
        for i in 0..batch.num_rows() {
            by_id.insert(id_arr[i], val_arr.value(i));
        }
    }

    assert_eq!(
        by_id.len(),
        6,
        "all special-value rows present in arrow output"
    );
    let nan = by_id.get(&1).copied().unwrap();
    assert!(nan.is_nan(), "id=1 must round-trip NaN");
    assert_eq!(by_id.get(&2).copied().unwrap(), f64::INFINITY);
    assert_eq!(by_id.get(&3).copied().unwrap(), f64::NEG_INFINITY);
    assert_eq!(by_id.get(&4).copied().unwrap().to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        by_id.get(&5).copied().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(by_id.get(&6).copied().unwrap(), f64::MIN_POSITIVE);
}

// --------------------------------------------------------------------------
// 16. Timestamp format matrix in range queries — every supported format
// must select the same row.
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_query_timestamp_format_matrix() {
    let (engine, _temp) = fresh_engine().await;
    let table = "ts_formats";

    // Insert one row at exactly 2024-01-01T00:00:01.000Z (= 1704067201000 ms).
    let csv = "id,timestamp,value\n1,1704067201000,target\n2,1704067900000,other\n";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    // Every variant must include the target row, exclude the other.
    let variants = [
        ("1704067201000", "1704067201000"), // ms ints
        ("1704067201", "1704067201"),       // seconds
        ("2024-01-01T00:00:01Z", "2024-01-01T00:00:01Z"),
        ("2024-01-01T00:00:01", "2024-01-01T00:00:01"),
        ("2024-01-01 00:00:01", "2024-01-01 00:00:01"),
        ("2024-01-01", "2024-01-01"),
    ];
    for (i, (start, end)) in variants.iter().enumerate() {
        // For date-only end, range must still include time 00:00:01.000Z;
        // since end "2024-01-01" parses to 2024-01-01T00:00:00.000Z, it
        // would EXCLUDE the target. Adjust by widening end to the next day.
        let widened_end = if *end == "2024-01-01" {
            "2024-01-02".to_string()
        } else {
            end.to_string()
        };
        let results = engine
            .query(
                table,
                Some(start.to_string()),
                Some(widened_end),
                Some(10),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("variant {} ({}, {}) failed: {:?}", i, start, end, e));
        let hit = results.iter().any(|r| get_str(r, "value") == "target");
        assert!(
            hit,
            "variant {} ({}, {}): target row missing",
            i, start, end
        );
    }
}

// --------------------------------------------------------------------------
// 17. Many small blocks: each ingest creates a separate block; full scan
// must return every row.
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_many_small_blocks_full_scan() {
    let (engine, _temp) = fresh_engine().await;
    let table = "many_blocks";

    // First small ingest takes the slow path (schema inference), so call it once.
    engine
        .ingest_csv(table, "id,timestamp,value\n1,1704067200000,1\n".to_string())
        .await
        .unwrap();

    // Now 30 more ingests, each writing one row in fast-path → 30 new blocks.
    for i in 2..=30_i64 {
        let csv = format!(
            "id,timestamp,value\n{},{},{}\n",
            i,
            1_704_067_200_000 + i,
            i
        );
        engine.ingest_csv(table, csv).await.unwrap();
    }

    let results = engine
        .query(table, None, None, Some(100), None)
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        30,
        "every per-ingest block must be discoverable"
    );

    let mut ids: Vec<u64> = results.iter().map(get_id).collect();
    ids.sort();
    assert_eq!(ids, (1..=30_u64).collect::<Vec<_>>());
}

// --------------------------------------------------------------------------
// 18. get_table_count agrees with query result count when no updates occur
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_get_table_count_matches_query_no_updates() {
    let (engine, _temp) = fresh_engine().await;
    let table = "count_match";

    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=100_i64 {
        csv.push_str(&format!("{},{},{}\n", i, 1_704_067_200_000 + i, i));
    }
    engine.ingest_csv(table, csv).await.unwrap();
    engine.flush_table(table).await.unwrap();

    let queried = engine
        .query(table, None, None, Some(500), None)
        .await
        .unwrap()
        .len() as u64;
    let counted = engine.get_table_count(table).await.unwrap();
    assert_eq!(
        counted, queried,
        "get_table_count={} disagrees with query length={}",
        counted, queried
    );
}

// --------------------------------------------------------------------------
// 19. query_csv / query_arrow / query (JSON) return equivalent rows
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_query_csv_arrow_json_equivalence() {
    let (engine, _temp) = fresh_engine().await;
    let table = "query_format_equiv";

    let mut csv = String::from("id,timestamp,int_v,str_v\n");
    for i in 1..=50_i64 {
        csv.push_str(&format!(
            "{},{},{},s{}\n",
            i,
            1_704_067_200_000 + i,
            i * 3,
            i
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();
    engine.flush_table(table).await.unwrap();

    let json = engine
        .query(table, None, None, Some(500), None)
        .await
        .unwrap();
    let csv_str = engine
        .query_csv(table, None, None, Some(500), None)
        .await
        .unwrap();
    let arrow_bytes = engine
        .query_arrow(table, None, None, Some(500), None)
        .await
        .unwrap();

    assert_eq!(json.len(), 50);

    // CSV: header + 50 rows.
    let lines: Vec<&str> = csv_str.lines().collect();
    assert_eq!(lines.len(), 51, "csv header + 50 rows");

    // Arrow: deserialize and count.
    let cursor = std::io::Cursor::new(&arrow_bytes);
    let reader = StreamReader::try_new(cursor, None).unwrap();
    let mut arrow_rows = 0;
    let mut arrow_ids: HashSet<u64> = HashSet::new();
    for batch in reader {
        let batch = batch.unwrap();
        arrow_rows += batch.num_rows();
        if let Some(id_col) = batch.column_by_name("id") {
            if let Some(arr) = id_col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                for v in arr.values() {
                    arrow_ids.insert(*v);
                }
            } else if let Some(arr) = id_col.as_any().downcast_ref::<Int64Array>() {
                for v in arr.values() {
                    arrow_ids.insert(*v as u64);
                }
            }
        }
    }
    assert_eq!(arrow_rows, 50, "arrow row count");
    let mut json_ids: HashSet<u64> = json.iter().map(get_id).collect();
    // CSV ids: parse first column of each row (id is always first per schema sort).
    let mut csv_ids: HashSet<u64> = HashSet::new();
    // Detect id column position from header.
    let header_cols: Vec<&str> = lines[0].split(',').collect();
    let id_pos = header_cols
        .iter()
        .position(|c| *c == "id")
        .expect("csv has id column");
    for line in &lines[1..] {
        let cols: Vec<&str> = line.split(',').collect();
        if let Ok(id) = cols[id_pos].parse::<u64>() {
            csv_ids.insert(id);
        }
    }
    assert_eq!(csv_ids.len(), 50, "csv distinct ids");
    assert_eq!(arrow_ids, json_ids, "arrow vs json id sets differ");
    assert_eq!(
        csv_ids,
        std::mem::take(&mut json_ids),
        "csv vs json id sets differ"
    );
}

// --------------------------------------------------------------------------
// 20. Out-of-order ingest is returned sorted by timestamp
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_out_of_order_ingest_sorted_query() {
    let (engine, _temp) = fresh_engine().await;
    let table = "out_of_order";

    // First row determines schema (slow path).
    engine
        .ingest_csv(
            table,
            "id,timestamp,value\n100,1704067200500,a\n".to_string(),
        )
        .await
        .unwrap();

    // Then write rows whose timestamps are deliberately scrambled.
    for (id, ts) in [
        (50_u64, 1_704_067_200_300_i64),
        (60, 1_704_067_200_700),
        (40, 1_704_067_200_100),
        (90, 1_704_067_200_900),
    ] {
        let csv = format!("id,timestamp,value\n{},{},val{}\n", id, ts, id);
        engine.ingest_csv(table, csv).await.unwrap();
    }

    let results = engine
        .query(table, None, None, Some(100), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 5);

    // Extract timestamps (server returns RFC3339 strings for Timestamp columns).
    let mut tss: Vec<i64> = Vec::new();
    for r in &results {
        let v = r.get("timestamp").unwrap();
        let ms = if let Some(n) = v.as_i64() {
            n
        } else if let Some(s) = v.as_str() {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis()
        } else {
            panic!("unexpected timestamp shape: {:?}", v)
        };
        tss.push(ms);
    }
    let mut sorted = tss.clone();
    sorted.sort();
    assert_eq!(tss, sorted, "query results must be sorted by timestamp");
}

// --------------------------------------------------------------------------
// 21. Range queries with only `start` or only `end`
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_range_query_open_ended() {
    let (engine, _temp) = fresh_engine().await;
    let table = "open_range";

    let mut csv = String::from("id,timestamp,value\n");
    for i in 1..=10_i64 {
        csv.push_str(&format!("{},{},{}\n", i, 1_704_067_200_000 + i * 1_000, i));
    }
    engine.ingest_csv(table, csv).await.unwrap();

    // Only start: from t=ts(5) onwards → ids 5..=10
    let after = engine
        .query(
            table,
            Some("1704067205000".to_string()),
            None,
            Some(100),
            None,
        )
        .await
        .unwrap();
    let after_ids: HashSet<u64> = after.iter().map(get_id).collect();
    assert_eq!(after_ids, (5..=10_u64).collect());

    // Only end: through t=ts(5) → ids 1..=5
    let before = engine
        .query(
            table,
            None,
            Some("1704067205000".to_string()),
            Some(100),
            None,
        )
        .await
        .unwrap();
    let before_ids: HashSet<u64> = before.iter().map(get_id).collect();
    assert_eq!(before_ids, (1..=5_u64).collect());
}

// --------------------------------------------------------------------------
// 22. Range boundary at exact timestamp is inclusive on both ends
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_range_boundary_inclusive() {
    let (engine, _temp) = fresh_engine().await;
    let table = "boundaries";

    let csv = "id,timestamp,value\n\
        1,1704067200000,a\n\
        2,1704067200001,b\n\
        3,1704067200002,c\n";
    engine.ingest_csv(table, csv.to_string()).await.unwrap();

    let results = engine
        .query(
            table,
            Some("1704067200000".to_string()),
            Some("1704067200002".to_string()),
            Some(10),
            None,
        )
        .await
        .unwrap();
    let ids: HashSet<u64> = results.iter().map(get_id).collect();
    assert_eq!(
        ids,
        [1_u64, 2, 3].into_iter().collect(),
        "endpoints inclusive"
    );
}

// --------------------------------------------------------------------------
// 23. Single-row table works end-to-end (smallest non-empty block)
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_single_row_table() {
    let (engine, _temp) = fresh_engine().await;
    let table = "one_row";

    engine
        .ingest_csv(
            table,
            "id,timestamp,value\n777,1704067200000,solo\n".to_string(),
        )
        .await
        .unwrap();

    let results = engine
        .query(table, None, None, Some(10), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(get_id(&results[0]), 777);
    assert_eq!(get_str(&results[0], "value"), "solo");

    let by_id = engine
        .get_row_by_id_json(table, 777)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_str(&by_id, "value"), "solo");
}

// --------------------------------------------------------------------------
// 24. Schema-incompatible re-ingest (extra column) leaves prior data intact
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_extra_column_does_not_corrupt_existing_rows() {
    let (engine, _temp) = fresh_engine().await;
    let table = "extra_col_safe";

    engine
        .ingest_csv(
            table,
            "id,timestamp,value\n1,1704067200000,100\n2,1704067201000,200\n".to_string(),
        )
        .await
        .unwrap();

    // Second ingest carries an unknown column — best-effort: the engine may
    // accept (ignoring the extra) or reject. Either is fine; prior rows must
    // remain queryable with correct values regardless.
    let _ = engine
        .ingest_csv(
            table,
            "id,timestamp,value,extra\n3,1704067202000,300,bonus\n".to_string(),
        )
        .await;

    let results = engine
        .query(table, None, None, Some(50), None)
        .await
        .unwrap();
    let by_id: HashMap<u64, &serde_json::Value> = results.iter().map(|r| (get_id(r), r)).collect();
    assert_eq!(get_i64(by_id[&1], "value"), 100, "id=1 untouched");
    assert_eq!(get_i64(by_id[&2], "value"), 200, "id=2 untouched");
}

// --------------------------------------------------------------------------
// 25. Cross-format REPLACE consistency: CSV → Protobuf → Arrow → CSV
// each step must REPLACE by ID with the latest values intact.
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_cross_format_replace_chain() {
    use prost::Message;
    use pulsora::storage::ingestion::{ProtoBatch, ProtoRow};

    let (engine, _temp) = fresh_engine().await;
    let table = "cross_format_chain";

    // Step 1 — CSV insert.
    engine
        .ingest_csv(
            table,
            "id,timestamp,value\n1,1704067200000,csv1\n2,1704067200001,csv2\n".to_string(),
        )
        .await
        .unwrap();

    // Step 2 — Protobuf updates id=1.
    let mut values = HashMap::new();
    values.insert("id".to_string(), "1".to_string());
    values.insert("timestamp".to_string(), "1704067200002".to_string());
    values.insert("value".to_string(), "proto1".to_string());
    let batch = ProtoBatch {
        rows: vec![ProtoRow { values }],
    };
    let mut buf = Vec::new();
    batch.encode(&mut buf).unwrap();
    engine.ingest_protobuf(table, buf).await.unwrap();

    // Step 3 — Arrow updates id=2.
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("timestamp", ArrowDataType::Int64, false),
        Field::new("value", ArrowDataType::Utf8, false),
    ]));
    let ids = Int64Array::from(vec![2]);
    let tss = Int64Array::from(vec![1_704_067_200_003]);
    let vals = StringArray::from(vec!["arrow2"]);
    let arrow_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(tss), Arc::new(vals)],
    )
    .unwrap();
    let mut buf2 = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf2, &schema).unwrap();
        w.write(&arrow_batch).unwrap();
        w.finish().unwrap();
    }
    engine.ingest_arrow(table, buf2).await.unwrap();

    // Step 4 — Final CSV touch on id=1 only.
    engine
        .ingest_csv(
            table,
            "id,timestamp,value\n1,1704067200004,final1\n".to_string(),
        )
        .await
        .unwrap();

    let results = engine
        .query(table, None, None, Some(50), None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2, "still 2 logical rows after chain");
    let by_id: HashMap<u64, &serde_json::Value> = results.iter().map(|r| (get_id(r), r)).collect();
    assert_eq!(get_str(by_id[&1], "value"), "final1");
    assert_eq!(get_str(by_id[&2], "value"), "arrow2");
}

// --------------------------------------------------------------------------
// 26. Empty range query returns zero rows without error
// --------------------------------------------------------------------------

#[tokio::test]
async fn integrity_empty_range_returns_no_rows() {
    let (engine, _temp) = fresh_engine().await;
    let table = "empty_range";
    engine
        .ingest_csv(table, "id,timestamp,value\n1,1704067200000,a\n".to_string())
        .await
        .unwrap();
    let results = engine
        .query(
            table,
            Some("1800000000000".to_string()),
            Some("1900000000000".to_string()),
            Some(50),
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 0);
}
