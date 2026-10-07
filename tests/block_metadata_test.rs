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

//! Reads resolve blocks from the in-memory block metadata instead of walking
//! the on-disk block index: time-range queries and id lookups must see every
//! block, the latest REPLACE copy, and the same data after a reopen.

use pulsora::config::Config;
use pulsora::storage::StorageEngine;
use tempfile::TempDir;

const BASE_TS: i64 = 1_704_067_200_000;

async fn open_engine(dir: &TempDir) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    StorageEngine::new(&config).await.unwrap()
}

/// One flushed block per call: ids `first..first+count`, one second apart.
async fn write_block(engine: &StorageEngine, table: &str, first: u64, count: u64, value: i64) {
    let mut csv = String::from("id,timestamp,value\n");
    for id in first..first + count {
        csv.push_str(&format!(
            "{},{},{}\n",
            id,
            BASE_TS + id as i64 * 1000,
            value
        ));
    }
    engine.ingest_csv(table, csv).await.unwrap();
    engine.flush_table(table).await.unwrap();
}

async fn ids_and_values(
    engine: &StorageEngine,
    table: &str,
    from_id: u64,
    to_id: u64,
) -> Vec<(u64, i64)> {
    let rows = engine
        .query(
            table,
            Some((BASE_TS + from_id as i64 * 1000).to_string()),
            Some((BASE_TS + to_id as i64 * 1000).to_string()),
            Some(100_000),
            None,
        )
        .await
        .unwrap();
    rows.iter()
        .map(|r| (r["id"].as_u64().unwrap(), r["value"].as_i64().unwrap()))
        .collect()
}

async fn value_of(engine: &StorageEngine, table: &str, id: u64) -> Option<i64> {
    engine
        .get_row_by_id_json(table, id)
        .await
        .unwrap()
        .map(|row| row["value"].as_i64().unwrap())
}

#[tokio::test]
async fn test_range_and_id_reads_across_many_blocks_with_replace() {
    let dir = TempDir::new().unwrap();
    let engine = open_engine(&dir).await;
    let table = "block_meta_reads";

    for block in 0..40u64 {
        write_block(&engine, table, block * 10 + 1, 10, 1).await;
    }
    // REPLACE a range spanning two old blocks.
    write_block(&engine, table, 95, 10, 2).await;

    // A narrow recent window returns exactly its rows, at the latest values.
    let window = ids_and_values(&engine, table, 300, 320).await;
    assert_eq!(window, (300..=320).map(|id| (id, 1)).collect::<Vec<_>>());
    let replaced = ids_and_values(&engine, table, 90, 110).await;
    let expected: Vec<(u64, i64)> = (90..=110)
        .map(|id| (id, if (95..105).contains(&id) { 2 } else { 1 }))
        .collect();
    assert_eq!(replaced, expected);

    // The full range holds every id once.
    let all = ids_and_values(&engine, table, 0, 1000).await;
    assert_eq!(all.len(), 400);

    assert_eq!(value_of(&engine, table, 100).await, Some(2));
    assert_eq!(value_of(&engine, table, 1).await, Some(1));
    assert_eq!(value_of(&engine, table, 400).await, Some(1));
    assert_eq!(value_of(&engine, table, 401).await, None);
}

#[tokio::test]
async fn test_reads_after_reopen_see_all_blocks() {
    let dir = TempDir::new().unwrap();
    let table = "block_meta_reopen";
    {
        let engine = open_engine(&dir).await;
        for block in 0..5u64 {
            write_block(&engine, table, block * 10 + 1, 10, 1).await;
        }
    }

    let engine = open_engine(&dir).await;
    write_block(&engine, table, 51, 10, 3).await;
    write_block(&engine, table, 5, 3, 4).await;

    let all = ids_and_values(&engine, table, 0, 1000).await;
    assert_eq!(all.len(), 60);
    assert_eq!(value_of(&engine, table, 6).await, Some(4));
    assert_eq!(value_of(&engine, table, 30).await, Some(1));
    assert_eq!(value_of(&engine, table, 60).await, Some(3));
}
