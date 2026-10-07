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

use arrow::array::{Int64Array, UInt64Array};
use arrow::ipc::reader::StreamReader;
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

/// Metric frames are rewritten every minute until they close, leaving runs of
/// fully-overridden single-row blocks. Every read format must skip them and
/// return each frame's last copy, while a partially overridden block still
/// yields its live rows.
#[tokio::test]
async fn test_fully_overridden_blocks_read_as_latest_copy_in_every_format() {
    let dir = TempDir::new().unwrap();
    let engine = open_engine(&dir).await;
    let table = "block_meta_frames";

    for frame in 1..=3u64 {
        for rewrite in 1..=20 {
            write_block(&engine, table, frame, 1, rewrite).await;
        }
    }
    write_block(&engine, table, 10, 3, 5).await;
    write_block(&engine, table, 11, 1, 6).await;

    let expected = vec![(1, 20), (2, 20), (3, 20), (10, 5), (11, 6), (12, 5)];
    assert_eq!(ids_and_values(&engine, table, 0, 100).await, expected);

    let csv = engine
        .query_csv(table, None, None, Some(100), None)
        .await
        .unwrap();
    let mut lines = csv.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let id_at = header.iter().position(|c| *c == "id").unwrap();
    let value_at = header.iter().position(|c| *c == "value").unwrap();
    let mut from_csv: Vec<(u64, i64)> = lines
        .map(|line| {
            let fields: Vec<&str> = line.split(',').collect();
            (
                fields[id_at].parse().unwrap(),
                fields[value_at].parse().unwrap(),
            )
        })
        .collect();
    from_csv.sort_unstable();
    assert_eq!(from_csv, expected);

    let arrow_bytes = engine
        .query_arrow(table, None, None, Some(100), None)
        .await
        .unwrap();
    let reader = StreamReader::try_new(std::io::Cursor::new(&arrow_bytes), None).unwrap();
    let mut from_arrow: Vec<(u64, i64)> = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        from_arrow.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    from_arrow.sort_unstable();
    assert_eq!(from_arrow, expected);

    assert_eq!(value_of(&engine, table, 2).await, Some(20));
}

#[tokio::test]
async fn test_count_without_flush_is_exact_for_sparse_ids_and_replacements() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    let engine = StorageEngine::new(&config).await.unwrap();
    engine
        .ingest_csv(
            "sparse",
            format!(
                "id,timestamp,value\n1,{BASE_TS},10\n100,{},20\n",
                BASE_TS + 10000
            ),
        )
        .await
        .unwrap();
    engine.flush_table("sparse").await.unwrap();
    // The second block starts later but ends earlier than the first.
    engine
        .ingest_csv(
            "sparse",
            format!("id,timestamp,value\n101,{},30\n", BASE_TS + 1000),
        )
        .await
        .unwrap();
    engine.flush_table("sparse").await.unwrap();
    let disk_stats = engine.get_table_stats("sparse").await.unwrap();
    assert_eq!(disk_stats.max_ts, Some(BASE_TS + 10000));
    // Protobuf uses the buffering path even when the schema already exists;
    // CSV's established-schema path intentionally writes directly to disk.
    use prost::Message;
    use pulsora::storage::ingestion::{parse_csv, ProtoBatch, ProtoRow};
    let rows = parse_csv(&format!(
        "id,timestamp,value\n1,{},40\n50,{},50\n50,{},60\n",
        BASE_TS - 1000,
        BASE_TS + 20000,
        BASE_TS + 20000,
    ))
    .unwrap()
    .into_iter()
    .map(|values| ProtoRow { values })
    .collect();
    engine
        .ingest_protobuf("sparse", ProtoBatch { rows }.encode_to_vec())
        .await
        .unwrap();
    assert_eq!(engine.buffers.read().await["sparse"].rows.len(), 2);
    assert_eq!(block_count(&engine, "sparse"), 2);
    let stats = engine.get_table_stats("sparse").await.unwrap();
    assert_eq!(stats.count, 4);
    assert_eq!(stats.min_ts, Some(BASE_TS - 1000));
    assert_eq!(stats.max_ts, Some(BASE_TS + 20000));
    assert_eq!(engine.get_table_count("sparse").await.unwrap(), 4);
    assert_eq!(engine.buffers.read().await["sparse"].rows.len(), 2);
    assert_eq!(block_count(&engine, "sparse"), 2);
    assert_eq!(value_of(&engine, "sparse", 50).await, Some(60));
    engine.flush_table("sparse").await.unwrap();
    assert_eq!(engine.get_table_count("sparse").await.unwrap(), 4);
    assert!(engine.get_table_stats("missing").await.is_err());
}

fn block_count(engine: &StorageEngine, table: &str) -> usize {
    let mut prefix = pulsora::storage::calculate_table_hash(table)
        .to_be_bytes()
        .to_vec();
    prefix.push(b'B');
    engine
        .db
        .prefix_iterator(&prefix)
        .take_while(|entry| entry.as_ref().unwrap().0.starts_with(&prefix))
        .count()
}

#[tokio::test]
async fn test_compaction_preserves_old_data_replacements_snapshots_and_reopen() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    let engine = StorageEngine::new(&config).await.unwrap();
    write_block(&engine, "compact", 1, 3, 10).await;
    write_block(&engine, "compact", 1, 1, 20).await;
    write_block(&engine, "compact", 4, 2, 30).await;
    write_block(&engine, "compact", 4, 2, 40).await;
    let before = ids_and_values(&engine, "compact", 1, 5).await;
    assert_eq!(block_count(&engine, "compact"), 4);
    let hash = pulsora::storage::calculate_table_hash("compact");
    let old_key = engine
        .db
        .prefix_iterator([hash.to_be_bytes().as_slice(), b"B"].concat())
        .next()
        .unwrap()
        .unwrap()
        .0;
    let snapshot = engine.db.snapshot();
    assert_eq!(engine.compact_table("compact").await.unwrap(), 4);
    assert_eq!(block_count(&engine, "compact"), 1);
    assert!(engine.db.get(&old_key).unwrap().is_none());
    assert!(snapshot.get(&old_key).unwrap().is_some());
    drop(snapshot);
    assert_eq!(ids_and_values(&engine, "compact", 1, 5).await, before);
    assert_eq!(engine.get_table_count("compact").await.unwrap(), 5);
    assert_eq!(engine.compact_table("compact").await.unwrap(), 0);
    assert_eq!(value_of(&engine, "compact", 1).await, Some(20));
    // New ingestion must find the rewritten block and override its old copy.
    write_block(&engine, "compact", 2, 1, 99).await;
    assert_eq!(engine.get_table_count("compact").await.unwrap(), 5);
    assert_eq!(value_of(&engine, "compact", 2).await, Some(99));
    drop(engine);
    let reopened = StorageEngine::new(&config).await.unwrap();
    assert_eq!(reopened.get_table_count("compact").await.unwrap(), 5);
    assert_eq!(value_of(&reopened, "compact", 2).await, Some(99));
}

#[tokio::test]
async fn test_compaction_deletes_fully_dead_blocks_and_empty_bounds() {
    use pulsora::storage::refs;
    let dir = TempDir::new().unwrap();
    let engine = open_engine(&dir).await;
    write_block(&engine, "dead", 1, 2, 10).await;
    let hash = pulsora::storage::calculate_table_hash("dead");
    let prefix = [hash.to_be_bytes().as_slice(), b"B"].concat();
    let (_, value) = engine.db.prefix_iterator(prefix).next().unwrap().unwrap();
    let meta = refs::parse_block_index_value(&value).unwrap();
    engine
        .db
        .merge(
            refs::override_key(hash, meta.block),
            refs::encode_override_positions(&[0, 1]),
        )
        .unwrap();
    let stats = engine.get_table_stats("dead").await.unwrap();
    assert_eq!(stats.count, 0);
    assert_eq!((stats.min_ts, stats.max_ts), (None, None));
    assert_eq!(engine.compact_table("dead").await.unwrap(), 1);
    assert_eq!(block_count(&engine, "dead"), 0);
    assert!(engine
        .db
        .get(refs::override_key(hash, meta.block))
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_background_compaction_enabled_and_bounded() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    config.storage.compaction_interval_ms = 10;
    config.ingestion.batch_size = 3;
    let engine = StorageEngine::new(&config).await.unwrap();
    for id in 1..=3 {
        write_block(&engine, "scheduled", id, 1, 10).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while block_count(&engine, "scheduled") > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(engine.get_table_count("scheduled").await.unwrap(), 3);
    assert_eq!(ids_and_values(&engine, "scheduled", 1, 3).await.len(), 3);
    assert_eq!(engine.compact_table("scheduled").await.unwrap(), 0);
}

#[tokio::test]
async fn test_compaction_recomputes_live_timestamp_bounds_in_milliseconds() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    let engine = StorageEngine::new(&config).await.unwrap();
    engine
        .ingest_csv(
            "bounds",
            "id,timestamp,value\n1,1704067200,10\n2,1704153600,20\n".into(),
        )
        .await
        .unwrap();
    engine.flush_table("bounds").await.unwrap();
    engine
        .ingest_csv("bounds", "id,timestamp,value\n2,1704067201,30\n".into())
        .await
        .unwrap();
    engine.flush_table("bounds").await.unwrap();
    assert_eq!(
        engine.get_table_stats("bounds").await.unwrap().max_ts,
        Some(1704153600000)
    );
    engine.compact_table("bounds").await.unwrap();
    let stats = engine.get_table_stats("bounds").await.unwrap();
    assert_eq!(stats.count, 2);
    assert_eq!(stats.min_ts, Some(BASE_TS));
    assert_eq!(stats.max_ts, Some(BASE_TS + 1000));
}

#[tokio::test]
async fn test_count_and_compaction_without_timestamp_column() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    let engine = StorageEngine::new(&config).await.unwrap();
    for id in 1..=2 {
        engine
            .ingest_csv("untimed", format!("id,value\n{id},10\n"))
            .await
            .unwrap();
        let stats = engine.get_table_stats("untimed").await.unwrap();
        assert_eq!(stats.count, id);
        assert_eq!((stats.min_ts, stats.max_ts), (None, None));
        engine.flush_table("untimed").await.unwrap();
    }
    assert_eq!(engine.compact_table("untimed").await.unwrap(), 2);
    assert_eq!(engine.get_table_count("untimed").await.unwrap(), 2);
}
