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

//! Compact block keys and per-block override sets.
//!
//! There are NO per-row index entries: a row's identity and liveness are
//! per-block facts. Each block carries its id range in the block index, and
//! a (usually absent) override set lists the row positions superseded by
//! later REPLACE ingests. Per-row entries would dominate database size —
//! billions of rows but only hundreds of thousands of blocks — so all
//! bookkeeping lives at block granularity:
//!
//! ```text
//! block data key:  [hash: u32 BE]['D'][block: u64 BE]                  13 B
//! block index key: [hash: u32 BE]['B'][min_ts: i64 BE][block: u64 BE]  21 B
//! block index val: [block u64][min_ts i64][max_ts i64][rows u32][min_id u64][max_id u64] LE  44 B
//! override key:    [hash: u32 BE]['O'][block: u64 BE]                  13 B
//! override val:    concatenated [row: u32 LE] dead positions (merged, deduped on full merge)
//! ```
//!
//! Override values are written with RocksDB `merge` (associative union), so
//! concurrent REPLACE batches against the same old block cannot lose
//! updates. Readers must treat the position list as a set — partial merges
//! may leave duplicates.
//!
//! Keys stay byte-ordered (RocksDB applies prefix-delta compression inside
//! SST blocks, so shared `[hash][ts]` prefixes cost almost nothing), and all
//! parses are fixed-offset — no length prefixes, no string handling.
//!
//! Block ids come from a process-wide persisted sequence (key `_block_seq`),
//! persisted BEFORE first use so a crash can never reuse an id. They are
//! strictly increasing, so a larger block id always means a LATER write —
//! point lookups scan candidate blocks newest-first and stop at the first
//! live hit.

use crate::error::{PulsoraError, Result};
use rocksdb::DB;
use std::sync::Mutex;

/// Fixed block index value length:
/// block + min_ts + max_ts + row_count + min_id + max_id.
pub const BLOCK_INDEX_VALUE_LEN: usize = 44;

const BLOCK_SEQ_KEY: &[u8] = b"_block_seq";

static BLOCK_SEQ: Mutex<u64> = Mutex::new(0);

/// Allocate the next block id. Crash-safe: the advanced sequence is persisted
/// before the id is handed out, so restarts can never reuse one. One read and
/// one write per BLOCK (not per row) — negligible against block payload IO.
pub fn allocate_block_id(db: &DB) -> Result<u64> {
    let mut guard = BLOCK_SEQ
        .lock()
        .map_err(|_| PulsoraError::Internal("block sequence lock poisoned".to_string()))?;

    let persisted = db
        .get(BLOCK_SEQ_KEY)
        .map_err(PulsoraError::RocksDb)?
        .and_then(|v| v.as_slice().try_into().ok().map(u64::from_be_bytes))
        .unwrap_or(0);

    let next = (*guard).max(persisted) + 1;
    db.put(BLOCK_SEQ_KEY, next.to_be_bytes())
        .map_err(PulsoraError::RocksDb)?;
    *guard = next;
    Ok(next)
}

/// `[hash: u32 BE]['D'][block: u64 BE]` — where the serialized block lives.
pub fn block_data_key(table_hash: u32, block: u64) -> [u8; 13] {
    let mut out = [0u8; 13];
    out[0..4].copy_from_slice(&table_hash.to_be_bytes());
    out[4] = b'D';
    out[5..13].copy_from_slice(&block.to_be_bytes());
    out
}

/// `[hash: u32 BE]['B'][min_ts: i64 BE][block: u64 BE]`
pub fn block_index_key(table_hash: u32, min_ts: i64, block: u64) -> [u8; 21] {
    let mut out = [0u8; 21];
    out[0..4].copy_from_slice(&table_hash.to_be_bytes());
    out[4] = b'B';
    out[5..13].copy_from_slice(&min_ts.to_be_bytes());
    out[13..21].copy_from_slice(&block.to_be_bytes());
    out
}

/// Block index metadata: time range, row count and id range of one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockMeta {
    pub block: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u32,
    pub min_id: u64,
    pub max_id: u64,
}

/// `[block u64][min_ts i64][max_ts i64][rows u32][min_id u64][max_id u64]` LE
pub fn encode_block_index_value(meta: &BlockMeta) -> [u8; BLOCK_INDEX_VALUE_LEN] {
    let mut out = [0u8; BLOCK_INDEX_VALUE_LEN];
    out[0..8].copy_from_slice(&meta.block.to_le_bytes());
    out[8..16].copy_from_slice(&meta.min_ts.to_le_bytes());
    out[16..24].copy_from_slice(&meta.max_ts.to_le_bytes());
    out[24..28].copy_from_slice(&meta.rows.to_le_bytes());
    out[28..36].copy_from_slice(&meta.min_id.to_le_bytes());
    out[36..44].copy_from_slice(&meta.max_id.to_le_bytes());
    out
}

/// Parse a block index value.
pub fn parse_block_index_value(data: &[u8]) -> Result<BlockMeta> {
    if data.len() != BLOCK_INDEX_VALUE_LEN {
        return Err(PulsoraError::Internal(format!(
            "invalid block index value ({} bytes)",
            data.len()
        )));
    }
    Ok(BlockMeta {
        block: u64::from_le_bytes(data[0..8].try_into().unwrap()),
        min_ts: i64::from_le_bytes(data[8..16].try_into().unwrap()),
        max_ts: i64::from_le_bytes(data[16..24].try_into().unwrap()),
        rows: u32::from_le_bytes(data[24..28].try_into().unwrap()),
        min_id: u64::from_le_bytes(data[28..36].try_into().unwrap()),
        max_id: u64::from_le_bytes(data[36..44].try_into().unwrap()),
    })
}

/// `[hash: u32 BE]['O'][block: u64 BE]` — the block's override (dead-row) set.
pub fn override_key(table_hash: u32, block: u64) -> [u8; 13] {
    let mut out = [0u8; 13];
    out[0..4].copy_from_slice(&table_hash.to_be_bytes());
    out[4] = b'O';
    out[5..13].copy_from_slice(&block.to_be_bytes());
    out
}

/// Encode row positions as a mergeable override operand (`[row: u32 LE]`*).
pub fn encode_override_positions(positions: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(positions.len() * 4);
    for &pos in positions {
        out.extend_from_slice(&pos.to_le_bytes());
    }
    out
}

/// Decode an override value into the set of dead row positions. Tolerates
/// duplicates (concatenated merge operands) by virtue of returning a set.
pub fn decode_override_positions(data: &[u8]) -> std::collections::HashSet<u32> {
    let mut out = std::collections::HashSet::with_capacity(data.len() / 4);
    for chunk in data.chunks_exact(4) {
        out.insert(u32::from_le_bytes(chunk.try_into().unwrap()));
    }
    out
}

/// Associative merge for override values: union of dead positions.
/// Registered as the database-wide merge operator — `merge()` is only ever
/// issued against `'O'` keys, so the union semantics never touch other data.
/// Deduplicates on every merge so values stay bounded by the block row count.
pub fn override_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let mut set = existing.map(decode_override_positions).unwrap_or_default();
    for op in operands {
        set.extend(decode_override_positions(op));
    }
    let mut positions: Vec<u32> = set.into_iter().collect();
    positions.sort_unstable();
    Some(encode_override_positions(&positions))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_index_value_roundtrip() {
        let meta = BlockMeta {
            block: 7,
            min_ts: -5,
            max_ts: 12345678901234,
            rows: 999,
            min_id: 42,
            max_id: u64::MAX - 1,
        };
        assert_eq!(
            parse_block_index_value(&encode_block_index_value(&meta)).unwrap(),
            meta
        );
    }

    #[test]
    fn block_index_value_rejects_old_format() {
        assert!(parse_block_index_value(&[0u8; 28]).is_err());
    }

    #[test]
    fn override_positions_roundtrip_dedup() {
        let encoded = encode_override_positions(&[3, 1, 3, 7]);
        let set = decode_override_positions(&encoded);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&1) && set.contains(&3) && set.contains(&7));
    }
}
