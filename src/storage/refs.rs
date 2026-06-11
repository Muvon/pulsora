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

//! Compact storage references and block keys.
//!
//! Per-row index entries dominate database size: every row stores its
//! (block, row) location twice — once under its id-key and once under its
//! time-key. The previous format embedded a ~54-byte string block id
//! (`_block_{table}_{uuid}`) in every reference (~63 bytes per ref, ~158
//! bytes of index per row — ~10x the compressed row data itself). This
//! module replaces it with fixed-size binary encodings:
//!
//! ```text
//! ref value:       [0xFF][block: u64 BE][row: u32 BE]                  13 B
//! block data key:  [hash: u32 BE]['D'][block: u64 BE]                  13 B
//! block index key: [hash: u32 BE]['B'][min_ts: i64 BE][block: u64 BE]  21 B
//! block index val: [block: u64 LE][min_ts: i64 LE][max_ts: i64 LE][rows: u32 LE]  28 B
//! ```
//!
//! Keys stay byte-ordered (RocksDB applies prefix-delta compression inside
//! SST blocks, so shared `[hash][ts]` prefixes cost almost nothing), and all
//! parses are fixed-offset — no length prefixes, no string handling.
//!
//! Block ids come from a process-wide persisted sequence (key `_block_seq`),
//! persisted BEFORE first use so a crash can never reuse an id.

use crate::error::{PulsoraError, Result};
use rocksdb::DB;
use std::sync::Mutex;

/// Marker byte distinguishing row references from other values when scanning
/// mixed keyspaces (e.g. get_table_count over id-keys).
pub const REF_MARKER: u8 = 0xFF;

/// Fixed reference length: marker + block u64 + row u32.
pub const REF_LEN: usize = 13;

/// Fixed block index value length: block + min_ts + max_ts + row_count.
pub const BLOCK_INDEX_VALUE_LEN: usize = 28;

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

/// `[0xFF][block: u64 BE][row: u32 BE]`
pub fn encode_ref(block: u64, row: u32) -> [u8; REF_LEN] {
    let mut out = [0u8; REF_LEN];
    out[0] = REF_MARKER;
    out[1..9].copy_from_slice(&block.to_be_bytes());
    out[9..13].copy_from_slice(&row.to_be_bytes());
    out
}

/// Parse a row reference into (block, row).
pub fn parse_ref(data: &[u8]) -> Result<(u64, u32)> {
    if data.len() != REF_LEN || data[0] != REF_MARKER {
        return Err(PulsoraError::Internal(format!(
            "invalid row reference ({} bytes)",
            data.len()
        )));
    }
    let block = u64::from_be_bytes(data[1..9].try_into().unwrap());
    let row = u32::from_be_bytes(data[9..13].try_into().unwrap());
    Ok((block, row))
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

/// `[block: u64 LE][min_ts: i64 LE][max_ts: i64 LE][rows: u32 LE]`
pub fn encode_block_index_value(
    block: u64,
    min_ts: i64,
    max_ts: i64,
    rows: u32,
) -> [u8; BLOCK_INDEX_VALUE_LEN] {
    let mut out = [0u8; BLOCK_INDEX_VALUE_LEN];
    out[0..8].copy_from_slice(&block.to_le_bytes());
    out[8..16].copy_from_slice(&min_ts.to_le_bytes());
    out[16..24].copy_from_slice(&max_ts.to_le_bytes());
    out[24..28].copy_from_slice(&rows.to_le_bytes());
    out
}

/// Parse a block index value into (block, min_ts, max_ts, row_count).
pub fn parse_block_index_value(data: &[u8]) -> Result<(u64, i64, i64, u32)> {
    if data.len() != BLOCK_INDEX_VALUE_LEN {
        return Err(PulsoraError::Internal(format!(
            "invalid block index value ({} bytes)",
            data.len()
        )));
    }
    Ok((
        u64::from_le_bytes(data[0..8].try_into().unwrap()),
        i64::from_le_bytes(data[8..16].try_into().unwrap()),
        i64::from_le_bytes(data[16..24].try_into().unwrap()),
        u32::from_le_bytes(data[24..28].try_into().unwrap()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_roundtrip() {
        let encoded = encode_ref(987654321, 4242);
        assert_eq!(encoded.len(), REF_LEN);
        assert_eq!(parse_ref(&encoded).unwrap(), (987654321, 4242));
    }

    #[test]
    fn ref_rejects_garbage() {
        assert!(parse_ref(&[0u8; 13]).is_err());
        assert!(parse_ref(&[0xFF, 1, 2]).is_err());
    }

    #[test]
    fn block_index_value_roundtrip() {
        let encoded = encode_block_index_value(7, -5, 12345678901234, 999);
        assert_eq!(
            parse_block_index_value(&encoded).unwrap(),
            (7, -5, 12345678901234, 999)
        );
    }
}
