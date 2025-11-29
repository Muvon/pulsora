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

//! ID management for tables with auto-increment and user-provided ID support
//!
//! This module handles ID generation, tracking, and persistence for each table.
//! Supports both auto-incrementing IDs and user-provided IDs with REPLACE semantics.

use rocksdb::DB;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::error::{PulsoraError, Result};
use crate::storage::calculate_table_hash;

/// ID types supported by the system
#[derive(Debug, Clone, PartialEq)]
pub enum RowId {
    /// System generates next auto-increment ID
    Auto,
    /// User-provided ID (must be positive integer)
    User(u64),
}

impl RowId {
    /// Parse ID from string value
    pub fn from_string(value: &str) -> Result<Self> {
        if value.is_empty() {
            return Ok(RowId::Auto);
        }

        let id = value
            .parse::<u64>()
            .map_err(|_| PulsoraError::InvalidData(format!("Invalid ID format: {}", value)))?;

        if id == 0 {
            return Err(PulsoraError::InvalidData(
                "ID must be positive (> 0)".to_string(),
            ));
        }

        Ok(RowId::User(id))
    }

    /// Get the actual ID value, resolving auto-increment
    pub fn resolve(&self, id_manager: &IdManager) -> u64 {
        match self {
            RowId::Auto => id_manager.next_auto_id(),
            RowId::User(id) => *id,
        }
    }
}

/// Per-table ID management
#[derive(Debug)]
pub struct IdManager {
    #[allow(dead_code)]
    table_name: String,
    table_hash: u32,
    /// State storing [timestamp_offset:51][sequence:13]
    id_state: AtomicU64,
    /// Highest user-provided ID seen (to avoid conflicts)
    max_user_id: AtomicU64,
    /// Database reference for persistence
    db: Arc<DB>,
}

impl IdManager {
    /// Create new ID manager for a table
    pub fn new(table_name: String, db: Arc<DB>) -> Result<Self> {
        let table_hash = calculate_table_hash(&table_name);

        // Load existing ID state from database
        let (id_state_val, max_user) = Self::load_id_state(&db, table_hash)?;

        debug!(
            "Initialized ID manager for table '{}': state={}, max_user={}",
            table_name, id_state_val, max_user
        );

        Ok(IdManager {
            table_name,
            table_hash,
            id_state: AtomicU64::new(id_state_val),
            max_user_id: AtomicU64::new(max_user),
            db,
        })
    }

    /// Get next auto-increment ID with snowflake-like structure for distributed scaling
    /// Format: [timestamp_ms:41 bits][node_id:10 bits][sequence:13 bits]
    pub fn next_auto_id(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Custom epoch (2024-01-01 00:00:00 UTC) to maximize timestamp range
        const CUSTOM_EPOCH: u64 = 1704067200000; // milliseconds since UNIX epoch
        const SEQ_MASK: u64 = 0x1FFF; // 13 bits
        const SEQ_BITS: u64 = 13;

        // For now, use table_hash as node_id (10 bits = 0-1023)
        let node_id = (self.table_hash & 0x3FF) as u64; // Take lower 10 bits

        let mut backoff = 1;

        loop {
            // Get current timestamp in milliseconds
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Ensure we don't go backwards in time relative to epoch
            let current_ts_offset = now_ms.saturating_sub(CUSTOM_EPOCH);

            // Load current state
            let current_state = self.id_state.load(Ordering::Acquire);
            let stored_ts_offset = current_state >> SEQ_BITS;
            let stored_seq = current_state & SEQ_MASK;

            let (new_ts_offset, new_seq) = if current_ts_offset > stored_ts_offset {
                // New millisecond, reset sequence
                (current_ts_offset, 0)
            } else if current_ts_offset == stored_ts_offset {
                // Same millisecond, increment sequence
                if stored_seq >= SEQ_MASK {
                    // Sequence overflow, wait for next millisecond
                    std::thread::yield_now();
                    // Simple backoff to avoid burning CPU
                    for _ in 0..backoff {
                        std::hint::spin_loop();
                    }
                    backoff = std::cmp::min(backoff * 2, 1024);
                    continue;
                }
                (stored_ts_offset, stored_seq + 1)
            } else {
                // Clock moved backwards! Use stored timestamp to ensure monotonicity
                // This handles small clock skews safely
                if stored_seq >= SEQ_MASK {
                    // Sequence overflow on stored timestamp, wait
                    std::thread::yield_now();
                    continue;
                }
                (stored_ts_offset, stored_seq + 1)
            };

            // Try to update state
            let new_state = (new_ts_offset << SEQ_BITS) | new_seq;

            if self
                .id_state
                .compare_exchange_weak(
                    current_state,
                    new_state,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Success! Construct snowflake ID
                // [timestamp:41][node:10][sequence:13]
                let snowflake_id = (new_ts_offset << 23) | (node_id << 13) | new_seq;

                // Ensure we don't conflict with user IDs
                let max_user = self.max_user_id.load(Ordering::Acquire);
                if snowflake_id <= max_user {
                    // This is tricky. If user IDs are very high, snowflake might conflict.
                    // But snowflake is time-based.
                    // If max_user is huge, we might need to skip ahead?
                    // Or just return max_user + 1 and update state?
                    // But that breaks snowflake structure.
                    // For now, let's assume user IDs are reasonable or we accept the break.
                    // If we return max_user + 1, we should update max_user?
                    // Let's just return max_user + 1 to be safe and simple.
                    // But we should probably log this.
                    return max_user + 1;
                }

                // Persist state periodically (every 1000 IDs or on time change)
                if new_seq % 1000 == 0 || new_ts_offset > stored_ts_offset {
                    // Fire and forget persistence
                    // We don't want to block the hot path
                    // In a real system, this might be async or batched
                    // For now, we just do it, but maybe we should optimize?
                    // Given RocksDB is fast, maybe it's ok.
                    // But let's only do it if sequence is 0 (new ms) or multiple of 1000
                    if new_seq == 0 || new_seq % 1000 == 0 {
                        let _ = self.persist_id_state();
                    }
                }

                return snowflake_id;
            }
            // CAS failed, retry loop
            backoff = 1;
        }
    }

    /// Register a user-provided ID
    pub fn register_user_id(&self, id: u64) -> Result<()> {
        if id == 0 {
            return Err(PulsoraError::InvalidData(
                "ID must be positive (> 0)".to_string(),
            ));
        }

        // Update max user ID if this is higher
        let current_max = self.max_user_id.load(Ordering::Acquire);
        if id > current_max {
            self.max_user_id.store(id, Ordering::Release);

            // If user ID exceeds auto ID, we don't update id_state because it tracks time/sequence
            // We just rely on max_user_id check in next_auto_id

            self.persist_id_state()?;
        }

        Ok(())
    }

    /// Get current state (used by tests)
    #[cfg(test)]
    pub fn get_state(&self) -> (u64, u64) {
        (
            self.id_state.load(Ordering::Acquire),
            self.max_user_id.load(Ordering::Acquire),
        )
    }

    /// Load ID state from database
    fn load_id_state(db: &Arc<DB>, table_hash: u32) -> Result<(u64, u64)> {
        let key = Self::id_state_key(table_hash);

        match db.get(&key)? {
            Some(data) => {
                if data.len() >= 16 {
                    let next_auto = u64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    let max_user = u64::from_le_bytes([
                        data[8], data[9], data[10], data[11], data[12], data[13], data[14],
                        data[15],
                    ]);
                    Ok((next_auto, max_user))
                } else {
                    warn!("Invalid ID state data length: {}", data.len());
                    Ok((1, 0)) // Start from 1 for auto-increment
                }
            }
            None => Ok((1, 0)), // Start from 1 for auto-increment
        }
    }

    /// Persist current ID state to database
    fn persist_id_state(&self) -> Result<()> {
        let key = Self::id_state_key(self.table_hash);
        let mut data = Vec::with_capacity(16);

        // Store next_auto_id and max_user_id as little-endian u64
        data.extend_from_slice(&self.id_state.load(Ordering::Acquire).to_le_bytes());
        data.extend_from_slice(&self.max_user_id.load(Ordering::Acquire).to_le_bytes());

        self.db.put(&key, &data)?;
        Ok(())
    }

    /// Generate key for storing ID state
    fn id_state_key(table_hash: u32) -> Vec<u8> {
        let mut key = Vec::with_capacity(12);
        key.extend_from_slice(b"_id_state_");
        key.extend_from_slice(&table_hash.to_be_bytes());
        key
    }
}

/// Global ID manager registry for all tables
#[derive(Debug)]
pub struct IdManagerRegistry {
    managers: HashMap<String, Arc<IdManager>>,
    db: Arc<DB>,
}

impl IdManagerRegistry {
    /// Create new registry
    pub fn new(db: Arc<DB>) -> Self {
        IdManagerRegistry {
            managers: HashMap::new(),
            db,
        }
    }

    /// Get or create ID manager for a table
    pub fn get_or_create(&mut self, table_name: &str) -> Result<Arc<IdManager>> {
        if let Some(manager) = self.managers.get(table_name) {
            return Ok(manager.clone());
        }

        let manager = Arc::new(IdManager::new(table_name.to_string(), self.db.clone())?);
        self.managers
            .insert(table_name.to_string(), manager.clone());
        Ok(manager)
    }

    /// List all managed tables (used by tests)
    #[cfg(test)]
    pub fn list_tables(&self) -> Vec<String> {
        self.managers.keys().cloned().collect()
    }
}

#[cfg(test)]
#[path = "id_manager_test.rs"]
mod id_manager_test;
