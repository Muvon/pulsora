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
    /// Next auto-increment ID to assign
    next_auto_id: AtomicU64,
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
        let (next_auto, max_user) = Self::load_id_state(&db, table_hash)?;

        debug!(
            "Initialized ID manager for table '{}': next_auto={}, max_user={}",
            table_name, next_auto, max_user
        );

        Ok(IdManager {
            table_name,
            table_hash,
            next_auto_id: AtomicU64::new(next_auto),
            max_user_id: AtomicU64::new(max_user),
            db,
        })
    }

    /// Get next auto-increment ID with snowflake-like structure for distributed scaling
    /// Format: [timestamp_ms:41 bits][node_id:10 bits][sequence:13 bits]
    /// This gives us:
    /// - ~69 years of unique timestamps from epoch
    /// - 1024 possible nodes
    /// - 8192 IDs per millisecond per node
    pub fn next_auto_id(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Custom epoch (2024-01-01 00:00:00 UTC) to maximize timestamp range
        const CUSTOM_EPOCH: u64 = 1704067200000; // milliseconds since UNIX epoch

        // Get current timestamp in milliseconds
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let timestamp = now_ms - CUSTOM_EPOCH;

        // For now, use table_hash as node_id (10 bits = 0-1023)
        let node_id = (self.table_hash & 0x3FF) as u64; // Take lower 10 bits

        // Get sequence number (13 bits = 0-8191)
        let sequence = self.next_auto_id.fetch_add(1, Ordering::SeqCst) & 0x1FFF;

        // Combine into snowflake ID
        let snowflake_id = (timestamp << 23) | (node_id << 13) | sequence;

        // Ensure we don't conflict with user IDs
        let max_user = self.max_user_id.load(Ordering::Acquire);
        if snowflake_id <= max_user {
            // If conflict, use simple increment from max_user
            let new_auto = max_user + 1;
            self.persist_id_state().unwrap_or_else(|e| {
                warn!("Failed to persist ID state: {}", e);
            });
            return new_auto;
        }

        // Persist state periodically (every 100 IDs to reduce I/O)
        if sequence.is_multiple_of(100) {
            self.persist_id_state().unwrap_or_else(|e| {
                warn!("Failed to persist ID state: {}", e);
            });
        }

        snowflake_id
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

            // If user ID exceeds auto ID, update auto counter
            let current_auto = self.next_auto_id.load(Ordering::Acquire);
            if id >= current_auto {
                self.next_auto_id.store(id + 1, Ordering::Release);
            }

            self.persist_id_state()?;
        }

        Ok(())
    }

    /// Get current state (used by tests)
    #[cfg(test)]
    pub fn get_state(&self) -> (u64, u64) {
        (
            self.next_auto_id.load(Ordering::Acquire),
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
        data.extend_from_slice(&self.next_auto_id.load(Ordering::Acquire).to_le_bytes());
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
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (Arc<DB>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(DB::open_default(temp_dir.path()).unwrap());
        (db, temp_dir)
    }

    #[test]
    fn test_row_id_parsing() {
        assert_eq!(RowId::from_string("").unwrap(), RowId::Auto);
        assert_eq!(RowId::from_string("123").unwrap(), RowId::User(123));
        assert!(RowId::from_string("0").is_err());
        assert!(RowId::from_string("abc").is_err());
        assert!(RowId::from_string("-1").is_err());
    }

    #[test]
    fn test_auto_increment() {
        let (db, _temp) = create_test_db();
        let manager = IdManager::new("test_table".to_string(), db).unwrap();

        // Snowflake IDs are not sequential, but should be unique
        let id1 = manager.next_auto_id();
        let id2 = manager.next_auto_id();
        let id3 = manager.next_auto_id();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        // IDs should be increasing (due to timestamp component)
        assert!(id2 >= id1);
        assert!(id3 >= id2);
    }

    #[test]
    fn test_user_id_registration() {
        let (db, _temp) = create_test_db();
        let manager = IdManager::new("test_table".to_string(), db).unwrap();

        // Register user ID
        manager.register_user_id(100).unwrap();

        // Auto increment should not conflict with user ID
        let auto_id = manager.next_auto_id();
        assert_ne!(auto_id, 100);

        // If we register a very large user ID, auto IDs should still work
        let large_id = 1_000_000_000_000u64;
        manager.register_user_id(large_id).unwrap();

        let next_auto = manager.next_auto_id();
        // Should either be a snowflake ID or large_id + 1
        assert!(next_auto > 0);

        // Register higher user ID
        manager.register_user_id(200).unwrap();
        let next_id = manager.next_auto_id();
        assert!(next_id != 200); // Should not conflict
    }

    #[test]
    fn test_id_persistence() {
        let (db, _temp) = create_test_db();

        // Create manager and generate some IDs
        {
            let manager = IdManager::new("test_table".to_string(), db.clone()).unwrap();
            manager.next_auto_id(); // 1
            manager.next_auto_id(); // 2
            manager.register_user_id(50).unwrap();
            manager.persist_id_state().unwrap();
        }

        // Create new manager - should load previous state
        {
            let manager = IdManager::new("test_table".to_string(), db).unwrap();
            let (next_auto, max_user) = manager.get_state();
            assert_eq!(next_auto, 51); // Should be max_user + 1
            assert_eq!(max_user, 50);
        }
    }

    #[test]
    fn test_registry() {
        let (db, _temp) = create_test_db();
        let mut registry = IdManagerRegistry::new(db);

        let manager1 = registry.get_or_create("table1").unwrap();
        let manager2 = registry.get_or_create("table2").unwrap();
        let manager1_again = registry.get_or_create("table1").unwrap();

        // Should reuse existing manager
        assert!(Arc::ptr_eq(&manager1, &manager1_again));

        // Different tables should have different managers
        assert!(!Arc::ptr_eq(&manager1, &manager2));

        let tables = registry.list_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
    }

    #[test]
    fn test_concurrent_id_generation() {
        let (db, _temp) = create_test_db();
        let manager = Arc::new(IdManager::new("test_table".to_string(), db).unwrap());

        // Simulate concurrent access
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let manager = manager.clone();
                std::thread::spawn(move || {
                    let mut ids = Vec::new();
                    for _ in 0..100 {
                        ids.push(manager.next_auto_id());
                    }
                    ids
                })
            })
            .collect();

        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // All IDs should be unique
        let unique_count = all_ids.len();
        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), unique_count, "Found duplicate IDs");
    }

    #[test]
    fn test_large_ids() {
        let (db, _temp) = create_test_db();
        let manager = IdManager::new("test_table".to_string(), db).unwrap();

        let large_id = u64::MAX - 1000;
        manager.register_user_id(large_id).unwrap();

        // Next auto ID should be after the large ID
        assert_eq!(manager.next_auto_id(), large_id + 1);
    }

    #[test]
    fn test_invalid_user_ids() {
        let (db, _temp) = create_test_db();
        let manager = IdManager::new("test_table".to_string(), db).unwrap();

        // ID 0 should be rejected
        assert!(manager.register_user_id(0).is_err());
    }

    #[test]
    fn test_id_conflict_resolution() {
        let (db, _temp) = create_test_db();
        let manager = IdManager::new("test_table".to_string(), db).unwrap();

        // Generate some auto IDs
        let id1 = manager.next_auto_id();
        let id2 = manager.next_auto_id();

        assert_ne!(id1, id2);

        // Register a user ID that might conflict
        manager.register_user_id(5).unwrap();

        // Auto increment should not produce ID 5
        let id3 = manager.next_auto_id();
        let id4 = manager.next_auto_id();

        assert_ne!(id3, 5);
        assert_ne!(id4, 5);
    }
}
