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
        manager.next_auto_id(); // Generates snowflake ID
        manager.next_auto_id(); // Generates snowflake ID
        manager.register_user_id(50).unwrap();
        manager.persist_id_state().unwrap();
    }

    // Create new manager - should load previous state
    {
        let manager = IdManager::new("test_table".to_string(), db).unwrap();
        let (next_auto_state, max_user) = manager.get_state();

        // max_user should be persisted correctly
        assert_eq!(max_user, 50);

        // next_auto_state is [timestamp][sequence], so it should be > 0
        assert!(next_auto_state > 0);
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
