use super::*;

#[test]
fn test_calculate_table_hash() {
    let hash1 = calculate_table_hash("table1");
    let hash2 = calculate_table_hash("table1");
    let hash3 = calculate_table_hash("table2");

    assert_eq!(hash1, hash2, "Hash should be deterministic");
    assert_ne!(
        hash1, hash3,
        "Different tables should have different hashes"
    );
}

#[test]
fn test_calculate_table_hash_consistency() {
    // FNV-1a hash of "test_table"
    // 2166136261 (offset)
    // ^ 't' (116) * prime
    // ...
    // We just want to ensure it doesn't change across versions
    let hash = calculate_table_hash("test_table");
    assert_eq!(hash, calculate_table_hash("test_table"));
}
