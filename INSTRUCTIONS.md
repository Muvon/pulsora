# PULSORA DEVELOPMENT GUIDE

> **Pulsora** - High-performance time series database optimized for market data. Built on RocksDB with a REST API interface, featuring columnar storage, custom compression, and efficient ID management.

## 🚀 QUICK START

```bash
# Setup
git clone https://github.com/muvon/pulsora.git && cd pulsora

# Development cycle (ALWAYS in this order)
cargo check --message-format=short                              # 1. Fast check
cargo clippy --all-features --all-targets -- -D warnings        # 2. Fix warnings
cargo test                                                      # 3. Test (Unit + Integration)
cargo run                                                       # 4. Run server
```

## 🎯 ARCHITECTURE (5-MINUTE READ)

**This is a database engine** - Performance, consistency, and data integrity are paramount.

### Core Concepts
```
REST API → Server → StorageEngine → SchemaManager / IdManager
                                ↓
                        RocksDB (Columnar Storage)
```

**Key Design:**
- **Columnar Storage**: Data stored by column for compression efficiency (`src/storage/columnar.rs`)
- **ID Management**: Hybrid Auto-Increment (Snowflake-like) + User IDs (`src/storage/id_manager.rs`)
- **Compression**: Type-specific (Delta, XOR, Varint) (`src/storage/compression.rs`)
- **Indexing**: Block Index (`[hash][B][min_ts]`) + ID Index (`[hash][id]`)
- **Consistency**: Latest-write-wins for ID updates

### What Lives Where
```
src/
├── main.rs            → Entry point, CLI args
├── server.rs          → Axum REST API handlers
├── config.rs          → Configuration loading
├── error.rs           → Unified error handling
└── storage/
    ├── mod.rs         → StorageEngine (Main Interface)
    ├── columnar.rs    → ColumnBlock serialization/deserialization
    ├── compression.rs → Bit-packing, Delta, XOR algorithms
    ├── encoding.rs    → Low-level varint/varfloat encoding
    ├── id_manager.rs  → ID generation & persistence
    ├── ingestion.rs   → CSV parsing & batch writing
    ├── query.rs       → Range queries & filtering
    └── schema.rs      → Schema inference & validation

tests/                 → Integration tests (End-to-End)
```

## 🧪 TESTING GUIDELINES (STRICT)

### Integration Tests
- **Location**: `tests/` directory ONLY.
- **Naming**: Must have `_test.rs` suffix (e.g., `consistency_test.rs`).
- **Scope**: Verify public API (`StorageEngine`) and end-to-end behavior.
- **Access**: Black-box testing (public methods only).

### Unit Tests
- **Location**: `src/` directory, alongside source files.
- **Naming**: Separate file with `_test.rs` suffix (e.g., `ingestion_test.rs` for `ingestion.rs`).
- **Inclusion**: Include in source file via:
  ```rust
  #[cfg(test)]
  #[path = "module_name_test.rs"]
  mod module_name_test;
  ```
- **Scope**: Internal logic, private functions, edge cases.
- **Access**: White-box testing (`use super::*;`).

## 📍 TASK-BASED GUIDE

### "Add new compression algorithm"
1. **Implement**: Add logic to `src/storage/compression.rs`
2. **Test**: Add unit tests in `src/storage/compression_test.rs`
3. **Integrate**: Update `compress_column` in `src/storage/columnar.rs`
4. **Verify**: Run `cargo test` to ensure round-trip works

### "Modify storage format"
1. **Schema**: Check `src/storage/schema.rs` if metadata changes
2. **Columnar**: Update `ColumnBlock::serialize/deserialize` in `src/storage/columnar.rs`
3. **Compatibility**: Ensure backward compatibility or migration path
4. **Test**: Verify `columnar_test.rs` passes

### "Add API endpoint"
1. **Handler**: Add function in `src/server.rs`
2. **Route**: Register in `app()` function in `src/server.rs`
3. **Storage**: Add corresponding method to `StorageEngine` in `src/storage/mod.rs`
4. **Test**: Add integration test in `tests/api_test.rs` (create if needed)

### "Fix query performance"
1. **Analyze**: Check `src/storage/query.rs` -> `execute_query`
2. **Index**: Verify Block Index usage
3. **Profile**: Use `benches/query.rs` with `cargo bench`
4. **Optimize**: Reduce I/O, improve filtering, or parallelize

## 🚫 CRITICAL RULES

### Database Code - NEVER DO
```rust
// ❌ Panic in storage engine
panic!("Corrupt data");
.unwrap()  // in runtime paths
.expect()  // in runtime paths

// ✅ Return Result
Err(PulsoraError::InvalidData("Corrupt block".to_string()))

// ❌ Print to console
println!("Writing block");

// ✅ Use tracing
tracing::debug!("Writing block {}", block_id);

// ❌ Blocking I/O in async context
std::fs::read(...)

// ✅ Use tokio::fs or spawn_blocking
tokio::fs::read(...)
```

### Development Workflow
```bash
# ✅ ALWAYS this order
cargo check --message-format=short                       # Fast
cargo clippy --all-features --all-targets -- -D warnings # Fix ALL warnings
cargo test                                               # Verify correctness

# ❌ NEVER during development
cargo build --release  # Too slow, unless benchmarking
```

## 🐛 QUICK DEBUG

**Problem: Tests failing**
→ Check `cargo test` output
→ Unit tests: `src/storage/*_test.rs`
→ Integration tests: `tests/*_test.rs`

**Problem: Performance regression**
→ Run benchmarks: `cargo bench`
→ Check `benches/` folder

**Problem: Data corruption**
→ Check `src/storage/columnar.rs` serialization
→ Verify `src/storage/compression.rs` round-trip

## 📚 EXAMPLES AS TEMPLATES

**Adding tests?** → See `tests/consistency_test.rs` or `src/storage/ingestion_test.rs`
**New storage feature?** → Check `src/storage/columnar.rs` structure
**Query logic?** → See `src/storage/query.rs`

---

**Need more details?** Check `doc/` folder.
**This guide is for**: Getting started fast and maintaining high code quality.
