# Pulsora Architecture

## Overview

Pulsora is a high-performance time series database built with Rust, designed for market data and similar time-ordered datasets. It uses RocksDB as the storage backend with a custom columnar storage layer and provides a REST API for data ingestion and querying.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Pulsora Server                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌──────────────────┐                   │
│  │   Axum REST     │    │   Configuration  │                   │
│  │   API Layer     │    │   Management     │                   │
│  │   (CORS + Logs) │    │   (TOML)         │                   │
│  └─────────────────┘    └──────────────────┘                   │
│           │                       │                            │
│           ▼                       ▼                            │
│  ┌─────────────────────────────────────────────────────────────┤
│  │                Storage Engine                               │
│  │  ┌─────────────────┐    ┌──────────────────┐               │
│  │  │   Schema        │    │   CSV Ingestion  │               │
│  │  │   Management    │    │   Engine         │               │
│  │  │   (Dynamic)     │    │   (Streaming)    │               │
│  │  └─────────────────┘    └──────────────────┘               │
│  │           │                       │                        │
│  │           ▼                       ▼                        │
│  │  ┌─────────────────┐    ┌──────────────────┐               │
│  │  │   Query Engine  │    │   Columnar       │               │
│  │  │   (Block Cache) │    │   Storage        │               │
│  │  │   (ID Manager)  │    │   (Type-aware)   │               │
│  │  └─────────────────┘    └──────────────────┘               │
│  │           │                       │                        │
│  │           ▼                       ▼                        │
│  │  ┌─────────────────┐    ┌──────────────────┐               │
│  │  │   Compression   │    │   Encoding       │               │
│  │  │   Engine        │    │   Engine         │               │
│  │  │   (Gorilla/XOR) │    │   (Varint/Float) │               │
│  │  └─────────────────┘    └──────────────────┘               │
│  └─────────────────────────────────────────────────────────────┤
│                           │                                    │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┤
│  │                    RocksDB Backend                         │
│  │  • Binary key encoding (20 bytes fixed)                   │
│  │  • Block-based columnar storage                           │
│  │  • Multi-level compression (LZ4/ZSTD)                     │
│  │  • Row pointers + compressed blocks                       │
│  │  • Optimized for time-series workloads                    │
│  └─────────────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. REST API Layer (Axum)

**Location:** `src/server.rs`

- **Framework:** Axum with Tokio async runtime
- **Features:** CORS support, JSON responses, structured error handling
- **Endpoints:** Health check, CSV ingestion, data querying, schema introspection, table management
- **Middleware:** Access logging with correlation IDs, performance logging
- **Request Tracing:** UUID-based correlation IDs for request tracking

**Key Design Decisions:**
- Chose Axum over Actix-Web for better Tokio ecosystem integration
- Consistent JSON response format across all endpoints
- Streaming request body processing for large CSV uploads
- Configurable body size limits with graceful error handling

**Endpoints:**
- `GET /health` - Server health and version
- `GET /tables` - List all tables
- `POST /tables/{name}/ingest` - CSV data ingestion
- `GET /tables/{name}/query` - Time-range queries with pagination
- `GET /tables/{name}/schema` - Schema information
- `GET /tables/{name}/count` - Row count
- `GET /tables/{name}/rows/{id}` - Get row by ID

### 2. Storage Engine

**Location:** `src/storage/mod.rs`

Central component that orchestrates all storage operations:
- Schema management with dynamic inference
- Data ingestion coordination with batch processing
- Query execution with block caching
- Transaction management and consistency
- ID management with collision detection

**Core Structure:**
```rust
pub struct StorageEngine {
    pub db: Arc<DB>,                                    // RocksDB instance
    pub schemas: Arc<RwLock<SchemaManager>>,           // Schema registry
    pub id_managers: Arc<RwLock<IdManagerRegistry>>,   // ID generators
    config: Config,                                     // Configuration
}
```

### 3. Schema Management

**Location:** `src/storage/schema.rs`

**Features:**
- Dynamic schema inference from CSV headers and data
- Type detection (Integer, Float, String, Boolean, Timestamp)
- Automatic timestamp column detection with multiple format support
- Schema validation for incoming data with type coercion
- Schema persistence in RocksDB with versioning

**Data Types:**
```rust
pub enum DataType {
    Integer,    // i64 with delta + varint compression
    Float,      // f64 with XOR (Gorilla) + varfloat compression
    String,     // String with dictionary encoding
    Boolean,    // bool with run-length encoding
    Timestamp,  // Various datetime formats with delta-of-delta compression
}
```

**Timestamp Detection:**
- RFC3339 formats with timezone support
- Common formats like "YYYY-MM-DD HH:MM:SS"
- Unix timestamps (seconds and milliseconds)
- Date-only formats
- Automatic timezone handling

### 4. Columnar Storage Engine

**Location:** `src/storage/columnar.rs`

**Features:**
- Column-oriented storage for better compression and cache locality
- Block-based storage with lightweight row pointers
- Custom compression pipeline combining type-specific algorithms
- Null bitmap support for sparse data
- Type-aware compression strategies

**Block Structure:**
```rust
pub struct ColumnBlock {
    pub row_count: usize,                           // Number of rows
    pub columns: HashMap<String, Vec<u8>>,          // Compressed column data
    pub null_bitmaps: HashMap<String, Vec<u8>>,     // Null value tracking
}
```

**Compression Strategies by Type:**
- **Timestamps:** Delta-of-delta + varint encoding (5-10x compression)
- **Integers:** Delta + varint encoding (3-8x compression)
- **Floats:** XOR compression (Gorilla algorithm) + varfloat encoding (2-5x compression)
- **Booleans:** Run-length encoding for sparse/repetitive data (10-50x compression)
- **Strings:** Dictionary encoding for repeated values, direct encoding otherwise (2-4x compression)

### 5. CSV Ingestion Engine

**Location:** `src/storage/ingestion.rs`

**Features:**
- Streaming CSV processing with configurable batch sizes
- Block-based ingestion with row pointer generation
- Data validation against schema with type coercion
- Efficient binary key encoding for time-ordered storage
- Performance logging with throughput metrics

**Storage Strategy:**
```
Storage Pattern:
1. Group rows into chunks (configurable batch size)
2. Create ColumnBlock per chunk with compressed columns
3. Store block once with unique BlockID
4. Store lightweight RowPointer per row: [marker][block_id][row_index]
```

**Key Encoding Strategy:**
```
Binary Key Format: [table_hash:u32][timestamp:i64][row_id:u64]
Total: 20 bytes fixed-size for optimal performance
```

This encoding ensures:
- Time-ordered storage for efficient range queries
- Table isolation via FNV-1a hash prefix
- Fixed-size keys for better RocksDB performance
- Unique row identification with collision detection

### 6. Query Engine

**Location:** `src/storage/query.rs`

**Features:**
- Time-range queries using RocksDB iterators with binary key ranges
- Block-level caching for performance optimization
- Batch block fetching to minimize I/O operations
- Pagination support (limit/offset) with efficient skipping
- Flexible timestamp parsing with multiple format support

**Optimized Query Flow:**
1. Parse timestamp parameters and build binary key range
2. Iterate through RocksDB collecting row pointers
3. Group requests by BlockID for batch processing
4. Fetch unique blocks once and cache decompressed data
5. Extract specific rows from cached blocks
6. Convert to JSON and apply pagination

**Performance Optimizations:**
- **Block Caching:** Avoids repeated decompression of same blocks
- **Batch Fetching:** Groups row requests by block to minimize RocksDB operations
- **Lazy Decompression:** Only decompress blocks that are actually needed
- **Binary Key Ranges:** Efficient time-based iteration

### 7. Compression Engine

**Location:** `src/storage/compression.rs`

**Features:**
- Type-specific compression algorithms optimized for time-series data
- Delta-of-delta compression for timestamps (Facebook Gorilla paper)
- XOR compression for floating-point values with bit-level operations
- Varint encoding for integers with small deltas
- Bit-level operations for maximum efficiency

**Compression Algorithms:**
- **Timestamps:** Delta-of-delta + varint → excellent for regular intervals
- **Floats:** XOR with previous value + bit packing → great for slowly changing values
- **Integers:** Delta encoding + signed varint → efficient for counters/IDs
- **Booleans:** Run-length encoding → optimal for sparse data
- **Strings:** Dictionary encoding when repetitive, direct encoding otherwise

**Implementation Details:**
```rust
// BitWriter for efficient bit-level operations
struct BitWriter {
    data: Vec<u8>,
    current_byte: u8,
    bits_in_current: u8,
}

// BitReader for decompression
struct BitReader<'a> {
    data: &'a [u8],
    current_byte: u8,
    bits_in_current: u8,
    byte_index: usize,
}
```

### 8. Encoding Engine

**Location:** `src/storage/encoding.rs`

**Features:**
- Variable-length integer encoding (varint) with 1-10 byte efficiency
- Variable-length float encoding (varfloat) with 1-9 byte efficiency
- Signed integer encoding with zigzag encoding for negative values
- Efficient string encoding with length prefixes
- Type-safe value encoding/decoding with error handling

**Encoding Formats:**
- **Varint:** 1-10 bytes for u64, optimized for small values
- **Varfloat:** 1-9 bytes for f64, optimized for common ranges
- **Strings:** Length prefix + UTF-8 bytes
- **Signed integers:** Zigzag encoding to handle negative values efficiently

**Zigzag Encoding:**
```rust
// Maps signed integers to unsigned for efficient varint encoding
// Positive: 0, 1, 2, 3, ... → 0, 2, 4, 6, ...
// Negative: -1, -2, -3, ... → 1, 3, 5, ...
let zigzag = ((value << 1) ^ (value >> 63)) as u64;
```

### 9. ID Management

**Location:** `src/storage/id_manager.rs`

**Features:**
- Unique row ID generation per table
- Collision detection and handling
- Persistent ID counters in RocksDB
- Thread-safe ID allocation with atomic operations

### 10. Configuration System

**Location:** `src/config.rs`

**TOML-based configuration with categories:**

```toml
[server]        # HTTP server settings (host, port, body limits)
[storage]       # RocksDB configuration (data dir, buffers, files)
[ingestion]     # CSV processing settings (batch size, limits)
[performance]   # Optimization parameters (compression, cache)
[logging]       # Log levels, format, and output options
```

**Configuration Structure:**
```rust
pub struct Config {
    pub server: ServerConfig,           // HTTP server configuration
    pub storage: StorageConfig,         // RocksDB settings
    pub ingestion: IngestionConfig,     // CSV processing options
    pub performance: PerformanceConfig, // Optimization settings
    pub logging: LoggingConfig,         // Logging configuration
}
```

## Data Flow

### Ingestion Flow

```
CSV Data → Stream Processing → Parse Headers → Infer/Validate Schema →
Parse Rows → Group into Chunks → Create ColumnBlocks →
Compress Columns → Generate BlockID → Store Block →
Generate RowPointers → Batch Write to RocksDB → Return Statistics
```

**Detailed Steps:**
1. **Stream Processing:** Handle large CSV uploads with configurable body size limits
2. **Schema Inference:** Analyze all values (not just first row) for accurate type detection
3. **Batch Processing:** Group rows into configurable chunks for optimal compression
4. **Columnar Compression:** Apply type-specific compression algorithms
5. **Block Storage:** Store compressed blocks with unique identifiers
6. **Row Pointers:** Create lightweight references to rows within blocks
7. **Performance Logging:** Track throughput, processing time, and compression ratios

### Query Flow

```
HTTP Request → Parse Parameters → Build Binary Key Range →
RocksDB Iterator → Collect RowPointers → Group by BlockID →
Batch Fetch Blocks → Decompress & Cache → Extract Rows →
Convert to JSON → Apply Pagination → HTTP Response
```

**Detailed Steps:**
1. **Parameter Parsing:** Handle multiple timestamp formats and validation
2. **Key Range Construction:** Build efficient binary key ranges for time-based queries
3. **Iterator Processing:** Use RocksDB iterators for efficient range scans
4. **Block Grouping:** Minimize I/O by batching requests for same blocks
5. **Caching:** Cache decompressed blocks to avoid repeated decompression
6. **JSON Conversion:** Type-aware conversion with proper numeric handling
7. **Pagination:** Efficient offset-based pagination with limit enforcement

## Storage Design

### Block-Based Columnar Storage

**Architecture:**
- **ColumnBlocks:** Compressed columnar data stored once per chunk
- **RowPointers:** Lightweight references to specific rows within blocks
- **BlockID:** Unique identifier for each compressed block
- **Null Bitmaps:** Efficient sparse data handling

**Storage Pattern:**
```
Row Key → RowPointer: [0xFF][block_id_len][block_id][row_index]
Block Key → ColumnBlock: Compressed columnar data with null bitmaps
Schema Key → Schema: Table schema with column definitions
```

### Key Encoding

**Binary key structure (20 bytes fixed):**
- **Table Hash (4 bytes):** FNV-1a hash of table name for isolation
- **Timestamp (8 bytes):** Milliseconds since epoch (big-endian for ordering)
- **Row ID (8 bytes):** Unique identifier for deduplication

**FNV-1a Hash Implementation:**
```rust
pub fn calculate_table_hash(table: &str) -> u32 {
    let mut hash = 2166136261u32;
    for byte in table.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}
```

**Benefits:**
- Fixed-size keys for optimal RocksDB performance
- Time-ordered storage for efficient range queries
- Table isolation without string prefixes
- Collision-resistant table separation

### Value Encoding

**RowPointer Format:**
- **Marker (1 byte):** 0xFF to identify pointer vs direct data
- **Block ID Length (4 bytes):** Length of block identifier
- **Block ID (variable):** Unique block identifier
- **Row Index (4 bytes):** Index within the block

**ColumnBlock Format:**
- **Row Count (4 bytes):** Number of rows in block
- **Column Count (4 bytes):** Number of columns
- **Per Column:** Name + compressed data + null bitmap

### Compression Strategy

**Multi-level compression:**
1. **Type-specific encoding:** Varint, varfloat, dictionary, RLE
2. **Algorithm-specific compression:** Delta-of-delta, XOR, run-length
3. **Block-level optimization:** Null bitmaps, sparse data handling
4. **RocksDB compression:** LZ4/ZSTD at storage layer

**Compression Ratios (typical):**
- **Timestamps:** 5-10x (regular intervals with delta-of-delta)
- **Floats:** 2-5x (slowly changing values with XOR)
- **Integers:** 3-8x (small deltas with varint)
- **Strings:** 2-4x (dictionary encoding for repetitive data)
- **Booleans:** 10-50x (run-length encoding for sparse data)

### RocksDB Configuration

**Optimizations for time-series workload:**
- **Write buffers:** 6 buffers × 64MB for parallel writes
- **Compression:** Multi-level (LZ4 for upper levels, ZSTD for bottom)
- **Block cache:** Configurable with index/filter caching
- **Compaction:** Level-based with dynamic level bytes, 8 background jobs
- **File sizes:** 64MB target with 2x multiplier for better read performance
- **Bloom filters:** 10 bits per key for faster lookups

**Memory Management:**
- **Block cache:** Configurable size for hot data
- **Write buffers:** Multiple buffers for write parallelism
- **Compaction readahead:** 2MB for sequential access patterns

## Memory Management

### Current Implementation
- **Schema Storage:** In-memory HashMap with RwLock for concurrent access
- **Block Caching:** Query-level caching of decompressed blocks
- **Batch Processing:** Configurable chunk sizes for columnar storage
- **Memory Bounds:** Predictable memory usage with fixed block sizes
- **ID Management:** Per-table ID generators with atomic counters

### Concurrency Model
- **Async Runtime:** Tokio for non-blocking I/O operations
- **Schema Management:** RwLock for concurrent read access with exclusive writes
- **RocksDB:** Thread-safe with internal locking and atomic operations
- **Request Handling:** Concurrent request processing with correlation IDs
- **Block Cache:** Thread-safe caching with atomic reference counting

## Error Handling

**Error Types:** `src/error.rs`
- Configuration errors with validation details
- Storage/RocksDB errors with context
- Ingestion/parsing errors with line numbers
- Query execution errors with parameter details
- Schema validation errors with type mismatches

**Error Propagation:**
- Custom error types with `thiserror` for structured errors
- Automatic conversion from external library errors
- Consistent error responses in API layer with HTTP status codes
- Detailed logging with correlation IDs for debugging

## Performance Characteristics

### Write Performance
- **Columnar Compression:** 2-10x compression ratios depending on data type
- **Block-based Ingestion:** Reduced write amplification with batch processing
- **Binary Key Encoding:** Fixed 20-byte keys for optimal RocksDB performance
- **Streaming Processing:** Handle large CSV uploads without memory exhaustion
- **Batch Processing:** Configurable chunk sizes (default: 10,000 rows per block)

### Read Performance
- **Block Caching:** Avoids repeated decompression of same blocks
- **Batch Block Fetching:** Minimizes RocksDB get() operations
- **Range Queries:** Efficient with binary time-based key encoding
- **Lazy Decompression:** Only decompress blocks that contain requested rows
- **Pagination:** Efficient offset-based pagination with limit enforcement

### Compression Performance
- **Type-specific algorithms:** Optimized for each data type
- **Varint encoding:** 1-10 bytes for integers (vs 8 bytes fixed)
- **XOR compression:** Excellent for slowly changing float values
- **Dictionary encoding:** Automatic for repetitive string data
- **Delta compression:** Highly effective for sequential timestamps

### Memory Usage
- **Predictable:** Block-based storage with configurable chunk sizes
- **Efficient:** Custom compression reduces memory footprint by 2-10x
- **Bounded:** Query-level block caching prevents memory leaks
- **Scalable:** Columnar storage scales with data volume, not row count

## Logging and Monitoring

### Structured Logging
- **Correlation IDs:** UUID-based request tracking across components
- **Performance Metrics:** Throughput, latency, compression ratios
- **Access Logs:** HTTP request/response logging with timing
- **Error Context:** Detailed error information with stack traces

### Configuration Options
```toml
[logging]
level = "info"                    # trace, debug, info, warn, error
format = "pretty"                 # pretty or json
enable_access_logs = true         # HTTP request logging
enable_performance_logs = true    # Detailed performance metrics
file_output = "/var/log/pulsora.log"  # Optional file output
```

### Log Examples
```
INFO http_request{correlation_id=550e8400-e29b-41d4-a716-446655440000}: 📊 Starting CSV ingestion table=stocks size_mb=1.5 body_read_ms=12
INFO http_request{correlation_id=550e8400-e29b-41d4-a716-446655440000}: ✅ CSV ingestion completed successfully rows_inserted=10000 processing_time_ms=234 throughput_rows_per_sec=42735.04
```

## Future Architecture Enhancements

1. **Advanced Compression:** SIMD-optimized algorithms and adaptive compression selection
2. **Persistent Block Cache:** Disk-backed LRU cache for frequently accessed blocks
3. **Streaming Ingestion:** Async CSV processing with backpressure control
4. **Distributed Storage:** Multi-node clustering with block replication
5. **Query Optimization:** Column pruning and predicate pushdown
6. **Tiered Storage:** Hot/warm/cold data with different compression strategies
7. **Monitoring:** Detailed metrics for compression ratios, cache hit rates, and query performance
8. **WebSocket Support:** Real-time data streaming for live dashboards
