# Pulsora Architecture

## Overview

Pulsora is a high-performance time series database built with Rust, designed for market data and similar time-ordered datasets. It uses RocksDB as the storage backend and provides a REST API for data ingestion and querying.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Pulsora Server                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌──────────────────┐                   │
│  │   Axum REST     │    │   Configuration  │                   │
│  │   API Layer     │    │   Management     │                   │
│  │                 │    │   (TOML)         │                   │
│  └─────────────────┘    └──────────────────┘                   │
│           │                       │                            │
│           ▼                       ▼                            │
│  ┌─────────────────────────────────────────────────────────────┤
│  │                Storage Engine                               │
│  │  ┌─────────────────┐    ┌──────────────────┐               │
│  │  │   Schema        │    │   CSV Ingestion  │               │
│  │  │   Management    │    │   Engine         │               │
│  │  └─────────────────┘    └──────────────────┘               │
│  │           │                       │                        │
│  │           ▼                       ▼                        │
│  │  ┌─────────────────┐    ┌──────────────────┐               │
│  │  │   Query Engine  │    │   Columnar       │               │
│  │  │   (Block Cache) │    │   Storage        │               │
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
- **Features:** CORS support, JSON responses, error handling
- **Endpoints:** Health check, CSV ingestion, data querying, schema introspection

**Key Design Decisions:**
- Chose Axum over Actix-Web for better Tokio ecosystem integration
- Consistent JSON response format across all endpoints
- Streaming-ready architecture for future enhancements

### 2. Storage Engine

**Location:** `src/storage/mod.rs`

Central component that orchestrates all storage operations:
- Schema management
- Data ingestion coordination
- Query execution
- Transaction management

### 3. Schema Management

**Location:** `src/storage/schema.rs`

**Features:**
- Dynamic schema inference from CSV headers and data
- Type detection (Integer, Float, String, Boolean, Timestamp)
- Automatic timestamp column detection
- Schema validation for incoming data

**Data Types:**
```rust
pub enum DataType {
    Integer,    // i64
    Float,      // f64
    String,     // String
    Boolean,    // bool
    Timestamp,  // Various datetime formats
}
```

### 4. Columnar Storage Engine

**Location:** `src/storage/columnar.rs`

**Features:**
- Column-oriented storage for better compression and cache locality
- Block-based storage with lightweight row pointers
- Custom compression pipeline combining varint/varfloat with XOR-delta (Gorilla)
- Null bitmap support for sparse data
- Type-specific compression strategies

**Block Structure:**
```rust
pub struct ColumnBlock {
    pub row_count: usize,
    pub columns: HashMap<String, Vec<u8>>,      // Compressed column data
    pub null_bitmaps: HashMap<String, Vec<u8>>, // Null value tracking
}
```

**Compression Strategies by Type:**
- **Timestamps:** Delta-of-delta + varint encoding
- **Integers:** Delta + varint encoding
- **Floats:** XOR compression (Gorilla algorithm) + varfloat encoding
- **Booleans:** Run-length encoding for sparse/repetitive data
- **Strings:** Dictionary encoding for repeated values, direct encoding otherwise

### 5. CSV Ingestion Engine

**Location:** `src/storage/ingestion.rs`

**Features:**
- Chunk-based processing for columnar storage
- Block-based ingestion with row pointer generation
- Data validation against schema
- Efficient binary key encoding

**New Storage Strategy:**
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
- Table isolation via hash prefix
- Fixed-size keys for better RocksDB performance
- Unique row identification

### 6. Query Engine

**Location:** `src/storage/query.rs`

**Features:**
- Time-range queries using RocksDB iterators
- Block-level caching for performance optimization
- Batch block fetching to minimize I/O
- Pagination support (limit/offset)
- Flexible timestamp parsing

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

### 7. Compression Engine

**Location:** `src/storage/compression.rs`

**Features:**
- Type-specific compression algorithms optimized for time-series data
- Delta-of-delta compression for timestamps (Facebook Gorilla paper)
- XOR compression for floating-point values
- Varint encoding for integers with small deltas
- Bit-level operations for maximum efficiency

**Compression Algorithms:**
- **Timestamps:** Delta-of-delta + varint → excellent for regular intervals
- **Floats:** XOR with previous value + bit packing → great for slowly changing values
- **Integers:** Delta encoding + signed varint → efficient for counters/IDs
- **Booleans:** Run-length encoding → optimal for sparse data
- **Strings:** Dictionary encoding when repetitive, direct encoding otherwise

### 8. Encoding Engine

**Location:** `src/storage/encoding.rs`

**Features:**
- Variable-length integer encoding (varint)
- Variable-length float encoding (varfloat)
- Signed integer encoding with zigzag encoding
- Efficient string encoding with length prefixes
- Type-safe value encoding/decoding

**Encoding Formats:**
- **Varint:** 1-10 bytes for u64, optimized for small values
- **Varfloat:** 1-9 bytes for f64, optimized for common ranges
- **Strings:** Length prefix + UTF-8 bytes
- **Signed integers:** Zigzag encoding to handle negative values efficiently

### 9. Configuration System

**Location:** `src/config.rs`

**TOML-based configuration with categories:**

```toml
[server]        # HTTP server settings
[storage]       # RocksDB configuration
[ingestion]     # CSV processing settings
[performance]   # Optimization parameters
[logging]       # Log levels and format
```

## Data Flow

### Ingestion Flow

```
CSV Data → Parse Headers → Infer/Validate Schema →
Parse Rows → Group into Chunks → Create ColumnBlocks →
Compress Columns → Generate BlockID → Store Block →
Generate RowPointers → Batch Write to RocksDB → Return Statistics
```

### Query Flow

```
HTTP Request → Parse Parameters → Build Binary Key Range →
RocksDB Iterator → Collect RowPointers → Group by BlockID →
Batch Fetch Blocks → Decompress & Cache → Extract Rows →
Convert to JSON → Apply Pagination → HTTP Response
```

## Storage Design

### Block-Based Columnar Storage

**Architecture:**
- **ColumnBlocks:** Compressed columnar data stored once per chunk
- **RowPointers:** Lightweight references to specific rows within blocks
- **BlockID:** Unique identifier for each compressed block

**Storage Pattern:**
```
Row Key → RowPointer: [0xFF][block_id_len][block_id][row_index]
Block Key → ColumnBlock: Compressed columnar data
```

### Key Encoding

**Binary key structure (20 bytes fixed):**
- **Table Hash (4 bytes):** FNV-1a hash of table name for isolation
- **Timestamp (8 bytes):** Milliseconds since epoch (big-endian for ordering)
- **Row ID (8 bytes):** Unique identifier for deduplication

**Benefits:**
- Fixed-size keys for optimal RocksDB performance
- Time-ordered storage for efficient range queries
- Table isolation without string prefixes

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

**Compression Ratios (typical):**
- **Timestamps:** 5-10x (regular intervals)
- **Floats:** 2-5x (slowly changing values)
- **Integers:** 3-8x (small deltas)
- **Strings:** 2-4x (dictionary encoding)
- **Booleans:** 10-50x (sparse data)

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
- **Schema Storage:** In-memory HashMap with RwLock
- **Block Caching:** Query-level caching of decompressed blocks
- **Batch Processing:** Configurable chunk sizes for columnar storage
- **Memory Bounds:** Predictable memory usage with fixed block sizes

### Future Enhancements
- **Persistent Block Cache:** LRU cache with disk persistence
- **Async streaming CSV processing:** Non-blocking ingestion
- **Memory-mapped operations:** Zero-copy block access
- **Adaptive compression:** Dynamic algorithm selection based on data patterns

## Error Handling

**Error Types:** `src/error.rs`
- Configuration errors
- Storage/RocksDB errors
- Ingestion/parsing errors
- Query execution errors
- Schema validation errors

**Error Propagation:**
- Custom error types with `thiserror`
- Automatic conversion from external library errors
- Consistent error responses in API layer

## Performance Characteristics

### Write Performance
- **Columnar Compression:** 2-10x compression ratios depending on data type
- **Block-based Ingestion:** Reduced write amplification
- **Binary Key Encoding:** Fixed 20-byte keys for optimal RocksDB performance
- **Batch Processing:** Configurable chunk sizes (default: batch_size rows per block)

### Read Performance
- **Block Caching:** Avoids repeated decompression of same blocks
- **Batch Block Fetching:** Minimizes RocksDB get() operations
- **Range Queries:** Efficient with binary time-based key encoding
- **Lazy Decompression:** Only decompress blocks that contain requested rows

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

## Concurrency Model

- **Async Runtime:** Tokio for non-blocking I/O
- **Schema Management:** RwLock for concurrent read access
- **RocksDB:** Thread-safe with internal locking
- **Request Handling:** Concurrent request processing

## Future Architecture Enhancements

1. **Advanced Compression:** SIMD-optimized algorithms and adaptive compression selection
2. **Persistent Block Cache:** Disk-backed LRU cache for frequently accessed blocks
3. **Streaming Ingestion:** Async CSV processing with backpressure control
4. **Distributed Storage:** Multi-node clustering with block replication
5. **Query Optimization:** Column pruning and predicate pushdown
6. **Tiered Storage:** Hot/warm/cold data with different compression strategies
7. **Monitoring:** Detailed metrics for compression ratios, cache hit rates, and query performance
