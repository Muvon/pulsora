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
│  │  │   Query Engine  │    │   Transaction    │               │
│  │  │   (Time-based)  │    │   Management     │               │
│  │  └─────────────────┘    └──────────────────┘               │
│  └─────────────────────────────────────────────────────────────┤
│                           │                                    │
│                           ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┤
│  │                    RocksDB Backend                         │
│  │  • Time-optimized key encoding                             │
│  │  • LZ4 compression                                         │
│  │  • Write batching for ACID transactions                    │
│  │  • Configurable caching and memory management              │
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

### 4. CSV Ingestion Engine

**Location:** `src/storage/ingestion.rs`

**Features:**
- Synchronous CSV parsing (async streaming planned)
- Batch processing for high throughput
- Data validation against schema
- Efficient serialization with bincode

**Key Encoding Strategy:**
```
Key Format: {table}:{timestamp_ms}:{row_id}
Example: "market_data:1704110400000:0000000000000001"
```

This encoding ensures:
- Time-ordered storage for efficient range queries
- Table isolation
- Unique row identification

### 5. Query Engine

**Location:** `src/storage/query.rs`

**Features:**
- Time-range queries using RocksDB iterators
- Pagination support (limit/offset)
- Flexible timestamp parsing
- JSON serialization of results

**Query Flow:**
1. Parse timestamp parameters
2. Build start/end keys for range scan
3. Iterate through RocksDB with bounds checking
4. Deserialize and convert data to JSON
5. Apply pagination limits

### 6. Configuration System

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
Parse Rows → Validate Data → Generate Keys → 
Batch Write to RocksDB → Return Statistics
```

### Query Flow

```
HTTP Request → Parse Parameters → Build Key Range → 
RocksDB Iterator → Deserialize Data → 
Convert to JSON → Apply Pagination → HTTP Response
```

## Storage Design

### Key Encoding

Time-series optimized key structure:
- **Prefix:** Table name for isolation
- **Timestamp:** Milliseconds since epoch (20-digit zero-padded)
- **Row ID:** Unique identifier for deduplication

### Value Encoding

- **Format:** Bincode serialization of HashMap<String, String>
- **Benefits:** Compact binary format, fast serialization/deserialization
- **Schema:** Stored separately in memory for validation

### RocksDB Configuration

**Optimizations for time-series workload:**
- Write buffer size: Configurable (default 64MB)
- Compression: LZ4 for speed vs size balance
- Block cache: Configurable for read performance
- Compaction: Level-based with dynamic level bytes

## Memory Management

### Current Implementation
- **Schema Storage:** In-memory HashMap with RwLock
- **Data Processing:** Streaming-ready but currently synchronous
- **Batch Processing:** Configurable batch sizes for memory control

### Future Enhancements
- Async streaming CSV processing
- Memory-mapped file operations
- Backpressure handling for large ingestions

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
- **Batch Processing:** 10,000 rows per batch (configurable)
- **Key Encoding:** Optimized for time-ordered writes
- **Compression:** LZ4 for fast compression with good ratios

### Read Performance
- **Range Queries:** Efficient with time-based key encoding
- **Caching:** Configurable block cache for hot data
- **Pagination:** Memory-bounded result sets

### Memory Usage
- **Predictable:** Configurable limits prevent OOM
- **Efficient:** Binary serialization and minimal allocations
- **Scalable:** Streaming architecture for large datasets

## Concurrency Model

- **Async Runtime:** Tokio for non-blocking I/O
- **Schema Management:** RwLock for concurrent read access
- **RocksDB:** Thread-safe with internal locking
- **Request Handling:** Concurrent request processing

## Future Architecture Enhancements

1. **Streaming Ingestion:** Async CSV processing with backpressure
2. **Distributed Storage:** Multi-node clustering support
3. **Query Optimization:** Secondary indices and query planning
4. **Compression Tiers:** Hot/warm/cold data with different compression
5. **Monitoring:** Metrics collection and observability