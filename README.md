# Pulsora

**High-performance time-series database with columnar storage and advanced compression**

Pulsora is a Rust-based time-series database optimized for market data and time-ordered datasets. It features columnar storage, type-specific compression algorithms, dynamic schema inference, and sub-millisecond query performance.

## Quick Start

```bash
# Start the server
cargo run

# Or with custom configuration
cargo run -- -c pulsora.toml

# Build and run release version
cargo build --release
# Ingest CSV data
curl -X POST http://localhost:8080/tables/stocks/ingest \
  -H "Content-Type: text/csv" \
  --data-binary @data.csv

# Ingest Arrow IPC Stream
curl -X POST http://localhost:8080/tables/stocks/ingest \
  -H "Content-Type: application/vnd.apache.arrow.stream" \
  --data-binary @data.arrow

# Query data (JSON default)
curl "http://localhost:8080/tables/stocks/query?start=2024-01-01T10:00:00&limit=100"

# Query data as Arrow Stream
curl "http://localhost:8080/tables/stocks/query?limit=100" \
  -H "Accept: application/vnd.apache.arrow.stream" > output.arrow

# Query data as CSV
curl "http://localhost:8080/tables/stocks/query?limit=100" \
  -H "Accept: text/csv" > output.csv

# Get table schema
curl "http://localhost:8080/tables/stocks/schema"

# Health check
curl "http://localhost:8080/health"
```

## Key Features
- **🚀 High Performance**: 350K+ rows/s ingestion, sub-ms queries with intelligent caching
- **🔄 Multi-Format Support**: CSV, Apache Arrow (IPC), and Protocol Buffers support
- **📊 Columnar Storage**: Block-based architecture with 2-10x compression ratios
- **⚡ Advanced Compression**: Gorilla (XOR), delta-of-delta, varint, dictionary encoding
- **🔍 Time-Series Optimized**: Binary key encoding for efficient range queries
- **📈 Block Caching**: Intelligent decompression caching for query performance
- **🛠️ Simple API**: RESTful HTTP interface with automatic schema inference
- **🔧 Dynamic Schema**: Automatic type detection and validation
- **⚙️ Configurable**: TOML-based configuration with performance tuning options

## Architecture
- **Storage Engine**: RocksDB with columnar blocks and binary keys (`table_hash:timestamp:row_id`)
- **API Interface**: RESTful API supporting JSON, Arrow IPC, and Protobuf formats
- **Compression**: Type-specific algorithms (Gorilla XOR, delta-of-delta, varint, dictionary)
- **HTTP Server**: Axum/Tokio async server with CORS support and structured logging
- **Configuration**: TOML-based with comprehensive performance tuning options
- **Schema Management**: Dynamic inference with automatic timestamp detection and validation
- **ID Management**: Unique row ID generation with collision detection

## Data Types

Pulsora automatically infers and validates these data types:

- **Integer**: Whole numbers with delta + varint compression
- **Float**: Decimal numbers with XOR (Gorilla) + varfloat compression
- **String**: Text data with dictionary encoding for repetitive values
- **Boolean**: True/false values with run-length encoding
- **Timestamp**: Various datetime formats with delta-of-delta compression

## Documentation

- **[API Reference](doc/API.md)** - HTTP endpoints and usage examples
- **[Configuration](doc/CONFIGURATION.md)** - Server and storage settings
- **[Architecture](doc/ARCHITECTURE.md)** - System design and implementation details
- **[Development](doc/DEVELOPMENT.md)** - Setup and contribution guide
- **[Benchmarks](doc/BENCH.md)** - Performance testing and results

## Performance Characteristics

Based on comprehensive benchmarks:

| Operation | Throughput | Compression | Notes |
|-----------|------------|-------------|-------|
| CSV Ingestion | 350K+ rows/s | 2-10x | Type-specific algorithms |
| Arrow/Proto Ingestion | 400K+ rows/s | 2-10x | Zero-copy parsing |
| Range Queries | 7M+ ops/s | - | With block caching |
| Full Table Scan | 12M+ rows/s | - | Columnar decompression |
| Timestamp Compression | 5-10x | Delta-of-delta + varint | Regular intervals |
| Float Compression | 2-5x | XOR (Gorilla) + varfloat | Slowly changing values |
| Integer Compression | 3-8x | Delta + varint | Sequential/counter data |

## Configuration

Pulsora uses TOML configuration with performance-optimized defaults:

```toml
[server]
host = "0.0.0.0"
port = 8080

[storage]
data_dir = "./data"
write_buffer_size_mb = 64
max_open_files = 1000

[ingestion]
max_csv_size_mb = 512
batch_size = 10000

[performance]
compression = "lz4"
cache_size_mb = 256

[logging]
level = "info"
format = "pretty"
enable_access_logs = true
enable_performance_logs = true
```

## Development

```bash
# Development setup
cargo build
cargo test
cargo bench

# Code quality
cargo fmt
cargo clippy

# Run with debug logging
RUST_LOG=debug cargo run
```

## License

Apache 2.0 License - see [LICENSE](LICENSE) for details.
