# Pulsora

**The fastest and easiest time-series database with columnar storage**

Pulsora is a high-performance time-series database built with Rust, featuring columnar storage, advanced compression, dynamic schema inference, and sub-millisecond query performance.

## Quick Start

```bash
# Start the server in background
cargo run &

# Or build and run the binary
cargo build --release
./target/release/pulsora &

# Ingest CSV data
curl -X POST http://localhost:8080/ingest/stocks \
  -H "Content-Type: text/csv" \
  --data-binary @data.csv

# Query data
curl "http://localhost:8080/query/stocks?start=2024-01-01T10:00:00&limit=100"

# Stop the server
pkill pulsora
```

## Key Features

- **🚀 High Performance**: 100K+ rows/s ingestion, sub-ms queries with block caching
- **📊 Columnar Storage**: 2-10x compression with type-specific algorithms
- **⚡ Advanced Compression**: Gorilla (XOR), delta-of-delta, varint encoding
- **🔍 Time-Series Optimized**: Efficient range queries with binary key encoding
- **📈 Block-based Architecture**: Reduced I/O with intelligent caching
- **🛠️ Simple API**: RESTful HTTP interface with automatic schema inference

## Architecture

- **Storage**: RocksDB with columnar blocks and binary keys (`table_hash:timestamp:row_id`)
- **Compression**: Type-specific algorithms (Gorilla, delta-of-delta, varint, dictionary)
- **Server**: Axum/Tokio async HTTP server with block caching
- **Configuration**: TOML-based with performance-optimized defaults
- **Schema**: Dynamic inference with automatic timestamp detection

## Documentation

- **[API Reference](docs/API.md)** - HTTP endpoints and usage
- **[Configuration](docs/CONFIGURATION.md)** - Server and storage settings
- **[Architecture](docs/ARCHITECTURE.md)** - System design and components
- **[Benchmarks](docs/BENCH.md)** - Performance testing and results

## Performance

| Operation | Throughput | Latency |
|-----------|------------|---------|
| CSV Ingestion | 350K+ rows/s | ~3ms/1K rows |
| Range Queries | 7M+ ops/s | <1ms |
| Full Table Scan | 12M+ rows/s | ~800µs |

## License

MIT License - see [LICENSE](LICENSE) for details.
