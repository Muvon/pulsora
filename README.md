# Pulsora

**The fastest and easiest time-series database optimized for speed**

Pulsora is a high-performance time-series database built with Rust, featuring dynamic schema inference, streaming CSV ingestion, and sub-millisecond query performance.

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

- **🚀 High Performance**: 350K+ rows/s ingestion, sub-ms queries
- **📊 Dynamic Schema**: Automatic type inference from CSV data
- **⚡ Streaming Ingestion**: Memory-bounded processing with backpressure
- **🔍 Time-Series Optimized**: Efficient range queries and pagination
- **🛠️ Simple API**: RESTful HTTP interface
- **📈 Benchmarking Suite**: Comprehensive performance testing

## Architecture

- **Storage**: RocksDB with time-ordered keys (`table:timestamp:row_id`)
- **Server**: Axum/Tokio async HTTP server
- **Configuration**: TOML-based with sensible defaults
- **Schema**: Dynamic inference with timestamp detection

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
