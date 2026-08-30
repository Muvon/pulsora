# Pulsora

> High-performance time-series database with columnar storage and type-specific compression

[![CI](https://github.com/Muvon/pulsora/actions/workflows/ci.yml/badge.svg)](https://github.com/Muvon/pulsora/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmuvon%2Fpulsora%2Fbadges%2Fcoverage.json&style=flat-square)](https://github.com/Muvon/pulsora/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Pulsora ingests market data and time-ordered datasets over a plain REST API —
CSV, Arrow IPC, or Protobuf in — then compresses every column with an algorithm
picked for its type: Gorilla XOR for floats, delta-of-delta for timestamps,
dictionary encoding for strings. No query language to learn: HTTP in, JSON,
Arrow, or CSV out. Website: [pulsora.muvon.io](https://pulsora.muvon.io)

## Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Performance](#performance)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Development](#development)

## Quick Start

Prerequisites: a Rust toolchain and a C/C++ compiler (RocksDB builds from source).

```bash
git clone https://github.com/Muvon/pulsora.git && cd pulsora

# Release build — debug builds of RocksDB are painfully slow, skip them
cargo run --release
```

In a second terminal:

```bash
# Ingest CSV — tables and schema are created automatically
curl -X POST http://localhost:8080/tables/stocks/ingest \
  -H "Content-Type: text/csv" \
  --data-binary $'timestamp,symbol,price,volume\n2024-01-01T10:00:00,AAPL,185.2,1000\n2024-01-01T10:00:01,AAPL,185.4,1500'

# Query a time range (JSON)
curl "http://localhost:8080/tables/stocks/query?start=2024-01-01T10:00:00&limit=100"

# Export the same query as CSV or Arrow IPC
curl "http://localhost:8080/tables/stocks/query?limit=100" -H "Accept: text/csv"
curl "http://localhost:8080/tables/stocks/query?limit=100" \
  -H "Accept: application/vnd.apache.arrow.stream" > output.arrow

# Schema, row count, health
curl http://localhost:8080/tables/stocks/schema
curl http://localhost:8080/tables/stocks/count
curl http://localhost:8080/health
```

Arrow IPC and Protobuf ingest use the same endpoint with
`Content-Type: application/vnd.apache.arrow.stream` or `application/x-protobuf`.

## Features

- **Multi-format ingest & export** — CSV, Apache Arrow IPC, Protobuf in; JSON, CSV, Arrow out
- **Columnar block storage** on RocksDB with block-level metadata only — zero per-row index entries
- **Type-specific compression** — Gorilla XOR (floats), delta-of-delta (timestamps), varint (integers), dictionary (strings), RLE (booleans)
- **Dynamic schema inference** — tables auto-create on first ingest with timestamp detection and type validation
- **Write-ahead log** — buffered rows survive crashes (`wal_enabled` on by default)
- **Block cache** — decompressed blocks stay hot for repeat range queries
- **Binary key encoding** — fixed 20-byte keys make time-range scans cheap
- **TOML configuration** — tuning for buffers, cache, compression, and logging

## Performance

From the Criterion benchmark suite ([full results](doc/BENCH.md)):

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| CSV ingestion (end-to-end) | 290K–465K rows/s | includes schema inference |
| CSV parsing | 1.3–1.6M elements/s | raw parse stage |
| Full table scan | up to 117M elements/s | scales linearly with dataset |
| Time-range query | 63+ Gelem/s | binary key seek, cached blocks |

Compression ratios: timestamps 5–10×, integers 3–8×, floats 2–5×, strings 2–4×.
Numbers come from the suite in `benches/` — run `cargo bench` on your own hardware.

## Configuration

Pulsora runs with built-in defaults; pass `-c` to override
(`cargo run --release -- -c pulsora.toml`). The shipped [pulsora.toml](pulsora.toml) covers:

| Section | Key options |
|---------|-------------|
| `[server]` | `host`, `port`, `max_body_size_mb` |
| `[storage]` | `data_dir`, `write_buffer_size_mb`, `buffer_size`, `flush_interval_ms`, `wal_enabled` |
| `[query]` | `query_threads` (0 = auto-detect) |
| `[ingestion]` | `max_csv_size_mb`, `batch_size` |
| `[performance]` | `compression` (`none`/`snappy`/`lz4`/`zstd`), `cache_size_mb` |
| `[logging]` | `level`, `format` (`pretty`/`json`), access & performance logs |

Full reference: [doc/CONFIGURATION.md](doc/CONFIGURATION.md)

## Documentation

- [API Reference](doc/API.md) — endpoints, parameters, response formats
- [Configuration](doc/CONFIGURATION.md) — every option with tuning recipes
- [Architecture](doc/ARCHITECTURE.md) — storage engine design and internals
- [Benchmarks](doc/BENCH.md) · [Benchmarking guide](doc/BENCHMARKING.md)
- [Development](doc/DEVELOPMENT.md) — setup and contribution guide

## Development

```bash
cargo test          # unit + integration tests
cargo bench         # Criterion benchmark suite
cargo fmt && cargo clippy
RUST_LOG=debug cargo run
```

## License

[Apache 2.0](LICENSE) © Muvon Un Limited
