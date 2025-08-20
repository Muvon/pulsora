# Pulsora Benchmarking Guide

This document describes Pulsora's comprehensive benchmarking suite and performance characteristics.

## Benchmark Suite Overview

Pulsora includes four benchmark categories to measure different aspects of performance:

### 1. Ingestion Benchmarks (`benches/ingestion.rs`)

Tests CSV parsing and data ingestion performance:

- **CSV Parsing**: Raw CSV parsing throughput
- **Data Ingestion**: RocksDB insertion with schema validation
- **Full Pipeline**: End-to-end CSV ingestion including schema inference
- **Batch Size Optimization**: Performance across different batch sizes

### 2. Query Benchmarks (`benches/query.rs`)

Tests query performance across different patterns:

- **Time Range Queries**: Efficient range scans using time-ordered keys
- **Pagination**: Offset-based result pagination
- **Result Size Scaling**: Performance with different result set sizes
- **Concurrent Queries**: Multi-threaded query performance

### 3. End-to-End Benchmarks (`benches/end_to_end.rs`)

Tests realistic workload scenarios:

- **HTTP Ingestion**: Complete HTTP POST ingestion pipeline
- **Mixed Workloads**: Read/write ratio performance (90/10, 70/30, 50/50)
- **Table Scaling**: Multi-table operations within single database
- **Schema Inference**: Performance across different data types

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark suite
cargo bench --bench ingestion
cargo bench --bench query
cargo bench --bench end_to_end

# Generate detailed reports
cargo bench -- --output-format html
```

## Performance Baselines

### Ingestion Performance

| Dataset Size | CSV Parsing | Data Ingestion | End-to-End |
|--------------|-------------|----------------|------------|
| 100 rows | 1.4 Melem/s | 240K elem/s | 290K elem/s |
| 1,000 rows | 1.6 Melem/s | 360K elem/s | 435K elem/s |
| 10,000 rows | 1.3 Melem/s | 315K elem/s | 465K elem/s |

### Query Performance

| Query Type | 1K Dataset | 10K Dataset | 100K Dataset |
|------------|------------|-------------|--------------|
| Full Scan | 1.2 Melem/s | 12 Melem/s | 117 Melem/s |
| Range Query (10%) | 716 Melem/s | 7.1 Gelem/s | 63 Gelem/s |
| Recent Data (1%) | 1.2 Melem/s | 12 Melem/s | 120 Melem/s |

### Batch Size Optimization

| Batch Size | Throughput | Notes |
|------------|------------|-------|
| 100 | 300K elem/s | Higher overhead |
| 1,000 | 251K elem/s | Balanced |
| 2,500 | 307K elem/s | Optimal for most workloads |

## Benchmark Architecture

### Key Design Principles

1. **Single Database Reuse**: Benchmarks reuse `StorageEngine` instances to avoid file descriptor exhaustion
2. **Unique Table Names**: Each iteration uses unique table names to prevent data conflicts
3. **Realistic Data Generation**: Timestamps, symbols, and numeric data match real-world patterns
4. **Memory Efficiency**: Controlled sample sizes prevent resource exhaustion

### Data Generation

Benchmarks use realistic market data patterns:

```rust
// Timestamp: 2024-01-01 10:00:00 to 23:59:59 (wraps at 24h)
// Symbol: AAPL, GOOGL, MSFT, TSLA
// Price: Base price + incremental changes
// Volume: 1000 + row_index
```

### Critical Bug Fixes

#### 1. Schema Inference Bug
- **Issue**: Volume field "1000" incorrectly detected as timestamp
- **Fix**: Separated timezone-aware and naive datetime parsing
- **Impact**: Proper schema inference for mixed data types

#### 2. File Descriptor Exhaustion
- **Issue**: Each benchmark iteration created new RocksDB database
- **Fix**: Reuse `StorageEngine` instances across iterations
- **Impact**: Benchmarks can run with standard OS limits

#### 3. Invalid Timestamp Generation
- **Issue**: Hours could exceed 23 (e.g., "24:00:00")
- **Fix**: Added modulo operation for hour calculation
- **Impact**: All generated timestamps are valid

## Performance Tuning

### RocksDB Configuration

Key settings for optimal performance:

```toml
[storage]
write_buffer_size_mb = 64
max_open_files = 1000

[performance]
compression = "lz4"
cache_size_mb = 256
```

### Ingestion Optimization

- **Batch Size**: 1000-2500 rows per batch for optimal throughput
- **Memory Limits**: Configure based on available RAM
- **Compression**: LZ4 provides best balance of speed/space

### Query Optimization

- **Time Range Queries**: Leverage time-ordered keys for efficient scans
- **Pagination**: Use offset-based pagination for large result sets
- **Concurrent Access**: RocksDB handles multiple readers efficiently

## Monitoring and Profiling

### Built-in Metrics

Benchmarks provide detailed metrics:

- **Throughput**: Elements/operations per second
- **Latency**: Time per operation
- **Memory Usage**: Peak memory consumption
- **File Descriptors**: Database connection count

### External Tools

Recommended profiling tools:

```bash
# CPU profiling
cargo bench --bench ingestion -- --profile-time=10

# Memory profiling
valgrind --tool=massif target/release/deps/ingestion-*

# System monitoring
htop, iostat, lsof
```

## Regression Testing

### Baseline Establishment

Current performance baselines serve as regression detection:

```bash
# Save baseline
cargo bench > baseline.txt

# Compare against baseline
cargo bench | diff baseline.txt -
```

### Continuous Integration

Recommended CI benchmark integration:

```yaml
- name: Run Benchmarks
  run: cargo bench --bench ingestion -- --output-format json > bench.json

- name: Check Performance Regression
  run: ./scripts/check_regression.sh bench.json baseline.json
```

## Troubleshooting

### Common Issues

1. **"Too many open files"**
   - Increase OS limits: `ulimit -n 10000`
   - Reduce benchmark sample sizes
   - Check for database connection leaks

2. **Memory exhaustion**
   - Reduce dataset sizes in benchmarks
   - Configure RocksDB memory limits
   - Monitor system memory usage

3. **Inconsistent results**
   - Run benchmarks on dedicated hardware
   - Disable CPU frequency scaling
   - Close unnecessary applications

### Debug Mode

Enable detailed logging:

```bash
RUST_LOG=debug cargo bench --bench ingestion
```

## Contributing

When adding new benchmarks:

1. Follow existing patterns for data generation
2. Reuse `StorageEngine` instances
3. Use unique table names per iteration
4. Include performance regression tests
5. Document expected performance characteristics

## Future Improvements

Planned benchmark enhancements:

- **Network Latency**: HTTP client benchmarks
- **Concurrent Ingestion**: Multi-client ingestion tests
- **Large Dataset**: GB-scale ingestion benchmarks
- **Memory Profiling**: Detailed memory usage analysis
- **Disk I/O**: Storage performance characterization
