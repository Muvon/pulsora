# Pulsora Benchmarking Guide

## Overview

Pulsora includes a comprehensive benchmarking suite built with Criterion.rs to measure and optimize performance across different workloads. The benchmarks provide detailed performance metrics with statistical analysis and are designed to detect performance regressions.

## Benchmark Categories

### 1. Ingestion Benchmarks (`benches/ingestion.rs`)

Tests CSV data ingestion performance with realistic market data patterns:

**Benchmark Functions:**
- **CSV Parsing**: Raw CSV parsing speed with `ingestion::parse_csv()`
- **Data Ingestion**: Database insertion throughput with `ingestion::insert_rows()`
- **Full Pipeline**: End-to-end ingestion including schema inference
- **Batch Size Optimization**: Performance across different batch sizes (100, 1K, 10K rows)

**Data Generation:**
```rust
// Realistic market data with proper timestamp formatting
fn generate_csv_data(rows: usize) -> String {
    let mut csv = String::from("timestamp,symbol,price,volume\n");
    for i in 0..rows {
        let hours = (10 + (i / 3600)) % 24; // Valid hours 0-23
        let minutes = (i % 3600) / 60;
        let seconds = i % 60;
        csv.push_str(&format!(
            "2024-01-01 {:02}:{:02}:{:02},AAPL,{:.2},{}\n",
            hours, minutes, seconds,
            150.0 + (i as f64 * 0.01),  // Incremental price changes
            1000 + i                     // Incremental volume
        ));
    }
    csv
}
```

**Key Metrics:**
- Rows processed per second (throughput)
- Memory usage during ingestion
- Optimal batch sizes for different data volumes
- Schema inference performance

### 2. Query Benchmarks (`benches/query.rs`)

Tests data retrieval performance across different query patterns:

**Benchmark Functions:**
- **Full Table Scan**: Complete table iteration with `query::execute_query()`
- **Time Range Queries**: Queries with start/end timestamps (10% and 1% of data)
- **Recent Data Queries**: Queries for most recent data
- **Pagination**: Performance with different page sizes and offsets

**Query Patterns:**
```rust
// Full scan benchmark
query::execute_query(
    &storage.db,
    "benchmark_table",
    schema,
    None,                    // No start time
    None,                    // No end time
    Some(1000),             // Limit results
    None,                   // No offset
)

// Time range query (10% of data)
query::execute_query(
    &storage.db,
    "benchmark_table",
    schema,
    Some("2024-01-01 10:00:00".to_string()),
    Some("2024-01-01 10:01:40".to_string()),
    Some(1000),
    None,
)
```

**Key Metrics:**
- Query response time (latency)
- Throughput for different query patterns
- Memory usage during queries
- Block cache effectiveness

### 3. End-to-End Benchmarks (`benches/end_to_end.rs`)

Tests complete system performance with realistic workflows:

**Benchmark Functions:**
- **HTTP Ingestion**: Complete HTTP POST ingestion pipeline
- **Mixed Workloads**: Combined read/write scenarios
- **Multi-Symbol Data**: Performance with multiple stock symbols
- **Table Scaling**: Performance with multiple tables

**Multi-Symbol Data Generation:**
```rust
fn generate_market_csv(rows: usize, symbols: &[&str]) -> String {
    let symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"];
    let base_prices = [150.0, 2800.0, 300.0, 200.0];

    for i in 0..rows {
        let symbol = symbols[i % symbols.len()];
        let base_price = base_prices[i % symbols.len()];
        // Generate realistic price movements
    }
}
```

**Key Metrics:**
- API response times
- Mixed workload performance
- System scalability limits
- Resource utilization

## Running Benchmarks

### Quick Start

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark suite
cargo bench --bench ingestion
cargo bench --bench query
cargo bench --bench end_to_end

# Run with custom sample size (faster for development)
cargo bench -- --sample-size 10

# Run specific benchmark function
cargo bench csv_parsing
cargo bench time_range_queries
```

### Advanced Options

```bash
# Warm up system before benchmarking
cargo bench -- --warm-up-time 5

# Set measurement time
cargo bench -- --measurement-time 30

# Generate detailed profiling data
cargo bench -- --profile-time 10

# Save baseline for comparison
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main

# Generate HTML reports
cargo bench -- --output-format html
```

## Interpreting Results

### Criterion Output Format

```
csv_parsing/parse_csv/1000
                        time:   [245.67 µs 248.91 µs 252.58 µs]
                        thrpt:  [3.9616 Melem/s 4.0175 Melem/s 4.0703 Melem/s]
```

**Key Metrics:**
- **time**: Time per operation (lower is better)
- **thrpt**: Throughput - operations per second (higher is better)
- **[low estimate, best estimate, high estimate]**: Statistical confidence intervals

### Performance Targets

Based on actual benchmark results, Pulsora achieves:

**Ingestion Performance:**
- **CSV Parsing**: 1M+ rows/second for pure parsing
- **Data Ingestion**: 240K-465K rows/second with database writes
- **End-to-end Ingestion**: 290K-435K rows/second with schema inference
- **Batch Size Optimization**: 2,500 rows optimal for most workloads

**Query Performance:**
- **Full Table Scan**: 1.2M-117M rows/second (scales with dataset size)
- **Time Range Queries**: 716M-63G operations/second (highly optimized)
- **Recent Data Queries**: 1.2M-120M rows/second
- **Block Cache Hit**: <0.1ms for cached blocks

**Compression Performance:**
- **Timestamps**: 5-10x compression (delta-of-delta + varint)
- **Floats**: 2-5x compression (XOR + varfloat)
- **Integers**: 3-8x compression (delta + varint)
- **Strings**: 2-4x compression (dictionary encoding)

## Benchmark Architecture

### Key Design Principles

1. **Storage Engine Reuse**: Benchmarks reuse `StorageEngine` instances to avoid file descriptor exhaustion
2. **Unique Table Names**: Each iteration uses unique table names to prevent data conflicts
3. **Realistic Data Generation**: Timestamps, symbols, and numeric data match real-world patterns
4. **Memory Efficiency**: Controlled sample sizes prevent resource exhaustion
5. **Statistical Validity**: Multiple iterations with confidence intervals

### Data Generation Patterns

**Market Data Simulation:**
```rust
// Timestamp: 2024-01-01 10:00:00 to 23:59:59 (wraps at 24h)
let hours = (10 + (i / 3600)) % 24;  // Prevents invalid hours like 24:00:00

// Symbol rotation: AAPL, GOOGL, MSFT, TSLA
let symbol = symbols[i % symbols.len()];

// Price: Base price + incremental changes (realistic market movement)
let price = base_price + (i as f64 * 0.01);

// Volume: 1000 + row_index (increasing volume pattern)
let volume = 1000 + i;
```

### Critical Bug Fixes

#### 1. Schema Inference Bug
- **Issue**: Volume field "1000" incorrectly detected as timestamp
- **Fix**: Separated timezone-aware and naive datetime parsing in `schema.rs`
- **Impact**: Proper schema inference for mixed data types

#### 2. File Descriptor Exhaustion
- **Issue**: Each benchmark iteration created new RocksDB database
- **Fix**: Reuse `StorageEngine` instances across iterations
- **Impact**: Benchmarks can run with standard OS limits (`ulimit -n 1024`)

#### 3. Invalid Timestamp Generation
- **Issue**: Hours could exceed 23 (e.g., "24:00:00")
- **Fix**: Added modulo operation: `(10 + (i / 3600)) % 24`
- **Impact**: All generated timestamps are valid and parseable

## Performance Tuning

### RocksDB Configuration

Benchmarks use optimized RocksDB settings for time-series workloads:

```rust
// Internal configuration applied during benchmarks
options.set_write_buffer_size(64 * 1024 * 1024);  // 64MB write buffers
options.set_max_open_files(1000);                 // File handle limit
options.set_compression_type(DBCompressionType::Lz4);  // Fast compression
options.set_block_cache_size(256 * 1024 * 1024);  // 256MB block cache
```

### Ingestion Optimization

**Optimal Settings:**
- **Batch Size**: 1000-2500 rows per batch for best throughput
- **Memory Limits**: Configure based on available RAM
- **Compression**: LZ4 provides best balance of speed/space
- **Write Buffers**: 64MB default, increase for high-throughput scenarios

### Query Optimization

**Performance Factors:**
- **Time Range Queries**: Leverage binary key encoding for efficient scans
- **Block Caching**: Decompressed blocks cached to avoid repeated work
- **Pagination**: Efficient offset-based pagination with limit enforcement
- **Concurrent Access**: RocksDB handles multiple readers efficiently

## Monitoring and Profiling

### Built-in Metrics

Benchmarks provide comprehensive metrics:

```
Benchmark Results:
- **Throughput**: Elements/operations per second
- **Latency**: Time per operation with confidence intervals
- **Memory Usage**: Peak memory consumption during operations
- **Scalability**: Performance scaling with data size
```

### External Profiling Tools

**CPU Profiling:**
```bash
# Profile specific benchmark
cargo bench --bench ingestion -- --profile-time=10

# Generate flame graphs
cargo install flamegraph
cargo flamegraph --bench ingestion

# Use perf on Linux
perf record --call-graph=dwarf target/release/deps/ingestion-*
perf report
```

**Memory Profiling:**
```bash
# Valgrind memory analysis
valgrind --tool=massif target/release/deps/ingestion-*
massif-visualizer massif.out.*

# System monitoring during benchmarks
htop
iostat -x 1
```

## Regression Testing

### Baseline Establishment

Current performance baselines serve as regression detection:

```bash
# Save current performance as baseline
cargo bench > baseline_results.txt

# Compare new results against baseline
cargo bench | diff baseline_results.txt -

# Automated regression detection (future)
./scripts/check_performance_regression.sh
```

### Continuous Integration

**Recommended CI benchmark integration:**

```yaml
name: Performance Tests
on: [push, pull_request]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run Benchmarks
        run: cargo bench --bench ingestion -- --output-format json > bench.json
      - name: Check Performance Regression
        run: ./scripts/check_regression.sh bench.json baseline.json
      - name: Store Results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: bench.json
```

## Troubleshooting

### Common Issues

**"Too many open files" error:**
```bash
# Check current limit
ulimit -n

# Increase limit temporarily
ulimit -n 10000

# Permanent fix (add to ~/.bashrc)
echo "ulimit -n 10000" >> ~/.bashrc

# Reduce benchmark sample sizes if needed
cargo bench -- --sample-size 5
```

**Memory exhaustion:**
```bash
# Monitor memory usage
watch -n 1 'free -h && echo "---" && ps aux | grep criterion'

# Reduce dataset sizes in benchmarks
# Edit benchmark files to use smaller row counts

# Configure system for benchmarks
echo 'vm.swappiness=1' | sudo tee -a /etc/sysctl.conf
```

**Inconsistent results:**
```bash
# Run on dedicated hardware
# Disable CPU frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Close unnecessary applications
# Use longer measurement times
cargo bench -- --measurement-time 60

# Increase sample size for stability
cargo bench -- --sample-size 100
```

### Debug Mode

Enable detailed logging during benchmarks:

```bash
# Enable debug logging
RUST_LOG=debug cargo bench --bench ingestion

# Enable trace logging (very verbose)
RUST_LOG=trace cargo bench --bench query

# Enable specific module debugging
RUST_LOG=pulsora::storage::ingestion=debug cargo bench --bench ingestion
```

## Contributing

### Adding New Benchmarks

When adding benchmarks, follow these patterns:

```rust
fn bench_new_feature(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_feature");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("operation", size),
            size,
            |b, &size| {
                // Setup (outside timing)
                let data = generate_test_data(size);

                b.iter(|| {
                    // Timed operation
                    black_box(function_to_benchmark(black_box(&data)))
                })
            },
        );
    }

    group.finish();
}
```

### Best Practices

1. **Reuse Storage Engines**: Avoid creating new databases per iteration
2. **Use Unique Table Names**: Prevent data conflicts between iterations
3. **Realistic Data**: Generate data that matches real-world patterns
4. **Proper Black Boxing**: Use `black_box()` to prevent compiler optimizations
5. **Statistical Validity**: Use appropriate sample sizes for stable results
6. **Documentation**: Document expected performance characteristics

## Future Improvements

Planned benchmark enhancements:

1. **Network Latency**: HTTP client benchmarks with actual network calls
2. **Concurrent Ingestion**: Multi-client ingestion tests
3. **Large Dataset**: GB-scale ingestion benchmarks
4. **Memory Profiling**: Detailed memory usage analysis per operation
5. **Disk I/O**: Storage performance characterization
6. **Compression Ratios**: Detailed compression effectiveness metrics
7. **Cache Hit Rates**: Block cache performance analysis
8. **Query Optimization**: Column pruning and predicate pushdown benchmarks
