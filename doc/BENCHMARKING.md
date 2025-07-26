# Pulsora Benchmarking Guide

## Overview

Pulsora includes a comprehensive benchmarking suite to measure and optimize performance across different workloads. The benchmarks are built using Criterion.rs and provide detailed performance metrics with statistical analysis.

## Benchmark Categories

### 1. Ingestion Benchmarks (`benches/ingestion.rs`)

Tests CSV data ingestion performance:

- **CSV Parsing**: Raw CSV parsing speed
- **Data Ingestion**: Database insertion throughput  
- **Full Pipeline**: End-to-end ingestion including schema inference
- **Batch Size Optimization**: Performance across different batch sizes

**Key Metrics:**
- Rows processed per second
- Memory usage during ingestion
- Optimal batch sizes for different data volumes

### 2. Query Benchmarks (`benches/query.rs`)

Tests data retrieval performance:

- **Time Range Queries**: Queries with start/end timestamps
- **Pagination**: Performance with different page sizes and offsets
- **Result Set Scaling**: Performance with varying result sizes
- **Concurrent Queries**: Multi-threaded read performance

**Key Metrics:**
- Query response time
- Throughput for different query patterns
- Memory usage during queries
- Concurrent read scalability

### 3. End-to-End Benchmarks (`benches/end_to_end.rs`)

Tests complete system performance:

- **HTTP Ingestion**: REST API ingestion throughput
- **Mixed Workloads**: Combined read/write scenarios
- **Table Scaling**: Performance with multiple tables
- **Data Type Handling**: Performance across different schema types

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

# Run specific benchmark
cargo bench csv_parsing
```

### Using the Benchmark Script

```bash
# Run comprehensive benchmark suite with reporting
./scripts/benchmark.sh
```

This script:
- Builds optimized release version
- Runs all benchmark suites
- Generates HTML reports
- Creates summary documentation
- Saves results with timestamps

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

Based on benchmarks, Pulsora targets:

**Ingestion Performance:**
- CSV Parsing: 1M+ rows/second
- Database Insertion: 100K+ rows/second  
- End-to-end Ingestion: 50K+ rows/second

**Query Performance:**
- Time Range Queries: <1ms for 1K results
- Pagination: <5ms for any page
- Concurrent Reads: Linear scaling to CPU cores

**System Performance:**
- HTTP API: 1K+ requests/second
- Memory Usage: <2GB for 10M rows
- Storage Efficiency: 50%+ compression ratio

## Benchmark Configuration

### Environment Setup

For consistent results:

```bash
# Set CPU governor to performance mode (Linux)
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Disable CPU frequency scaling (macOS)
sudo pmset -a disablesleep 1

# Set high priority
nice -n -20 cargo bench
```

### System Requirements

**Minimum:**
- 4GB RAM
- 2 CPU cores
- 10GB free disk space

**Recommended:**
- 16GB+ RAM
- 8+ CPU cores
- SSD storage
- Dedicated benchmark environment

## Customizing Benchmarks

### Adding New Benchmarks

1. **Create benchmark function:**
```rust
fn bench_new_feature(c: &mut Criterion) {
    let mut group = c.benchmark_group("new_feature");
    
    group.bench_function("test_case", |b| {
        b.iter(|| {
            // Benchmark code here
            black_box(function_to_test())
        })
    });
    
    group.finish();
}
```

2. **Add to criterion group:**
```rust
criterion_group!(benches, bench_new_feature);
```

### Parameterized Benchmarks

```rust
for size in [100, 1000, 10000].iter() {
    group.throughput(Throughput::Elements(*size as u64));
    group.bench_with_input(
        BenchmarkId::new("function", size),
        size,
        |b, size| {
            b.iter(|| function_with_size(*size))
        },
    );
}
```

### Memory Profiling

```rust
use std::alloc::{GlobalAlloc, Layout, System};

// Custom allocator for memory tracking
struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Track allocation
        System.alloc(layout)
    }
    
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Track deallocation
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;
```

## Performance Analysis

### Identifying Bottlenecks

1. **Profile with perf (Linux):**
```bash
cargo build --release
perf record --call-graph=dwarf ./target/release/pulsora
perf report
```

2. **Profile with Instruments (macOS):**
```bash
cargo build --release
xcrun xctrace record --template "Time Profiler" --launch ./target/release/pulsora
```

3. **Memory profiling with valgrind:**
```bash
cargo build --release
valgrind --tool=massif ./target/release/pulsora
massif-visualizer massif.out.*
```

### Flame Graphs

```bash
# Install flamegraph
cargo install flamegraph

# Generate flame graph
cargo flamegraph --bench ingestion
```

## Continuous Benchmarking

### CI Integration

```yaml
# .github/workflows/benchmark.yml
name: Benchmark
on: [push, pull_request]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run benchmarks
        run: cargo bench -- --output-format json > benchmark_results.json
      - name: Store results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: benchmark_results.json
```

### Performance Regression Detection

```bash
# Save baseline
cargo bench -- --save-baseline main

# After changes, compare
cargo bench -- --baseline main

# Fail if performance degrades >10%
cargo bench -- --baseline main --significance-level 0.1
```

## Troubleshooting

### Common Issues

**Inconsistent Results:**
- Run on dedicated hardware
- Disable CPU frequency scaling
- Close other applications
- Use longer measurement times

**Out of Memory:**
- Reduce benchmark data sizes
- Increase system memory
- Use streaming benchmarks
- Monitor memory usage

**Slow Benchmarks:**
- Reduce sample sizes for development
- Use `--quick` flag for fast feedback
- Profile to find bottlenecks
- Optimize hot paths

### Debug Mode

```bash
# Run benchmarks in debug mode for detailed output
RUST_LOG=debug cargo bench

# Enable benchmark debugging
cargo bench -- --debug
```

## Best Practices

1. **Consistent Environment**: Always benchmark on the same hardware
2. **Statistical Significance**: Use adequate sample sizes
3. **Realistic Data**: Use representative datasets
4. **Baseline Comparisons**: Track performance over time
5. **Multiple Metrics**: Measure latency, throughput, and memory
6. **Documentation**: Record benchmark conditions and results
7. **Automation**: Integrate into CI/CD pipeline
8. **Regular Updates**: Keep benchmarks current with code changes