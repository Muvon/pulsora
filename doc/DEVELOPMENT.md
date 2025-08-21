# Pulsora Development Guide

## Development Setup

### Prerequisites

- **Rust:** 1.70+ (latest stable recommended)
- **System Dependencies:**
  - `clang` (for RocksDB compilation)
  - `cmake` (for RocksDB compilation)
  - `pkg-config`
  - `git` (for version control)

### Quick Start

```bash
# Clone repository
git clone <repository-url>
cd pulsora

# Build in development mode
cargo build

# Run with default configuration
cargo run

# Run with custom configuration
cargo run -- -c pulsora.toml

# Run tests
cargo test

# Run benchmarks
cargo bench

# Run with debug logging
RUST_LOG=debug cargo run
```

## Project Structure

```
pulsora/
├── src/
│   ├── main.rs              # CLI entry point and argument parsing
│   ├── lib.rs               # Library crate root (for benchmarks)
│   ├── config.rs            # Configuration management and validation
│   ├── server.rs            # Axum HTTP server and API endpoints
│   ├── error.rs             # Error types and conversions
│   └── storage/
│       ├── mod.rs           # Storage engine coordination
│       ├── schema.rs        # Dynamic schema management
│       ├── ingestion.rs     # CSV parsing and data ingestion
│       ├── query.rs         # Query execution and result formatting
│       ├── columnar.rs      # Column-oriented storage implementation
│       ├── compression.rs   # Type-specific compression algorithms
│       ├── encoding.rs      # Variable-length encoding (varint, varfloat)
│       └── id_manager.rs    # Unique ID generation per table
├── tests/                   # Integration tests
│   ├── comprehensive_regression_tests.rs
│   ├── json_serialization_bug.rs
│   ├── test_schema_inference_all_values.rs
│   ├── test_compression_with_types.rs
│   ├── test_get_by_id_json_fix.rs
│   └── final_verification.rs
├── benches/                 # Performance benchmarks
│   ├── ingestion.rs         # CSV ingestion benchmarks
│   ├── query.rs             # Query performance benchmarks
│   └── end_to_end.rs        # Complete workflow benchmarks
├── doc/                     # Documentation
│   ├── API.md               # REST API reference
│   ├── ARCHITECTURE.md      # System design and implementation
│   ├── CONFIGURATION.md     # Configuration guide
│   ├── DEVELOPMENT.md       # This file
│   ├── BENCHMARKING.md      # Benchmarking guide (detailed)
│   ├── BENCH.md             # Benchmark results and analysis
│   └── README.md            # Documentation index
├── Cargo.toml              # Dependencies and build configuration
├── pulsora.toml            # Sample configuration file
└── README.md               # Project overview and usage
```

## Code Organization

### Module Responsibilities

**`main.rs`**
- CLI argument parsing with `clap`
- Configuration loading with validation
- Server startup and error handling
- Logging initialization with structured output
- Graceful shutdown handling

**`lib.rs`**
- Library crate root for benchmarks and tests
- Re-exports public APIs for external use

**`config.rs`**
- TOML configuration parsing with `serde`
- Default value management with sensible defaults
- Configuration validation with detailed error messages
- Type-safe configuration structs with documentation

**`server.rs`**
- Axum HTTP server setup with middleware
- API endpoint handlers with error handling
- Request/response serialization with JSON
- CORS support and access logging
- Correlation ID generation for request tracing

**`error.rs`**
- Centralized error type definitions with `thiserror`
- Error conversion implementations for external libraries
- Consistent error messaging across components

**`storage/mod.rs`**
- Storage engine orchestration and coordination
- RocksDB initialization and configuration
- High-level storage operations (ingest, query, schema)
- Transaction coordination and consistency

**`storage/schema.rs`**
- Dynamic schema inference from CSV data
- Data type detection and validation (Integer, Float, String, Boolean, Timestamp)
- Schema storage and retrieval with persistence
- Type conversion utilities and error handling

**`storage/ingestion.rs`**
- CSV parsing and validation with streaming support
- Batch processing for performance optimization
- Key generation for time-series storage (binary encoding)
- Data serialization with type-specific handling

**`storage/query.rs`**
- Time-range query execution with binary key ranges
- RocksDB iterator management for efficient scans
- Result pagination and formatting with JSON conversion
- Block caching for performance optimization

**`storage/columnar.rs`**
- Column-oriented storage implementation
- Block-based storage with compression
- Null bitmap support for sparse data
- Type-aware compression strategies

**`storage/compression.rs`**
- Type-specific compression algorithms (Gorilla XOR, delta-of-delta, varint)
- Bit-level operations for maximum efficiency
- Compression/decompression with error handling

**`storage/encoding.rs`**
- Variable-length encoding (varint, varfloat)
- Zigzag encoding for signed integers
- String encoding with length prefixes
- Type-safe encoding/decoding

**`storage/id_manager.rs`**
- Unique row ID generation per table
- Collision detection and handling
- Persistent ID counters in RocksDB

## Dependencies

### Core Dependencies

```toml
# CLI and Configuration
clap = { version = "4.4", features = ["derive"] }    # Command-line argument parsing
config = "0.14"                                      # Configuration management
toml = "0.8"                                         # TOML parsing

# Web Framework (Tokio ecosystem)
axum = "0.7"                                         # HTTP server framework
tokio = { version = "1.0", features = ["full"] }    # Async runtime
tower = "0.4"                                        # Service abstractions
tower-http = { version = "0.5", features = ["fs", "trace", "cors"] }  # HTTP middleware

# Storage
rocksdb = "0.22"                                     # Embedded database

# Streaming & Memory Management
csv = "1.3"                                          # CSV parsing
tokio-stream = "0.1"                                 # Async streams
futures = "0.3"                                      # Future utilities
bytes = "1.5"                                        # Byte buffer management

# Serialization
serde = { version = "1.0", features = ["derive"] }  # Serialization framework
serde_json = "1.0"                                  # JSON serialization

# Time handling
chrono = { version = "0.4", features = ["serde"] }  # Date/time handling

# Error handling
anyhow = "1.0"                                       # Error handling utilities
thiserror = "1.0"                                   # Error derive macros

# Logging
tracing = "0.1"                                      # Structured logging
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }  # Log formatting
uuid = { version = "1.0", features = ["v4"] }       # UUID generation
atty = "0.2"                                         # Terminal detection

# Performance
rayon = "1.8"                                        # Data parallelism
```

### Development Dependencies

```toml
# Benchmarking
criterion = { version = "0.5", features = ["html_reports"] }  # Performance benchmarking
tempfile = "3.8"                                              # Temporary files for tests
```

## Development Workflow

### Building

```bash
# Development build (fast compilation, debug info)
cargo build

# Release build (optimized, no debug info)
cargo build --release

# Check for compilation errors without building
cargo check

# Check with all features and targets
cargo check --all-targets --all-features

# Clean build artifacts
cargo clean
```

### Testing

```bash
# Run all tests
cargo test

# Run tests with output (see println! statements)
cargo test -- --nocapture

# Run specific test
cargo test test_schema_inference_all_values

# Run tests in specific module
cargo test storage::ingestion

# Run tests with debug logging
RUST_LOG=debug cargo test

# Run integration tests only
cargo test --test comprehensive_regression_tests
```

### Benchmarking

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

# Generate HTML reports
cargo bench -- --output-format html
```

### Code Quality

```bash
# Format code according to Rust standards
cargo fmt

# Check formatting without making changes
cargo fmt --check

# Run clippy lints for code quality
cargo clippy

# Run clippy with all targets and features
cargo clippy --all-targets --all-features

# Fix clippy warnings automatically (when possible)
cargo clippy --fix

# Run clippy with strict settings
cargo clippy -- -D warnings
```

### Performance Testing

```bash
# Run benchmarks with profiling
cargo bench --bench ingestion -- --profile-time 10

# Profile with perf (Linux)
cargo build --release
perf record --call-graph=dwarf ./target/release/pulsora
perf report

# Memory profiling with valgrind
cargo build --release
valgrind --tool=massif ./target/release/pulsora

# Generate flame graphs
cargo install flamegraph
cargo flamegraph --bench ingestion
```

## Adding New Features

### Adding a New API Endpoint

1. **Define the handler in `server.rs`:**
```rust
async fn new_endpoint(
    State(state): State<AppState>,
    // Add extractors as needed (Path, Query, Json, etc.)
) -> std::result::Result<Json<ApiResponse<ResponseType>>, StatusCode> {
    // Implementation with error handling
    match state.storage.some_operation().await {
        Ok(result) => Ok(Json(ApiResponse::success(result))),
        Err(e) => {
            error!("Operation failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
```

2. **Add route in `start()` function:**
```rust
let app = Router::new()
    .route("/new-endpoint", get(new_endpoint))
    .route("/new-endpoint/:param", post(new_endpoint_with_param))
    // ... other routes
    .with_state(state);
```

3. **Add tests:**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_endpoint() {
        // Test implementation with proper setup/teardown
    }
}
```

### Adding Configuration Options

1. **Add field to appropriate config struct in `config.rs`:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectionConfig {
    pub new_option: TypeName,
    // ... existing fields
}
```

2. **Update default implementation:**
```rust
impl Default for SectionConfig {
    fn default() -> Self {
        Self {
            new_option: default_value,
            // ... existing defaults
        }
    }
}
```

3. **Add validation in `Config::validate()`:**
```rust
if self.section.new_option < minimum_value {
    return Err(PulsoraError::Config(
        "New option must be at least X".to_string()
    ));
}
```

4. **Update sample configuration file `pulsora.toml`**
5. **Update documentation in `doc/CONFIGURATION.md`**

### Adding New Data Types

1. **Add variant to `DataType` enum in `schema.rs`:**
```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    // ... existing types
    NewType,
}
```

2. **Update type inference in `infer_data_type()`:**
```rust
fn infer_data_type(&self, value: &str) -> DataType {
    // Add detection logic for new type
    if self.is_new_type(value) {
        return DataType::NewType;
    }
    // ... existing logic
}
```

3. **Update validation in `validate_value()`:**
```rust
match expected_type {
    DataType::NewType => {
        // Add validation logic
        if !self.is_valid_new_type(value) {
            return Err(PulsoraError::InvalidData(
                format!("Invalid new type value: {}", value)
            ));
        }
    }
    // ... existing cases
}
```

4. **Update JSON conversion in `query.rs`**
5. **Add compression support in `columnar.rs` and `compression.rs`**

### Adding Compression Algorithms

1. **Implement compression functions in `compression.rs`:**
```rust
pub fn compress_new_type(values: &[NewType]) -> Result<(NewType, Vec<u8>)> {
    // Implementation
}

pub fn decompress_new_type(base: NewType, data: &[u8], count: usize) -> Result<Vec<NewType>> {
    // Implementation
}
```

2. **Update `columnar.rs` to use new compression:**
```rust
match data_type {
    DataType::NewType => {
        let (base, compressed) = compression::compress_new_type(&typed_values)?;
        // Store base and compressed data
    }
    // ... existing cases
}
```

3. **Add tests for compression algorithm**
4. **Add benchmarks for performance validation**

## Debugging

### Logging

Use structured logging with `tracing`:

```rust
use tracing::{info, warn, error, debug, trace, instrument};

// Log levels from most to least verbose
trace!("Very detailed debugging info");
debug!("Debugging information");
info!("General information");
warn!("Warning message");
error!("Error occurred: {}", error);

// Structured logging with fields
info!(
    table = %table_name,
    rows_inserted = count,
    duration_ms = duration.as_millis(),
    "Ingestion completed successfully"
);

// Instrument functions for automatic tracing
#[instrument(skip(self))]
async fn some_function(&self, param: &str) -> Result<()> {
    // Function body
}
```

### Environment Variables

```bash
# Set log level (overrides config)
export RUST_LOG=debug
export RUST_LOG=pulsora=trace,rocksdb=warn

# Enable backtraces on panic
export RUST_BACKTRACE=1
export RUST_BACKTRACE=full

# Disable color output
export NO_COLOR=1
```

### Common Debugging Scenarios

**RocksDB Issues:**
```bash
# Enable RocksDB logging
export RUST_LOG=rocksdb=debug

# Check data directory permissions
ls -la ./data/

# Monitor RocksDB statistics
# (Add to code: db.property_value("rocksdb.stats"))
```

**CSV Parsing Issues:**
```bash
# Enable detailed ingestion logging
export RUST_LOG=pulsora::storage::ingestion=trace

# Test with minimal CSV
echo "timestamp,value
2024-01-01 10:00:00,100" | curl -X POST http://localhost:8080/tables/test/ingest \
  -H "Content-Type: text/csv" -d @-
```

**Query Issues:**
```bash
# Enable query debugging
export RUST_LOG=pulsora::storage::query=debug

# Test simple query
curl "http://localhost:8080/tables/test/query?limit=1"

# Test with correlation ID tracking
curl -H "X-Correlation-ID: test-123" "http://localhost:8080/tables/test/query"
```

**Schema Issues:**
```bash
# Enable schema debugging
export RUST_LOG=pulsora::storage::schema=debug

# Check schema inference
curl "http://localhost:8080/tables/test/schema"
```

## Performance Considerations

### Hot Paths

Critical performance paths to optimize:
1. **CSV parsing** - `ingestion.rs:parse_csv()` with streaming support
2. **Key generation** - `ingestion.rs:generate_key()` with binary encoding
3. **Data serialization** - `columnar.rs:from_rows()` with type-specific compression
4. **Query iteration** - `query.rs:execute_query()` with block caching
5. **JSON conversion** - `query.rs:convert_row_to_json()` with proper type handling
6. **Compression/decompression** - `compression.rs` algorithms for each data type

### Memory Management

- Use `Arc` for shared data structures (StorageEngine, SchemaManager)
- Prefer streaming over loading entire datasets in memory
- Configure appropriate batch sizes based on available memory
- Monitor memory usage during development with tools like `htop` or `ps`
- Use `tokio::task::spawn_blocking` for CPU-intensive operations

### Async Considerations

- Use `tokio::spawn` for independent CPU-intensive tasks
- Avoid blocking operations in async contexts (use `spawn_blocking`)
- Use `tokio::task::spawn_blocking` for RocksDB operations when needed
- Consider backpressure for streaming operations
- Use proper error handling with `Result` types in async functions

### Profiling and Optimization

```bash
# CPU profiling with perf (Linux)
cargo build --release
perf record --call-graph=dwarf ./target/release/pulsora
perf report

# Memory profiling
valgrind --tool=massif ./target/release/pulsora
massif-visualizer massif.out.*

# Flame graphs for visual profiling
cargo install flamegraph
cargo flamegraph --bin pulsora

# Benchmark-driven optimization
cargo bench --bench ingestion
cargo bench --bench query
```

## Contributing Guidelines

### Code Style

- Follow Rust standard formatting (`cargo fmt`)
- Use meaningful variable and function names
- Add documentation comments for public APIs with `///`
- Keep functions focused and small (prefer composition)
- Use `Result` types for error handling, avoid panics
- Prefer explicit error handling over `.unwrap()`

### Documentation

- Document all public APIs with `///` comments
- Include examples in documentation when helpful
- Update relevant documentation files when adding features
- Add inline comments for complex logic
- Keep README and doc files up-to-date

### Testing

- Write unit tests for all new functionality
- Include integration tests for API endpoints
- Test error conditions and edge cases
- Maintain test coverage above 80%
- Use descriptive test names that explain what is being tested

**Test Structure:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_specific_functionality() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        // Act
        let result = function_under_test(&config).await;

        // Assert
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value.expected_field, expected_value);
    }
}
```

### Performance

- Profile performance-critical changes with benchmarks
- Include benchmarks for new algorithms
- Consider memory usage implications of changes
- Test with realistic data sizes and patterns
- Document performance characteristics

### Error Handling

- Use `thiserror` for custom error types
- Provide meaningful error messages with context
- Handle all error cases explicitly
- Use proper error propagation with `?` operator
- Log errors at appropriate levels

**Error Type Example:**
```rust
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Invalid configuration: {message}")]
    Config { message: String },

    #[error("Storage error: {source}")]
    Storage { #[from] source: StorageError },

    #[error("IO error: {source}")]
    Io { #[from] source: std::io::Error },
}
```

## Release Process

### Version Management

- Follow semantic versioning (SemVer)
- Update version in `Cargo.toml`
- Tag releases in git with `v` prefix (e.g., `v0.1.0`)
- Maintain changelog with notable changes

### Build Verification

```bash
# Clean build from scratch
cargo clean
cargo build --release

# Run all tests
cargo test --release

# Run all benchmarks
cargo bench

# Check for warnings
cargo clippy --release -- -D warnings

# Verify formatting
cargo fmt --check

# Check documentation
cargo doc --no-deps
```

### Documentation Updates

- Update README.md with new features
- Update API documentation for endpoint changes
- Update configuration documentation for new options
- Update architecture documentation if design changes
- Verify all links and examples work

### Performance Validation

```bash
# Run comprehensive benchmarks
cargo bench > benchmark_results.txt

# Compare with previous version
# (Store baseline results for comparison)

# Verify no performance regressions
# (Automated in CI/CD pipeline)
```

## Continuous Integration

### GitHub Actions Workflow

```yaml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run tests
        run: cargo test --all-features
      - name: Run clippy
        run: cargo clippy -- -D warnings
      - name: Check formatting
        run: cargo fmt --check

  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run benchmarks
        run: cargo bench
      - name: Store benchmark results
        # Store results for performance regression detection
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/sh
set -e

echo "Running pre-commit checks..."

# Format check
cargo fmt --check

# Clippy check
cargo clippy -- -D warnings

# Test check
cargo test

echo "All checks passed!"
EOF

chmod +x .git/hooks/pre-commit
```

## IDE Setup

### VS Code

Recommended extensions:
- `rust-analyzer` - Rust language server
- `CodeLLDB` - Debugging support
- `Better TOML` - TOML syntax highlighting
- `Error Lens` - Inline error display

**Settings (.vscode/settings.json):**
```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy",
    "rust-analyzer.inlayHints.enable": true,
    "files.watcherExclude": {
        "**/target/**": true
    }
}
```

### IntelliJ IDEA / CLion

- Install Rust plugin
- Configure Rust toolchain
- Enable Clippy integration
- Set up run configurations for different scenarios

## Troubleshooting Development Issues

### Common Build Issues

**RocksDB compilation fails:**
```bash
# Install required dependencies
sudo apt-get install clang cmake pkg-config  # Ubuntu/Debian
brew install cmake pkg-config                # macOS

# Clear cargo cache if needed
cargo clean
rm -rf ~/.cargo/registry/cache
```

**Out of memory during compilation:**
```bash
# Reduce parallel compilation
export CARGO_BUILD_JOBS=1
cargo build --release
```

**Linker errors:**
```bash
# Install additional development tools
sudo apt-get install build-essential  # Ubuntu/Debian
xcode-select --install                # macOS
```

### Runtime Issues

**Database corruption:**
```bash
# Remove corrupted database
rm -rf ./data
# Restart with fresh database
cargo run
```

**Port already in use:**
```bash
# Find process using port
lsof -i :8080
# Kill process or use different port
cargo run -- -c pulsora.toml  # Edit port in config
```

**Permission denied:**
```bash
# Check data directory permissions
ls -la ./data
# Fix permissions if needed
chmod 755 ./data
```

### Testing Issues

**Tests fail intermittently:**
- Use `TempDir` for isolated test environments
- Avoid shared state between tests
- Use proper async test setup with `#[tokio::test]`

**Benchmarks inconsistent:**
- Run on dedicated hardware when possible
- Disable CPU frequency scaling
- Use longer measurement times for stability

## Advanced Development Topics

### Custom Compression Algorithms

To add a new compression algorithm:

1. **Implement in `compression.rs`:**
```rust
pub fn compress_custom(values: &[CustomType]) -> Result<(CustomType, Vec<u8>)> {
    // Implementation
}

pub fn decompress_custom(base: CustomType, data: &[u8], count: usize) -> Result<Vec<CustomType>> {
    // Implementation
}
```

2. **Add tests and benchmarks**
3. **Update columnar storage to use new algorithm**

### Custom Storage Backends

While RocksDB is the primary backend, the storage layer is designed to be pluggable:

```rust
pub trait StorageBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
    // ... other methods
}
```

### Monitoring and Metrics

Future development may include metrics collection:

```rust
// Example metrics integration
use prometheus::{Counter, Histogram, Registry};

pub struct Metrics {
    ingestion_counter: Counter,
    query_duration: Histogram,
}
```

This completes the comprehensive development guide with all current implementation details, dependencies, and development workflows.
