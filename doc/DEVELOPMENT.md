# Pulsora Development Guide

## Development Setup

### Prerequisites

- **Rust:** 1.70+ (latest stable recommended)
- **System Dependencies:** 
  - `clang` (for RocksDB compilation)
  - `cmake` (for RocksDB compilation)
  - `pkg-config`

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

# Run with debug logging
RUST_LOG=debug cargo run
```

## Project Structure

```
pulsora/
├── src/
│   ├── main.rs              # CLI entry point and argument parsing
│   ├── config.rs            # Configuration management and validation
│   ├── server.rs            # Axum HTTP server and API endpoints
│   ├── error.rs             # Error types and conversions
│   └── storage/
│       ├── mod.rs           # Storage engine coordination
│       ├── schema.rs        # Dynamic schema management
│       ├── ingestion.rs     # CSV parsing and data ingestion
│       └── query.rs         # Query execution and result formatting
├── doc/                     # Documentation
├── Cargo.toml              # Dependencies and build configuration
├── pulsora.toml            # Sample configuration file
└── README.md               # Project overview and usage
```

## Code Organization

### Module Responsibilities

**`main.rs`**
- CLI argument parsing with `clap`
- Configuration loading
- Server startup and error handling
- Logging initialization

**`config.rs`**
- TOML configuration parsing
- Default value management
- Configuration validation
- Type-safe configuration structs

**`server.rs`**
- Axum HTTP server setup
- API endpoint handlers
- Request/response serialization
- Error handling and status codes

**`error.rs`**
- Centralized error type definitions
- Error conversion implementations
- Consistent error messaging

**`storage/mod.rs`**
- Storage engine orchestration
- RocksDB initialization and configuration
- High-level storage operations
- Transaction coordination

**`storage/schema.rs`**
- Dynamic schema inference from CSV
- Data type detection and validation
- Schema storage and retrieval
- Type conversion utilities

**`storage/ingestion.rs`**
- CSV parsing and validation
- Batch processing for performance
- Key generation for time-series storage
- Data serialization with bincode

**`storage/query.rs`**
- Time-range query execution
- RocksDB iterator management
- Result pagination and formatting
- JSON serialization

## Development Workflow

### Building

```bash
# Development build (fast compilation, debug info)
cargo build

# Release build (optimized, no debug info)
cargo build --release

# Check for compilation errors without building
cargo check

# Check with all features
cargo check --all-features
```

### Testing

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_parse_csv

# Run tests in specific module
cargo test storage::ingestion
```

### Code Quality

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt --check

# Run clippy lints
cargo clippy

# Run clippy with all targets
cargo clippy --all-targets --all-features

# Fix clippy warnings automatically (when possible)
cargo clippy --fix
```

### Performance Testing

```bash
# Run benchmarks (when implemented)
cargo bench

# Profile with perf (Linux)
cargo build --release
perf record --call-graph=dwarf ./target/release/pulsora
perf report

# Memory profiling with valgrind
cargo build --release
valgrind --tool=massif ./target/release/pulsora
```

## Adding New Features

### Adding a New API Endpoint

1. **Define the handler in `server.rs`:**
```rust
async fn new_endpoint(
    State(state): State<AppState>,
    // Add extractors as needed
) -> std::result::Result<Json<ApiResponse<ResponseType>>, StatusCode> {
    // Implementation
}
```

2. **Add route in `start()` function:**
```rust
let app = Router::new()
    .route("/new-endpoint", get(new_endpoint))
    // ... other routes
```

3. **Add tests:**
```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_new_endpoint() {
        // Test implementation
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
    return Err(PulsoraError::Config("Validation message".to_string()));
}
```

4. **Update sample configuration file `pulsora.toml`**

### Adding New Data Types

1. **Add variant to `DataType` enum in `schema.rs`:**
```rust
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
    }
    // ... existing cases
}
```

4. **Update JSON conversion in `query.rs`**

## Debugging

### Logging

Use structured logging with `tracing`:

```rust
use tracing::{info, warn, error, debug, trace};

// Log levels from most to least verbose
trace!("Very detailed debugging info");
debug!("Debugging information");
info!("General information");
warn!("Warning message");
error!("Error occurred: {}", error);
```

### Environment Variables

```bash
# Set log level
export RUST_LOG=debug
export RUST_LOG=pulsora=trace

# Enable backtraces on panic
export RUST_BACKTRACE=1
export RUST_BACKTRACE=full
```

### Common Debugging Scenarios

**RocksDB Issues:**
```bash
# Enable RocksDB logging
export RUST_LOG=rocksdb=debug

# Check data directory permissions
ls -la ./data/
```

**CSV Parsing Issues:**
```bash
# Enable detailed ingestion logging
export RUST_LOG=pulsora::storage::ingestion=trace

# Test with minimal CSV
echo "timestamp,value\n2024-01-01 10:00:00,100" | curl -X POST http://localhost:8080/tables/test/ingest -H "Content-Type: text/csv" -d @-
```

**Query Issues:**
```bash
# Enable query debugging
export RUST_LOG=pulsora::storage::query=debug

# Test simple query
curl "http://localhost:8080/tables/test/query?limit=1"
```

## Performance Considerations

### Hot Paths

Critical performance paths to optimize:
1. **CSV parsing** - `ingestion.rs:parse_csv()`
2. **Key generation** - `ingestion.rs:generate_key()`
3. **Data serialization** - `ingestion.rs:serialize_row()`
4. **Query iteration** - `query.rs:execute_query()`
5. **JSON conversion** - `query.rs:convert_row_to_json()`

### Memory Management

- Use `Arc` for shared data structures
- Prefer streaming over loading entire datasets
- Configure appropriate batch sizes
- Monitor memory usage during development

### Async Considerations

- Use `tokio::spawn` for CPU-intensive tasks
- Avoid blocking operations in async contexts
- Use `tokio::task::spawn_blocking` for blocking operations
- Consider backpressure for streaming operations

## Contributing Guidelines

### Code Style

- Follow Rust standard formatting (`cargo fmt`)
- Use meaningful variable and function names
- Add documentation comments for public APIs
- Keep functions focused and small
- Use `Result` types for error handling

### Documentation

- Document all public APIs with `///` comments
- Include examples in documentation
- Update relevant documentation files
- Add inline comments for complex logic

### Testing

- Write unit tests for all new functionality
- Include integration tests for API endpoints
- Test error conditions and edge cases
- Maintain test coverage above 80%

### Performance

- Profile performance-critical changes
- Include benchmarks for new algorithms
- Consider memory usage implications
- Test with realistic data sizes

## Release Process

### Version Management

- Follow semantic versioning (SemVer)
- Update version in `Cargo.toml`
- Tag releases in git
- Maintain changelog

### Build Verification

```bash
# Clean build
cargo clean
cargo build --release

# Run all tests
cargo test --release

# Check for warnings
cargo clippy --release -- -D warnings

# Verify formatting
cargo fmt --check
```

### Documentation Updates

- Update README.md with new features
- Update API documentation
- Update configuration documentation
- Update architecture documentation if needed