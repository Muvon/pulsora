# Pulsora Documentation

Welcome to the Pulsora documentation! This directory contains comprehensive guides for using, configuring, developing, and optimizing Pulsora - a high-performance time-series database built with Rust.

## Documentation Overview

### 📚 User Documentation

- **[API.md](API.md)** - Complete REST API reference with examples and usage patterns
- **[CONFIGURATION.md](CONFIGURATION.md)** - Configuration guide and performance tuning recipes

### 🏗️ Technical Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture, design decisions, and implementation details
- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Development setup, contribution guide, and coding standards

### 📊 Performance Documentation

- **[BENCHMARKING.md](BENCHMARKING.md)** - Comprehensive benchmarking guide and methodology
- **[BENCH.md](BENCH.md)** - Benchmark results, analysis, and performance characteristics

## Quick Start Guide

### For New Users
1. Read the main [README.md](../README.md) for installation and basic usage
2. Check [API.md](API.md) for endpoint documentation and examples
3. Review [CONFIGURATION.md](CONFIGURATION.md) for performance tuning

### For Developers
1. Start with [DEVELOPMENT.md](DEVELOPMENT.md) for setup instructions and project structure
2. Review [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system design
3. Follow the contribution guidelines in the development guide

### For Operations Teams
1. Use [CONFIGURATION.md](CONFIGURATION.md) for production deployment settings
2. Reference [API.md](API.md) for monitoring and health checks
3. Check [ARCHITECTURE.md](ARCHITECTURE.md) for performance characteristics and scaling

### For Performance Analysis
1. Review [BENCH.md](BENCH.md) for current performance baselines
2. Use [BENCHMARKING.md](BENCHMARKING.md) for running custom benchmarks
3. Check [ARCHITECTURE.md](ARCHITECTURE.md) for optimization opportunities

## Key Features Covered

### High-Performance Ingestion
- **350K+ rows/second** CSV ingestion with streaming support
- **Type-specific compression** achieving 2-10x compression ratios
- **Dynamic schema inference** with automatic timestamp detection
- **Batch processing** with configurable chunk sizes

### Efficient Querying
- **Sub-millisecond queries** with intelligent block caching
- **Binary key encoding** for efficient time-range queries
- **Columnar storage** with lazy decompression
- **Pagination support** with offset-based navigation

### Advanced Compression
- **Gorilla (XOR) compression** for floating-point values (2-5x compression)
- **Delta-of-delta compression** for timestamps (5-10x compression)
- **Varint encoding** for integers (3-8x compression)
- **Dictionary encoding** for repetitive strings (2-4x compression)

### Production-Ready Features
- **TOML configuration** with comprehensive validation
- **Structured logging** with correlation IDs and performance metrics
- **Error handling** with detailed context and recovery
- **Benchmarking suite** for performance validation and regression detection

## Documentation Standards

All documentation follows these principles:

- **Practical**: Includes working examples and real-world scenarios
- **Complete**: Covers all features and configuration options
- **Accurate**: Kept up-to-date with code changes and actual implementation
- **Accessible**: Written for different skill levels and use cases
- **Performance-Focused**: Includes optimization guidance and benchmarking data

## API Reference Quick Links

### Core Endpoints
- `GET /health` - Server health and version information
- `GET /tables` - List all available tables
- `POST /tables/{name}/ingest` - CSV data ingestion with streaming support
- `GET /tables/{name}/query` - Time-range queries with pagination
- `GET /tables/{name}/schema` - Schema information and column definitions
- `GET /tables/{name}/count` - Total row count for table
- `GET /tables/{name}/rows/{id}` - Retrieve specific row by ID

### Configuration Sections
- `[server]` - HTTP server settings (host, port, body limits)
- `[storage]` - RocksDB configuration (data directory, buffers, file limits)
- `[ingestion]` - CSV processing settings (batch size, size limits)
- `[performance]` - Optimization parameters (compression, cache size)
- `[logging]` - Log levels, format, and output options

## Performance Highlights

### Benchmark Results
- **CSV Parsing**: 1.3-1.6M elements/second
- **Data Ingestion**: 240K-465K elements/second end-to-end
- **Query Performance**: 63+ Gelem/s for time-range queries
- **Full Table Scan**: 1.2M-117M elements/second (scales linearly)

### Compression Effectiveness
- **Timestamps**: 5-10x compression with delta-of-delta + varint
- **Floats**: 2-5x compression with XOR (Gorilla) + varfloat
- **Integers**: 3-8x compression with delta + varint
- **Strings**: 2-4x compression with dictionary encoding

## Architecture Highlights

### Storage Engine
- **RocksDB backend** with optimized configuration for time-series workloads
- **Columnar storage** with block-based architecture and intelligent caching
- **Binary key encoding** (20 bytes fixed) for optimal performance
- **Type-specific compression** algorithms for maximum efficiency

### HTTP Server
- **Axum framework** with Tokio async runtime
- **CORS support** and structured error handling
- **Request tracing** with correlation IDs
- **Performance logging** with detailed metrics

### Data Processing
- **Streaming CSV ingestion** with configurable body size limits
- **Dynamic schema inference** analyzing all values for accurate type detection
- **Batch processing** with configurable chunk sizes
- **Validation and error recovery** with detailed error messages

## Contributing to Documentation

When contributing to Pulsora:

1. **Update relevant documentation** for any feature changes
2. **Include examples** in API documentation with working code
3. **Update configuration docs** for new settings with validation rules
4. **Keep architecture docs current** with design changes and performance characteristics
5. **Add benchmark results** for new features or optimizations
6. **Test all examples** to ensure they work with current implementation

### Documentation Maintenance

- **API changes**: Update API.md with new endpoints, parameters, and response formats
- **Configuration changes**: Update CONFIGURATION.md with new options and validation rules
- **Architecture changes**: Update ARCHITECTURE.md with design decisions and implementation details
- **Performance changes**: Update BENCH.md with new benchmark results and analysis
- **Development changes**: Update DEVELOPMENT.md with new dependencies, tools, or workflows

## Support and Troubleshooting

### Common Issues
1. **Configuration errors**: Check CONFIGURATION.md for validation rules and examples
2. **Performance issues**: Review BENCH.md for optimization opportunities
3. **API usage**: Check API.md for endpoint documentation and examples
4. **Development setup**: Follow DEVELOPMENT.md for complete setup instructions

### Getting Help
1. **Check the relevant documentation** first for your specific use case
2. **Review the examples** in each guide for working implementations
3. **Look at the troubleshooting sections** for common issues and solutions
4. **Check the benchmark results** for performance expectations
5. **Open an issue** for missing documentation or unclear instructions

## Version Information

This documentation is current as of Pulsora v0.1.0 and covers:

- **Complete API reference** with all current endpoints
- **Full configuration options** from pulsora.toml and config.rs
- **Actual architecture implementation** from the current codebase
- **Real benchmark results** from the comprehensive test suite
- **Production deployment guidance** based on actual usage patterns

The documentation is automatically updated with each release to ensure accuracy and completeness.
