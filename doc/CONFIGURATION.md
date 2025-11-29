# Pulsora Configuration Guide

## Overview

Pulsora uses TOML configuration files for all operational settings. The configuration is designed to be minimal yet powerful, focusing on performance tuning and operational requirements. All settings have sensible defaults optimized for typical time-series workloads.

## Configuration File Location

- **Default:** Uses built-in defaults if no config file specified
- **Custom:** Specify with `-c` or `--config` flag: `pulsora -c pulsora.toml`
- **Format:** TOML (Tom's Obvious Minimal Language)
- **Validation:** All settings are validated on startup with descriptive error messages

## Complete Configuration Reference

```toml
# Pulsora Configuration
# High-performance time series database optimized for market data

[server]
# Server binding configuration
host = "0.0.0.0"                  # Bind address (default: "0.0.0.0")
port = 8080                       # Port number (default: 8080)
max_body_size_mb = 0              # Maximum request body size in MB (0 = unlimited)

[storage]
# RocksDB storage configuration
data_dir = "./data"               # Data directory path (default: "./data")
write_buffer_size_mb = 64         # Write buffer size in MB (default: 64)
max_open_files = 1000             # Maximum open files (default: 1000)

[ingestion]
# CSV ingestion performance tuning
max_csv_size_mb = 512             # Maximum CSV file size in MB (default: 512)
batch_size = 10000                # Batch size for database writes (default: 10000)

[performance]
# Storage engine optimizations
compression = "lz4"               # Compression algorithm (default: "lz4")
cache_size_mb = 256               # Block cache size in MB (default: 256)

[logging]
# Logging configuration
level = "info"                    # Log level (default: "info")
format = "pretty"                 # Log format: "pretty" or "json" (default: "pretty")
enable_access_logs = true         # Enable HTTP request logging (default: true)
enable_performance_logs = true    # Enable detailed performance metrics (default: true)
# file_output = "/var/log/pulsora.log"  # Optional file output (default: stdout only)
```

## Configuration Sections

### [server] - HTTP Server Settings

Controls the HTTP server behavior and network binding.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | String | `"0.0.0.0"` | IP address to bind to. Use `"127.0.0.1"` for localhost only |
| `port` | Integer | `8080` | TCP port number for HTTP server (1-65535) |
| `max_body_size_mb` | Integer | `0` | Maximum request body size in MB (0 = unlimited) |

**Examples:**
```toml
[server]
host = "127.0.0.1"      # Localhost only
port = 3000             # Custom port
max_body_size_mb = 100  # Limit uploads to 100MB
```

**Security Considerations:**
- Use `"127.0.0.1"` for development/testing
- Use `"0.0.0.0"` for production with proper firewall rules
- Set `max_body_size_mb` to prevent DoS attacks via large uploads

### [storage] - RocksDB Configuration

Controls the underlying RocksDB storage engine behavior.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_dir` | String | `"./data"` | Directory path for database files |
| `write_buffer_size_mb` | Integer | `64` | Size of RocksDB write buffer in memory (MB) |
| `max_open_files` | Integer | `1000` | Maximum number of open file handles |
| `buffer_size` | Integer | `1000` | Number of rows to buffer in memory before flushing |
| `flush_interval_ms` | Integer | `1000` | Max time (ms) to hold data in buffer (0 = disable time flush) |
| `wal_enabled` | Boolean | `true` | Enable Write-Ahead Log for durability |

**Tuning Guidelines:**

**buffer_size & flush_interval_ms:**
- **Low Latency:** `buffer_size=100`, `flush_interval_ms=100`
- **High Compression:** `buffer_size=10000`, `flush_interval_ms=0` (Batch Only Mode)
- **Balanced:** `buffer_size=1000`, `flush_interval_ms=1000`

**wal_enabled:**
- **True (Default):** Ensures data safety. Rows are written to disk immediately.
- **False:** Maximum write speed, but data in buffer is lost on crash.

**write_buffer_size_mb:**
- **Small datasets (< 1GB):** 32-64 MB
- **Medium datasets (1-10GB):** 64-128 MB
- **Large datasets (> 10GB):** 128-256 MB
- **High write throughput:** 256+ MB

**max_open_files:**
- **Development:** 1000
- **Production (small):** 5000-10000
- **Production (large):** 20000+
- **System limit check:** `ulimit -n`

**data_dir considerations:**
- Use SSD storage for better performance
- Ensure sufficient disk space (data can be 10-50% of original CSV size due to compression)
- Use absolute paths for production deployments

### [ingestion] - CSV Processing Settings

Controls CSV data ingestion behavior and limits.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_csv_size_mb` | Integer | `512` | Maximum size of CSV files to accept (MB) |
| `batch_size` | Integer | `10000` | Number of rows to process in each batch |

**Tuning Guidelines:**

**max_csv_size_mb:**
- Prevents out-of-memory errors from huge files
- Set based on available system memory
- Consider: `max_csv_size_mb * 2 < available_memory_mb`
- Use 0 for unlimited (not recommended for production)

**batch_size:**
- **High throughput:** 50000-100000 rows
- **Memory constrained:** 1000-5000 rows
- **Balanced:** 10000-25000 rows
- **SSD storage:** Higher batch sizes work well
- **HDD storage:** Lower batch sizes reduce I/O pressure

### [performance] - Optimization Settings

Controls storage engine performance optimizations.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `compression` | String | `"lz4"` | Compression algorithm for stored data |
| `cache_size_mb` | Integer | `256` | Block cache size for read performance (MB) |

**compression options:**
- `"none"`: No compression (fastest writes, largest storage)
- `"snappy"`: Fast compression with good ratios
- `"lz4"`: **Recommended** - Best balance of speed and compression
- `"zstd"`: Best compression ratios (slower writes, faster reads)

**Compression Trade-offs:**
| Algorithm | Write Speed | Read Speed | Compression Ratio | Use Case |
|-----------|-------------|------------|-------------------|----------|
| none | Fastest | Fast | 1x | Development, fast storage |
| snappy | Fast | Fast | 2-3x | Balanced workloads |
| lz4 | Fast | Fast | 2-4x | **Recommended default** |
| zstd | Slower | Fastest | 3-5x | Read-heavy workloads |

**cache_size_mb:**
- **Read-heavy workloads:** 512+ MB
- **Write-heavy workloads:** 128-256 MB
- **Memory constrained:** 64-128 MB
- **Rule of thumb:** 10-20% of available RAM

### [logging] - Logging Configuration

Controls application logging behavior and output.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | String | `"info"` | Minimum log level to output |
| `format` | String | `"pretty"` | Log format: "pretty" or "json" |
| `enable_access_logs` | Boolean | `true` | Enable HTTP request/response logging |
| `enable_performance_logs` | Boolean | `true` | Enable detailed performance metrics |
| `file_output` | String (optional) | `None` | Optional file path for log output |

**Log levels (from most to least verbose):**
- `"trace"`: Very detailed debugging information (not recommended for production)
- `"debug"`: Debugging information for development
- `"info"`: **Recommended** - General information and performance metrics
- `"warn"`: Warning messages only
- `"error"`: Error messages only

**Log formats:**
- `"pretty"`: Human-readable format with colors (good for development)
- `"json"`: Structured JSON format (good for log aggregation systems)

**Performance Logging Examples:**
```
INFO http_request{correlation_id=550e8400-e29b-41d4-a716-446655440000}: 📊 Starting CSV ingestion table=stocks size_mb=1.5
INFO http_request{correlation_id=550e8400-e29b-41d4-a716-446655440000}: ✅ CSV ingestion completed rows_inserted=10000 throughput_rows_per_sec=42735.04
```

## Environment Variables

You can override configuration values using environment variables:

```bash
# Override log level (takes precedence over config file)
export RUST_LOG=debug

# Override specific settings (future feature)
export PULSORA_SERVER_PORT=9000
export PULSORA_STORAGE_DATA_DIR=/var/lib/pulsora
export PULSORA_LOGGING_LEVEL=warn
```

**Current Environment Variable Support:**
- `RUST_LOG`: Controls log level (overrides config.logging.level)
- `RUST_BACKTRACE`: Enable stack traces on errors (1 or full)

## Performance Tuning Recipes

### High Throughput Ingestion

Optimized for maximum write performance:

```toml
[server]
max_body_size_mb = 0              # Unlimited uploads

[storage]
write_buffer_size_mb = 256        # Large write buffers
max_open_files = 10000            # More file handles

[ingestion]
batch_size = 50000                # Large batches

[performance]
compression = "lz4"               # Fast compression
cache_size_mb = 128               # Smaller cache for more write memory

[logging]
enable_performance_logs = true    # Monitor throughput
```

### Read-Heavy Workloads

Optimized for query performance:

```toml
[storage]
write_buffer_size_mb = 64         # Standard write buffers

[performance]
compression = "zstd"              # Better compression for faster reads
cache_size_mb = 1024              # Large cache for hot data

[logging]
enable_access_logs = true         # Monitor query patterns
```

### Memory-Constrained Systems

Optimized for low memory usage:

```toml
[server]
max_body_size_mb = 128            # Limit upload size

[storage]
write_buffer_size_mb = 32         # Smaller buffers
max_open_files = 500              # Fewer file handles

[ingestion]
max_csv_size_mb = 128             # Smaller CSV files
batch_size = 5000                 # Smaller batches

[performance]
compression = "zstd"              # Better compression to save space
cache_size_mb = 64                # Minimal cache

[logging]
level = "warn"                    # Reduce log volume
enable_performance_logs = false   # Disable detailed metrics
```

### Development/Testing

Optimized for development workflow:

```toml
[server]
host = "127.0.0.1"                # Localhost only
port = 8080                       # Standard port

[storage]
data_dir = "./test_data"          # Separate test data
write_buffer_size_mb = 16         # Small buffers

[ingestion]
max_csv_size_mb = 64              # Small test files
batch_size = 1000                 # Small batches for testing

[performance]
compression = "lz4"               # Fast for development
cache_size_mb = 64                # Minimal cache

[logging]
level = "debug"                   # Detailed logging
format = "pretty"                 # Human-readable
enable_access_logs = true         # See all requests
enable_performance_logs = true    # Monitor performance
```

### Production Deployment

Optimized for production use:

```toml
[server]
host = "0.0.0.0"                  # Accept external connections
port = 8080                       # Standard port
max_body_size_mb = 512            # Reasonable limit

[storage]
data_dir = "/var/lib/pulsora"     # Standard location
write_buffer_size_mb = 128        # Good performance
max_open_files = 20000            # High limits

[ingestion]
max_csv_size_mb = 1024            # Large files OK
batch_size = 25000                # Balanced performance

[performance]
compression = "lz4"               # Good balance
cache_size_mb = 512               # Substantial cache

[logging]
level = "info"                    # Standard logging
format = "json"                   # Structured logs
enable_access_logs = true         # Audit trail
enable_performance_logs = true    # Monitor performance
file_output = "/var/log/pulsora.log"  # Log to file
```

## Configuration Validation

Pulsora validates all configuration values on startup with detailed error messages:

**Validation Rules:**
- **Port ranges:** 1-65535
- **Memory sizes:** Must be > 0
- **Compression algorithms:** Must be valid option (none, snappy, lz4, zstd)
- **File paths:** Must be accessible and writable
- **Batch sizes:** Must be > 0
- **Log levels:** Must be valid (trace, debug, info, warn, error)
- **Log formats:** Must be "pretty" or "json"

**Example Validation Errors:**
```
Error: Configuration validation failed: Server port cannot be 0
Error: Configuration validation failed: Write buffer size must be greater than 0
Error: Configuration validation failed: Invalid compression algorithm: 'invalid'
Error: Configuration validation failed: Batch size must be greater than 0
```

## Best Practices

### Configuration Management
1. **Start with defaults** and tune based on actual performance metrics
2. **Monitor memory usage** when increasing buffer sizes
3. **Test configuration changes** in non-production environments first
4. **Use appropriate compression** based on CPU vs storage trade-offs
5. **Set file limits** appropriately for your system's ulimit settings
6. **Keep backups** of working configurations
7. **Document changes** and their performance impact

### Security Considerations
1. **Bind address:** Use `127.0.0.1` for development, proper firewall for production
2. **Body size limits:** Set `max_body_size_mb` to prevent DoS attacks
3. **File permissions:** Ensure data directory has appropriate permissions
4. **Log sensitivity:** Be careful with debug logging in production (may contain sensitive data)

### Monitoring and Observability
1. **Enable performance logs** to track system behavior
2. **Use structured logging** (JSON format) for log aggregation
3. **Monitor key metrics:** throughput, latency, compression ratios, cache hit rates
4. **Set up alerts** for error conditions and performance degradation

## Troubleshooting

### Common Issues

**"Too many open files" error:**
- Increase `max_open_files` in config
- Check system ulimit: `ulimit -n`
- Increase system limits if needed: `ulimit -n 65536`
- For permanent changes, edit `/etc/security/limits.conf`

**Out of memory errors:**
- Reduce `write_buffer_size_mb`
- Reduce `batch_size`
- Reduce `max_csv_size_mb`
- Reduce `cache_size_mb`
- Monitor system memory usage: `free -h`

**Slow write performance:**
- Increase `write_buffer_size_mb`
- Increase `batch_size`
- Use `"lz4"` or `"none"` compression
- Check disk I/O: `iostat -x 1`
- Consider SSD storage

**Slow read performance:**
- Increase `cache_size_mb`
- Consider SSD storage
- Use appropriate compression for your data
- Monitor cache hit rates in logs

**Configuration file not found:**
- Check file path and permissions
- Use absolute paths for production
- Verify TOML syntax with online validators

**Invalid TOML syntax:**
- Check for missing quotes around strings
- Verify proper section headers `[section]`
- Ensure proper key = value format
- Use TOML validator tools

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
# Enable debug logging
RUST_LOG=debug cargo run -- -c pulsora.toml

# Enable trace logging (very verbose)
RUST_LOG=trace cargo run -- -c pulsora.toml

# Enable specific module debugging
RUST_LOG=pulsora::storage=debug cargo run -- -c pulsora.toml
```

### Performance Debugging

Monitor system resources during operation:

```bash
# Monitor memory usage
watch -n 1 'free -h && echo "---" && ps aux | grep pulsora'

# Monitor disk I/O
iostat -x 1

# Monitor network connections
netstat -tulpn | grep :8080

# Monitor file descriptors
lsof -p $(pgrep pulsora) | wc -l
```

## Configuration Examples by Use Case

### Time Series Analytics Platform
```toml
[server]
host = "0.0.0.0"
port = 8080
max_body_size_mb = 1024

[storage]
data_dir = "/data/pulsora"
write_buffer_size_mb = 256
max_open_files = 50000

[ingestion]
max_csv_size_mb = 2048
batch_size = 50000

[performance]
compression = "zstd"
cache_size_mb = 2048

[logging]
level = "info"
format = "json"
enable_access_logs = true
enable_performance_logs = true
file_output = "/var/log/pulsora.log"
```

### IoT Data Collection
```toml
[server]
host = "0.0.0.0"
port = 8080
max_body_size_mb = 256

[storage]
data_dir = "/var/lib/pulsora"
write_buffer_size_mb = 128
max_open_files = 10000

[ingestion]
max_csv_size_mb = 512
batch_size = 25000

[performance]
compression = "lz4"
cache_size_mb = 512

[logging]
level = "info"
format = "json"
enable_access_logs = false
enable_performance_logs = true
```

### Financial Market Data
```toml
[server]
host = "127.0.0.1"  # Internal only
port = 8080
max_body_size_mb = 512

[storage]
data_dir = "/secure/pulsora"
write_buffer_size_mb = 512
max_open_files = 20000

[ingestion]
max_csv_size_mb = 1024
batch_size = 100000

[performance]
compression = "lz4"
cache_size_mb = 1024

[logging]
level = "warn"  # Minimal logging for security
format = "json"
enable_access_logs = false
enable_performance_logs = false
file_output = "/secure/logs/pulsora.log"
```

## Advanced Configuration

### Custom RocksDB Tuning

While not directly exposed in the configuration file, Pulsora uses optimized RocksDB settings internally. For advanced users who need custom RocksDB configuration, these settings are applied:

```rust
// Internal RocksDB configuration (not user-configurable)
options.set_write_buffer_size(config.storage.write_buffer_size_mb * 1024 * 1024);
options.set_max_open_files(config.storage.max_open_files);
options.set_compression_type(compression_type);
options.set_block_cache_size(config.performance.cache_size_mb * 1024 * 1024);
options.set_level_compaction_dynamic_level_bytes(true);
options.set_max_background_jobs(8);
options.set_bytes_per_sync(1048576);
```

### Future Configuration Options

Planned configuration enhancements:

```toml
# Future features (not yet implemented)
[clustering]
node_id = "node1"
cluster_peers = ["node2:8081", "node3:8082"]
replication_factor = 3

[security]
enable_tls = true
cert_file = "/etc/pulsora/cert.pem"
key_file = "/etc/pulsora/key.pem"
enable_auth = true
auth_token = "secret"

[monitoring]
enable_metrics = true
metrics_port = 9090
health_check_interval = 30

[retention]
default_retention_days = 365
auto_cleanup = true
cleanup_interval_hours = 24
```

## Migration Guide

### Upgrading Configuration

When upgrading Pulsora versions, configuration compatibility is maintained. However, new options may be added:

1. **Backup current configuration**
2. **Check release notes** for new configuration options
3. **Update configuration file** with new options if desired
4. **Test in non-production environment**
5. **Deploy to production**

### Configuration Schema Versioning

Future versions may include configuration schema versioning:

```toml
# Future feature
[meta]
config_version = "1.0"
```

This will enable automatic configuration migration and validation.
