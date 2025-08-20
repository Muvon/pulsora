# Pulsora Configuration Guide

## Overview

Pulsora uses TOML configuration files for all operational settings. The configuration is designed to be minimal yet powerful, focusing on performance tuning and operational requirements.

## Configuration File Location

- **Default:** Uses built-in defaults if no config file specified
- **Custom:** Specify with `-c` or `--config` flag: `pulsora -c pulsora.toml`
- **Format:** TOML (Tom's Obvious Minimal Language)

## Complete Configuration Reference

```toml
# Pulsora Configuration
# High-performance time series database optimized for market data

[server]
# Server binding configuration
host = "0.0.0.0"        # Bind address (default: "0.0.0.0")
port = 8080             # Port number (default: 8080)

[storage]
# RocksDB storage configuration
data_dir = "./data"                # Data directory path (default: "./data")
write_buffer_size_mb = 64          # Write buffer size in MB (default: 64)
max_open_files = 1000              # Maximum open files (default: 1000)

[ingestion]
# CSV ingestion performance tuning
max_csv_size_mb = 512              # Maximum CSV file size in MB (default: 512)
batch_size = 10000                 # Batch size for database writes (default: 10000)

[performance]
# Storage engine optimizations
compression = "lz4"                # Compression algorithm (default: "lz4")
cache_size_mb = 256                # Block cache size in MB (default: 256)

[logging]
# Logging configuration
level = "info"                     # Log level (default: "info")
```

## Configuration Sections

### [server] - HTTP Server Settings

Controls the HTTP server behavior and network binding.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | String | `"0.0.0.0"` | IP address to bind to. Use `"127.0.0.1"` for localhost only |
| `port` | Integer | `8080` | TCP port number for HTTP server |

**Examples:**
```toml
[server]
host = "127.0.0.1"  # Localhost only
port = 3000         # Custom port
```

### [storage] - RocksDB Configuration

Controls the underlying RocksDB storage engine behavior.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_dir` | String | `"./data"` | Directory path for database files |
| `write_buffer_size_mb` | Integer | `64` | Size of write buffer in memory (MB) |
| `max_open_files` | Integer | `1000` | Maximum number of open file handles |

**Tuning Guidelines:**

**write_buffer_size_mb:**
- **Small datasets (< 1GB):** 32-64 MB
- **Medium datasets (1-10GB):** 64-128 MB
- **Large datasets (> 10GB):** 128-256 MB
- **High write throughput:** 256+ MB

**max_open_files:**
- **Development:** 1000
- **Production (small):** 5000-10000
- **Production (large):** 20000+

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

**batch_size:**
- **High throughput:** 50000-100000 rows
- **Memory constrained:** 1000-5000 rows
- **Balanced:** 10000-25000 rows

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
- `"zstd"`: Best compression ratios (slower)

**cache_size_mb:**
- **Read-heavy workloads:** 512+ MB
- **Write-heavy workloads:** 128-256 MB
- **Memory constrained:** 64-128 MB

### [logging] - Logging Configuration

Controls application logging behavior.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | String | `"info"` | Minimum log level to output |

**Log levels (from most to least verbose):**
- `"trace"`: Very detailed debugging information
- `"debug"`: Debugging information
- `"info"`: **Recommended** - General information
- `"warn"`: Warning messages only
- `"error"`: Error messages only

## Environment Variables

You can override configuration values using environment variables:

```bash
# Override log level
export RUST_LOG=debug

# Override specific settings (future feature)
export PULSORA_SERVER_PORT=9000
export PULSORA_STORAGE_DATA_DIR=/var/lib/pulsora
```

## Performance Tuning Recipes

### High Throughput Ingestion

```toml
[storage]
write_buffer_size_mb = 256
max_open_files = 10000

[ingestion]
batch_size = 50000

[performance]
compression = "lz4"
cache_size_mb = 128
```

### Read-Heavy Workloads

```toml
[storage]
write_buffer_size_mb = 64

[performance]
compression = "zstd"
cache_size_mb = 1024
```

### Memory-Constrained Systems

```toml
[storage]
write_buffer_size_mb = 32
max_open_files = 500

[ingestion]
max_csv_size_mb = 128
batch_size = 5000

[performance]
cache_size_mb = 64
```

### Development/Testing

```toml
[server]
host = "127.0.0.1"
port = 8080

[storage]
data_dir = "./test_data"
write_buffer_size_mb = 16

[ingestion]
max_csv_size_mb = 64
batch_size = 1000

[logging]
level = "debug"
```

## Configuration Validation

Pulsora validates all configuration values on startup:

- **Port ranges:** 1-65535
- **Memory sizes:** Must be > 0
- **Compression algorithms:** Must be valid option
- **File paths:** Must be accessible
- **Batch sizes:** Must be > 0

Invalid configurations will cause startup failure with descriptive error messages.

## Best Practices

1. **Start with defaults** and tune based on actual performance metrics
2. **Monitor memory usage** when increasing buffer sizes
3. **Test configuration changes** in non-production environments first
4. **Use appropriate compression** based on CPU vs storage trade-offs
5. **Set file limits** appropriately for your system's ulimit settings
6. **Keep backups** of working configurations

## Troubleshooting

### Common Issues

**"Too many open files" error:**
- Increase `max_open_files` in config
- Check system ulimit: `ulimit -n`
- Increase system limits if needed

**Out of memory errors:**
- Reduce `write_buffer_size_mb`
- Reduce `batch_size`
- Reduce `max_csv_size_mb`

**Slow write performance:**
- Increase `write_buffer_size_mb`
- Increase `batch_size`
- Use `"lz4"` or `"none"` compression

**Slow read performance:**
- Increase `cache_size_mb`
- Consider SSD storage
- Use appropriate compression for your data
