# Pulsora API Documentation

## Overview

Pulsora provides a RESTful API for high-performance time series data ingestion and querying. All endpoints return JSON responses with a consistent structure and include comprehensive logging with correlation IDs for request tracing.

## Response Format

All API responses follow this structure:

```json
{
  "success": boolean,
  "data": object | null,
  "error": string | null
}
```

## Endpoints

### Health Check

**GET** `/health`

Returns server health status and version information.

**Response:**
```json
{
  "success": true,
  "data": {
    "status": "healthy",
    "version": "0.1.0"
  },
  "error": null
}
```

### List Tables

**GET** `/tables`

Returns a list of all available tables in the database.

**Response:**
```json
{
  "success": true,
  "data": ["market_data", "sensor_readings", "user_events"],
  "error": null
}
```

### CSV Data Ingestion

**POST** `/tables/{table_name}/ingest`

Ingests CSV data into the specified table. Schema is automatically inferred from the first ingestion. Supports streaming ingestion with configurable body size limits.

**Headers:**
- `Content-Type: text/csv`

**Request Body:**
CSV data with headers in the first row.

**Configuration:**
- Maximum body size configurable via `server.max_body_size_mb` (0 = unlimited)
- Batch processing with configurable `ingestion.batch_size`

**Example:**
```bash
curl -X POST http://localhost:8080/tables/market_data/ingest \
  -H "Content-Type: text/csv" \
  -d "timestamp,symbol,price,volume
2024-01-01 10:00:00,AAPL,150.25,1000
2024-01-01 10:01:00,AAPL,150.50,1500"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "rows_inserted": 2,
    "processing_time_ms": 15
  },
  "error": null
}
```

**Performance Logging:**
When `logging.enable_performance_logs = true`, detailed ingestion metrics are logged including throughput, processing time, and data size.

### Query Data

**GET** `/tables/{table_name}/query`

Queries data from the specified table with optional time range and pagination. Uses efficient binary key encoding for time-based queries.

**Query Parameters:**
- `start` (optional): Start timestamp (ISO format, Unix timestamp, or common formats)
- `end` (optional): End timestamp (ISO format, Unix timestamp, or common formats)
- `limit` (optional): Maximum number of rows to return (default: 1000, max: 10000)
- `offset` (optional): Number of rows to skip (default: 0)

**Examples:**

Get all data:
```bash
curl "http://localhost:8080/tables/market_data/query"
```

Time range query:
```bash
curl "http://localhost:8080/tables/market_data/query?start=2024-01-01&end=2024-01-02"
```

With pagination:
```bash
curl "http://localhost:8080/tables/market_data/query?limit=100&offset=0"
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "timestamp": "2024-01-01 10:00:00",
      "symbol": "AAPL",
      "price": 150.25,
      "volume": 1000
    },
    {
      "id": 2,
      "timestamp": "2024-01-01 10:01:00",
      "symbol": "AAPL",
      "price": 150.50,
      "volume": 1500
    }
  ],
  "error": null
}
```

### Get Table Count

**GET** `/tables/{table_name}/count`

Returns the total number of rows in the specified table.

**Response:**
```json
{
  "success": true,
  "data": {
    "count": 1500000
  },
  "error": null
}
```

### Get Row by ID

**GET** `/tables/{table_name}/rows/{id}`

Retrieves a specific row by its unique ID.

**Response:**
```json
{
  "success": true,
  "data": {
    "id": 1,
    "timestamp": "2024-01-01 10:00:00",
    "symbol": "AAPL",
    "price": 150.25,
    "volume": 1000
  },
  "error": null
}
```

If row not found:
```json
{
  "success": true,
  "data": null,
  "error": null
}
```

### Get Schema

**GET** `/tables/{table_name}/schema`

Returns the schema information for the specified table, including column definitions and metadata.

**Response:**
```json
{
  "success": true,
  "data": {
    "table_name": "market_data",
    "columns": [
      {
        "name": "id",
        "data_type": "Integer",
        "nullable": false
      },
      {
        "name": "timestamp",
        "data_type": "Timestamp",
        "nullable": false
      },
      {
        "name": "symbol",
        "data_type": "String",
        "nullable": false
      },
      {
        "name": "price",
        "data_type": "Float",
        "nullable": false
      },
      {
        "name": "volume",
        "data_type": "Integer",
        "nullable": false
      }
    ],
    "timestamp_column": "timestamp",
    "created_at": "2024-01-01T10:00:00Z"
  },
  "error": null
}
```

## Data Types

Pulsora automatically infers and validates these data types from CSV data:

- **Integer**: Whole numbers (e.g., `1000`, `-50`) - compressed with delta + varint encoding
- **Float**: Decimal numbers (e.g., `150.25`, `-0.5`) - compressed with XOR (Gorilla) + varfloat encoding
- **String**: Text data (e.g., `"AAPL"`, `"Hello World"`) - compressed with dictionary encoding for repetitive values
- **Boolean**: True/false values (case-insensitive) - compressed with run-length encoding
- **Timestamp**: Date/time values in various formats - compressed with delta-of-delta + varint encoding

## Timestamp Formats

Supported timestamp formats for automatic detection:

- **ISO 8601**: `2024-01-01T10:00:00Z`, `2024-01-01T10:00:00+00:00`
- **Common format**: `2024-01-01 10:00:00`, `2024-01-01 10:00:00.123`
- **Date only**: `2024-01-01`
- **Unix timestamp (seconds)**: `1704110400`
- **Unix timestamp (milliseconds)**: `1704110400000`

The system automatically detects the timestamp column and uses it for time-based indexing and queries.

## Compression and Performance

Pulsora uses type-specific compression algorithms optimized for time-series data:

| Data Type | Compression Algorithm | Typical Ratio | Use Case |
|-----------|----------------------|---------------|----------|
| Timestamp | Delta-of-delta + varint | 5-10x | Regular intervals |
| Float | XOR (Gorilla) + varfloat | 2-5x | Slowly changing values |
| Integer | Delta + varint | 3-8x | Sequential/counter data |
| String | Dictionary encoding | 2-4x | Repetitive text |
| Boolean | Run-length encoding | 10-50x | Sparse data |

## Error Handling

Error responses include details about what went wrong:

```json
{
  "success": false,
  "data": null,
  "error": "Table 'nonexistent' not found"
}
```

Common HTTP status codes:
- `200 OK`: Successful operation
- `400 Bad Request`: Invalid request format or parameters
- `404 Not Found`: Table or resource not found
- `413 Payload Too Large`: Request body exceeds configured limit
- `500 Internal Server Error`: Server-side error

## Request Tracing and Logging

When `logging.enable_access_logs = true`, all requests include:

- **Correlation ID**: Unique identifier for request tracing
- **Performance metrics**: Response time, throughput, data size
- **Structured logging**: JSON or pretty format based on configuration

Example log entry:
```
INFO http_request{correlation_id=550e8400-e29b-41d4-a716-446655440000 method=POST uri=/tables/market_data/ingest}: 📤 Request completed successfully status=200 duration_ms=45 content_length=1024
```

## Configuration Impact on API

Several configuration options affect API behavior:

### Server Configuration
```toml
[server]
max_body_size_mb = 512  # Limits CSV upload size (0 = unlimited)
```

### Performance Configuration
```toml
[performance]
cache_size_mb = 256     # Affects query response times
```

### Logging Configuration
```toml
[logging]
enable_access_logs = true        # Request/response logging
enable_performance_logs = true   # Detailed performance metrics
```

## Rate Limiting

Currently no rate limiting is implemented. This will be added in future versions based on configuration settings.

## CORS

CORS is enabled for all origins in development. Configure appropriately for production use.

## Best Practices

### Ingestion
- Use appropriate batch sizes (10,000-50,000 rows for optimal performance)
- Include proper timestamp columns for time-series optimization
- Use consistent data types within columns for better compression

### Querying
- Use time range queries when possible for better performance
- Implement pagination for large result sets
- Cache frequently accessed data at the application level

### Schema Design
- Choose descriptive column names
- Use appropriate data types (integers vs floats)
- Include timestamp columns for time-series data

## Examples

### Complete Workflow Example

```bash
# 1. Check server health
curl "http://localhost:8080/health"

# 2. Ingest sample data
curl -X POST http://localhost:8080/tables/stocks/ingest \
  -H "Content-Type: text/csv" \
  -d "timestamp,symbol,price,volume
2024-01-01 09:30:00,AAPL,150.00,1000
2024-01-01 09:31:00,AAPL,150.25,1500
2024-01-01 09:32:00,AAPL,149.75,2000"

# 3. Query recent data
curl "http://localhost:8080/tables/stocks/query?limit=10"

# 4. Get table schema
curl "http://localhost:8080/tables/stocks/schema"

# 5. Get table count
curl "http://localhost:8080/tables/stocks/count"

# 6. Get specific row
curl "http://localhost:8080/tables/stocks/rows/1"

# 7. List all tables
curl "http://localhost:8080/tables"
```

### Time Range Query Examples

```bash
# Last hour
curl "http://localhost:8080/tables/stocks/query?start=2024-01-01T09:00:00&end=2024-01-01T10:00:00"

# Unix timestamp range
curl "http://localhost:8080/tables/stocks/query?start=1704110400&end=1704114000"

# Date range (full days)
curl "http://localhost:8080/tables/stocks/query?start=2024-01-01&end=2024-01-02"
```
