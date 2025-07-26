# Pulsora API Documentation

## Overview

Pulsora provides a RESTful API for high-performance time series data ingestion and querying. All endpoints return JSON responses with a consistent structure.

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

### CSV Data Ingestion

**POST** `/tables/{table_name}/ingest`

Ingests CSV data into the specified table. Schema is automatically inferred from the first ingestion.

**Headers:**
- `Content-Type: text/csv`

**Request Body:**
CSV data with headers in the first row.

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

### Query Data

**GET** `/tables/{table_name}/query`

Queries data from the specified table with optional time range and pagination.

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
      "timestamp": "2024-01-01 10:00:00",
      "symbol": "AAPL",
      "price": 150.25,
      "volume": 1000
    },
    {
      "timestamp": "2024-01-01 10:01:00",
      "symbol": "AAPL",
      "price": 150.50,
      "volume": 1500
    }
  ],
  "error": null
}
```

### Get Schema

**GET** `/tables/{table_name}/schema`

Returns the schema information for the specified table.

**Response:**
```json
{
  "success": true,
  "data": {
    "table_name": "market_data",
    "columns": [
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

Pulsora automatically infers data types from CSV data:

- **Integer**: Whole numbers (e.g., `1000`, `-50`)
- **Float**: Decimal numbers (e.g., `150.25`, `-0.5`)
- **String**: Text data (e.g., `"AAPL"`, `"Hello World"`)
- **Boolean**: True/false values (case-insensitive)
- **Timestamp**: Date/time values in various formats

## Timestamp Formats

Supported timestamp formats:
- ISO 8601: `2024-01-01T10:00:00Z`
- Common format: `2024-01-01 10:00:00`
- Date only: `2024-01-01`
- Unix timestamp (seconds): `1704110400`
- Unix timestamp (milliseconds): `1704110400000`

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
- `500 Internal Server Error`: Server-side error

## Rate Limiting

Currently no rate limiting is implemented. This will be added in future versions based on configuration settings.

## CORS

CORS is enabled for all origins in development. Configure appropriately for production use.