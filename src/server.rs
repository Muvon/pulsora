// Copyright 2025 Muvon Un Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::error::Result;
use crate::storage::StorageEngine;

/// Application state shared across all request handlers
#[derive(Clone)]
pub struct AppState {
    /// Storage engine instance
    pub storage: Arc<StorageEngine>,
    /// Configuration
    pub config: Arc<Config>,
}

#[derive(Deserialize)]
pub struct QueryParams {
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Serialize)]
pub struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
        }
    }
}

pub async fn create_app(config: Config) -> Result<Router> {
    // Initialize storage engine
    let storage = Arc::new(StorageEngine::new(&config).await?);
    let config = Arc::new(config);

    let state = AppState {
        storage,
        config: config.clone(),
    };

    // Build router with middleware
    Ok(Router::new()
        .route("/health", get(health_check))
        .route("/tables", get(list_tables))
        .route("/tables/{table}/ingest", post(ingest_data))
        .route("/tables/{table}/query", get(query_data))
        .route("/tables/{table}/schema", get(get_schema))
        .route("/tables/{table}/count", get(get_table_count))
        .route("/tables/{table}/row/{id}", get(get_row_by_id))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            access_logging_middleware,
        ))
        .layer(CorsLayer::permissive())
        .with_state(state))
}

pub async fn start(config: Config) -> Result<()> {
    let app = create_app(config.clone()).await?;

    // Start server
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = TcpListener::bind(&addr).await?;

    info!("🌐 Pulsora server listening on {}", addr);
    info!("📡 Ready to accept connections");
    info!("🔗 Health check: http://{}/health", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn list_tables(State(state): State<AppState>) -> Json<ApiResponse<Vec<String>>> {
    let schemas = state.storage.schemas.read().await;
    let tables = schemas.list_tables();
    Json(ApiResponse::success(tables))
}

async fn health_check() -> Json<ApiResponse<HashMap<String, String>>> {
    let mut status = HashMap::new();
    status.insert("status".to_string(), "healthy".to_string());
    status.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());

    Json(ApiResponse::success(status))
}

async fn ingest_data(
    State(state): State<AppState>,
    Path(table): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> std::result::Result<Json<ApiResponse<HashMap<String, u64>>>, StatusCode> {
    let start_time = Instant::now();

    // Stream the body and collect it in chunks
    let body_stream = body.into_data_stream();

    // Get max body size from config (0 means unlimited)
    let max_size = if state.config.server.max_body_size_mb > 0 {
        Some(state.config.server.max_body_size_mb * 1024 * 1024)
    } else {
        None
    };

    // Process the data in a streaming fashion
    let mut data = Vec::new();
    let mut stream = body_stream;
    let mut total_size = 0usize;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                total_size += bytes.len();

                // Check size limit if configured (0 means unlimited)
                if let Some(max) = max_size {
                    if total_size > max {
                        error!(
                            table = %table,
                            size_mb = total_size / (1024 * 1024),
                            max_mb = state.config.server.max_body_size_mb,
                            "💥 Request body exceeds maximum size"
                        );
                        return Err(StatusCode::PAYLOAD_TOO_LARGE);
                    }
                }

                data.extend_from_slice(&bytes);
            }
            Err(e) => {
                error!(table = %table, error = %e, "💥 Error reading request body");
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    }

    let body_read_time = start_time.elapsed();

    if state.config.logging.enable_performance_logs {
        info!(
            table = %table,
            size_bytes = total_size,
            size_mb = total_size as f64 / (1024.0 * 1024.0),
            body_read_ms = body_read_time.as_millis(),
            "📊 Starting ingestion"
        );
    }

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/csv");

    let result = match content_type {
        "application/vnd.apache.arrow.stream" | "application/arrow" => {
            state.storage.ingest_arrow(&table, data).await
        }
        "application/x-protobuf" | "application/protobuf" => {
            state.storage.ingest_protobuf(&table, data).await
        }
        _ => {
            // Convert bytes to string for CSV
            let csv_string = match String::from_utf8(data) {
                Ok(s) => s,
                Err(e) => {
                    error!(table = %table, error = %e, "💥 Invalid UTF-8 in CSV data");
                    return Err(StatusCode::BAD_REQUEST);
                }
            };
            state.storage.ingest_csv(&table, csv_string).await
        }
    };

    match result {
        Ok(stats) => {
            let total_time = start_time.elapsed();

            if state.config.logging.enable_performance_logs {
                info!(
                    table = %table,
                    rows_inserted = stats.rows_inserted,
                    processing_time_ms = stats.processing_time_ms,
                    total_time_ms = total_time.as_millis(),
                    throughput_rows_per_sec = if total_time.as_secs_f64() > 0.0 {
                        stats.rows_inserted as f64 / total_time.as_secs_f64()
                    } else {
                        0.0
                    },
                    size_mb = total_size as f64 / (1024.0 * 1024.0),
                    "✅ Ingestion completed successfully"
                );
            }

            let mut response = HashMap::new();
            response.insert("rows_inserted".to_string(), stats.rows_inserted);
            response.insert("processing_time_ms".to_string(), stats.processing_time_ms);
            Ok(Json(ApiResponse::success(response)))
        }
        Err(e) => {
            let total_time = start_time.elapsed();
            error!(
                table = %table,
                error = %e,
                total_time_ms = total_time.as_millis(),
                size_mb = total_size as f64 / (1024.0 * 1024.0),
                "💥 Ingestion error"
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn query_data(
    State(state): State<AppState>,
    Path(table): Path<String>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> std::result::Result<Response, StatusCode> {
    let start_time = Instant::now();

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    // FAST PATH: CSV Export
    // Use the optimized query_csv method directly to avoid intermediate allocations
    if accept.contains("text/csv") {
        if state.config.logging.enable_performance_logs {
            debug!(
                table = %table,
                start = ?params.start,
                end = ?params.end,
                limit = ?params.limit,
                offset = ?params.offset,
                "🔍 Starting CSV query execution"
            );
        }

        match state
            .storage
            .query_csv(
                &table,
                params.start,
                params.end,
                params.limit,
                params.offset,
            )
            .await
        {
            Ok(csv_data) => {
                let duration = start_time.elapsed();
                // Estimate row count from newlines (approximate)
                let row_count = csv_data.lines().count().saturating_sub(1);

                if state.config.logging.enable_performance_logs {
                    info!(
                        table = %table,
                        result_count = row_count,
                        duration_ms = duration.as_millis(),
                        throughput_rows_per_sec = if duration.as_secs_f64() > 0.0 {
                            row_count as f64 / duration.as_secs_f64()
                        } else {
                            0.0
                        },
                        "✅ CSV Query completed successfully"
                    );
                }

                return Ok(Response::builder()
                    .header("Content-Type", "text/csv")
                    .body(Body::from(csv_data))
                    .unwrap());
            }
            Err(e) => {
                let duration = start_time.elapsed();
                error!(
                    table = %table,
                    error = %e,
                    duration_ms = duration.as_millis(),
                    "💥 CSV Query error"
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    // FAST PATH: Arrow Export
    if accept.contains("application/arrow")
        || accept.contains("application/vnd.apache.arrow.stream")
    {
        if state.config.logging.enable_performance_logs {
            debug!(
                table = %table,
                start = ?params.start,
                end = ?params.end,
                limit = ?params.limit,
                offset = ?params.offset,
                "🔍 Starting Arrow query execution"
            );
        }

        match state
            .storage
            .query_arrow(
                &table,
                params.start,
                params.end,
                params.limit,
                params.offset,
            )
            .await
        {
            Ok(arrow_data) => {
                let duration = start_time.elapsed();
                // We don't easily know row count without parsing, so skip it or estimate
                // For now, just log success

                if state.config.logging.enable_performance_logs {
                    info!(
                        table = %table,
                        size_bytes = arrow_data.len(),
                        duration_ms = duration.as_millis(),
                        "✅ Arrow Query completed successfully"
                    );
                }

                return Ok(Response::builder()
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .body(Body::from(arrow_data))
                    .unwrap());
            }
            Err(e) => {
                let duration = start_time.elapsed();
                error!(
                    table = %table,
                    error = %e,
                    duration_ms = duration.as_millis(),
                    "💥 Arrow Query error"
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    if state.config.logging.enable_performance_logs {
        debug!(
            table = %table,
            start = ?params.start,
            end = ?params.end,
            limit = ?params.limit,
            offset = ?params.offset,
            "🔍 Starting query execution"
        );
    }

    match state
        .storage
        .query(
            &table,
            params.start,
            params.end,
            params.limit,
            params.offset,
        )
        .await
    {
        Ok(results) => {
            let duration = start_time.elapsed();
            let result_count = results.len();

            if state.config.logging.enable_performance_logs {
                info!(
                    table = %table,
                    result_count = result_count,
                    duration_ms = duration.as_millis(),
                    throughput_rows_per_sec = if duration.as_secs_f64() > 0.0 {
                        result_count as f64 / duration.as_secs_f64()
                    } else {
                        0.0
                    },
                    "✅ Query completed successfully"
                );
            }

            if accept.contains("application/arrow")
                || accept.contains("application/vnd.apache.arrow.stream")
            {
                // This path should have been handled by the FAST PATH above
                // But if we fall through (e.g. query returned results but fast path failed?),
                // we can still try to convert.
                // However, since we removed the manual conversion logic to avoid duplication,
                // we should probably just return JSON or error.
                // Or better, since we handle Arrow in FAST PATH, this branch is unreachable
                // for Arrow unless something weird happens.

                // Let's just remove this branch or make it a fallback
                Ok(Json(ApiResponse::success(results)).into_response())
            } else if accept.contains("application/protobuf")
                || accept.contains("application/x-protobuf")
            {
                // Convert to Protobuf
                // We need to map JSON values to ProtoRow
                use crate::storage::ingestion::{ProtoBatch, ProtoRow};
                let mut proto_rows = Vec::with_capacity(results.len());
                for val in results {
                    if let serde_json::Value::Object(map) = val {
                        let mut values = HashMap::new();
                        for (k, v) in map {
                            let v_str = match v {
                                serde_json::Value::String(s) => s,
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => String::new(),
                                _ => v.to_string(),
                            };
                            values.insert(k, v_str);
                        }
                        proto_rows.push(ProtoRow { values });
                    }
                }
                let batch = ProtoBatch { rows: proto_rows };
                use prost::Message;
                let bytes = batch.encode_to_vec();

                Ok(Response::builder()
                    .header("Content-Type", "application/x-protobuf")
                    .body(Body::from(bytes))
                    .unwrap())
            } else if accept.contains("text/csv") {
                // Fetch schema to ensure correct column order
                let schema_json = match state.storage.get_schema(&table).await {
                    Ok(s) => s,
                    Err(_) => return Ok(Json(ApiResponse::success(results)).into_response()),
                };

                let schema: crate::storage::schema::Schema =
                    serde_json::from_value(schema_json).unwrap();
                let mut wtr = csv::Writer::from_writer(Vec::new());

                // Write headers
                let headers: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                wtr.write_record(&headers).unwrap();

                // Write rows
                for row in results {
                    let record: Vec<String> = headers
                        .iter()
                        .map(|header| {
                            row.get(header)
                                .map(|v| match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Null => String::new(),
                                    serde_json::Value::Number(n) => n.to_string(),
                                    serde_json::Value::Bool(b) => b.to_string(),
                                    _ => v.to_string(),
                                })
                                .unwrap_or_default()
                        })
                        .collect();
                    wtr.write_record(&record).unwrap();
                }

                let data = wtr.into_inner().unwrap();

                Ok(Response::builder()
                    .header("Content-Type", "text/csv")
                    .body(Body::from(data))
                    .unwrap())
            } else {
                Ok(Json(ApiResponse::success(results)).into_response())
            }
        }
        Err(e) => {
            let duration = start_time.elapsed();
            error!(
                table = %table,
                error = %e,
                duration_ms = duration.as_millis(),
                "💥 Query error"
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_table_count(
    Path(table): Path<String>,
    State(state): State<AppState>,
) -> Json<ApiResponse<HashMap<String, u64>>> {
    match state.storage.get_table_count(&table).await {
        Ok(count) => {
            let mut result = HashMap::new();
            result.insert("count".to_string(), count);
            Json(ApiResponse::success(result))
        }
        Err(e) => {
            error!("Failed to get table count: {}", e);
            Json(ApiResponse::error(e.to_string()))
        }
    }
}

async fn get_schema(
    State(state): State<AppState>,
    Path(table): Path<String>,
) -> std::result::Result<Json<ApiResponse<serde_json::Value>>, StatusCode> {
    match state.storage.get_schema(&table).await {
        Ok(schema) => Ok(Json(ApiResponse::success(schema))),
        Err(e) => {
            error!("Schema error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_row_by_id(
    State(state): State<AppState>,
    Path((table, id)): Path<(String, u64)>,
) -> std::result::Result<Json<ApiResponse<Option<serde_json::Value>>>, StatusCode> {
    match state.storage.get_row_by_id_json(&table, id).await {
        Ok(row) => Ok(Json(ApiResponse::success(row))),
        Err(e) => {
            error!("Get row by ID error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Access logging middleware
async fn access_logging_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Skip access logging if disabled in config
    if !state.config.logging.enable_access_logs {
        return next.run(request).await;
    }

    let start_time = Instant::now();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let version = request.version();
    let headers = request.headers().clone();

    // Generate correlation ID for request tracing
    let correlation_id = Uuid::new_v4().to_string();

    // Get content length if available
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Get user agent
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");

    // Create a span for this request
    let span = tracing::info_span!(
        "http_request",
        correlation_id = %correlation_id,
        method = %method,
        uri = %uri.path(),
        version = ?version
    );

    let _enter = span.enter();

    debug!(
        correlation_id = %correlation_id,
        method = %method,
        uri = %uri,
        content_length = content_length,
        user_agent = user_agent,
        "📥 Incoming request"
    );

    // Process the request
    let response = next.run(request).await;

    let duration = start_time.elapsed();
    let status = response.status();

    // Log the response
    let log_level = match status.as_u16() {
        200..=299 => tracing::Level::INFO,
        300..=399 => tracing::Level::INFO,
        400..=499 => tracing::Level::WARN,
        500..=599 => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };

    match log_level {
        tracing::Level::ERROR => {
            error!(
                correlation_id = %correlation_id,
                method = %method,
                uri = %uri.path(),
                status = status.as_u16(),
                duration_ms = duration.as_millis(),
                content_length = content_length,
                "📤 Request completed with error"
            );
        }
        tracing::Level::WARN => {
            warn!(
                correlation_id = %correlation_id,
                method = %method,
                uri = %uri.path(),
                status = status.as_u16(),
                duration_ms = duration.as_millis(),
                content_length = content_length,
                "📤 Request completed with warning"
            );
        }
        _ => {
            info!(
                correlation_id = %correlation_id,
                method = %method,
                uri = %uri.path(),
                status = status.as_u16(),
                duration_ms = duration.as_millis(),
                content_length = content_length,
                "📤 Request completed successfully"
            );
        }
    }

    response
}

#[cfg(test)]
#[path = "server_test.rs"]
mod server_test;
