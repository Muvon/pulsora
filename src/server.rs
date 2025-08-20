use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

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
}

pub async fn start(config: Config) -> Result<()> {
    // Initialize storage engine
    let storage = Arc::new(StorageEngine::new(&config).await?);
    let config = Arc::new(config);

    let state = AppState {
        storage,
        config: config.clone(),
    };

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/tables", get(list_tables))
        .route("/tables/:table/ingest", post(ingest_csv))
        .route("/tables/:table/query", get(query_data))
        .route("/tables/:table/schema", get(get_schema))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Start server
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = TcpListener::bind(&addr).await?;

    info!("Pulsora server listening on {}", addr);

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

async fn ingest_csv(
    State(state): State<AppState>,
    Path(table): Path<String>,
    body: Body,
) -> std::result::Result<Json<ApiResponse<HashMap<String, u64>>>, StatusCode> {
    // Stream the body and collect it in chunks
    let body_stream = body.into_data_stream();

    // Get max body size from config (0 means unlimited)
    let max_size = if state.config.server.max_body_size_mb > 0 {
        Some(state.config.server.max_body_size_mb * 1024 * 1024)
    } else {
        None
    };

    // Process the CSV data in a streaming fashion
    let mut csv_data = Vec::new();
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
                            "Request body exceeds maximum size of {} MB",
                            state.config.server.max_body_size_mb
                        );
                        return Err(StatusCode::PAYLOAD_TOO_LARGE);
                    }
                }

                csv_data.extend_from_slice(&bytes);
            }
            Err(e) => {
                error!("Error reading request body: {}", e);
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    }

    // Convert bytes to string
    let csv_string = match String::from_utf8(csv_data) {
        Ok(s) => s,
        Err(e) => {
            error!("Invalid UTF-8 in CSV data: {}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    match state.storage.ingest_csv(&table, csv_string).await {
        Ok(stats) => {
            let mut response = HashMap::new();
            response.insert("rows_inserted".to_string(), stats.rows_inserted);
            response.insert("processing_time_ms".to_string(), stats.processing_time_ms);
            Ok(Json(ApiResponse::success(response)))
        }
        Err(e) => {
            error!("Ingestion error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn query_data(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Query(params): Query<QueryParams>,
) -> std::result::Result<Json<ApiResponse<Vec<serde_json::Value>>>, StatusCode> {
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
        Ok(results) => Ok(Json(ApiResponse::success(results))),
        Err(e) => {
            error!("Query error: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
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
