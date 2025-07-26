use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
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

    let state = AppState { storage };

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
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

async fn health_check() -> Json<ApiResponse<HashMap<String, String>>> {
    let mut status = HashMap::new();
    status.insert("status".to_string(), "healthy".to_string());
    status.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());

    Json(ApiResponse::success(status))
}

async fn ingest_csv(
    State(state): State<AppState>,
    Path(table): Path<String>,
    body: String,
) -> std::result::Result<Json<ApiResponse<HashMap<String, u64>>>, StatusCode> {
    match state.storage.ingest_csv(&table, body).await {
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
