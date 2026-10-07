use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use pulsora::config::Config;
use pulsora::server::create_app;
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt; // for oneshot

#[tokio::test]
async fn test_health_check() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let app = create_app(config).await.unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    assert!(body.get("success").unwrap().as_bool().unwrap());
    assert_eq!(
        body.get("data")
            .unwrap()
            .get("status")
            .unwrap()
            .as_str()
            .unwrap(),
        "healthy"
    );
}

#[tokio::test]
async fn test_ingest_and_query_csv() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let app = create_app(config).await.unwrap();
    let table = "api_test_table";

    // 1. Ingest CSV
    let csv_data = "id,timestamp,value\n1,1704067200000,100\n2,1704067201000,200";
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/tables/{}/ingest", table))
                .header("Content-Type", "text/csv")
                .body(Body::from(csv_data))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 2. Query
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/tables/{}/query", table))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    let results = body.get("data").unwrap().as_array().unwrap();

    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_list_tables() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let app = create_app(config).await.unwrap();
    let table = "api_list_test";

    // Ingest to create table
    let csv_data = "id,val\n1,100";
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/tables/{}/ingest", table))
                .header("Content-Type", "text/csv")
                .body(Body::from(csv_data))
                .unwrap(),
        )
        .await
        .unwrap();

    // List tables
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();

    let tables = body.get("data").unwrap().as_array().unwrap();
    assert!(tables.iter().any(|t| t.as_str().unwrap() == table));
}

#[tokio::test]
async fn test_count_includes_buffer_and_additive_timestamp_bounds() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_string_lossy().to_string();
    config.storage.flush_interval_ms = 0;
    let app = create_app(config).await.unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/count_test/ingest")
                .header("Content-Type", "text/csv")
                .body(Body::from(
                    "id,timestamp,value\n1,1704067200000,10\n2,1704067201000,20\n",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/tables/count_test/count")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["count"], 2);
    assert_eq!(json["data"]["min_ts"], 1704067200000i64);
    assert_eq!(json["data"]["max_ts"], 1704067201000i64);
}
