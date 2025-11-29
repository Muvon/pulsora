use super::*;

#[test]
fn test_api_response_success() {
    let response = ApiResponse::success("test_data");
    assert!(response.success);
    assert_eq!(response.data, Some("test_data"));
    assert_eq!(response.error, None);

    let json = serde_json::to_string(&response).unwrap();
    assert!(json.contains("\"success\":true"));
    assert!(json.contains("\"data\":\"test_data\""));
}

#[test]
fn test_api_response_error() {
    let response: ApiResponse<()> = ApiResponse::error("test_error".to_string());
    assert!(!response.success);
    assert_eq!(response.data, None);
    assert_eq!(response.error, Some("test_error".to_string()));

    let json = serde_json::to_string(&response).unwrap();
    assert!(json.contains("\"success\":false"));
    assert!(json.contains("\"error\":\"test_error\""));
}

#[test]
fn test_query_params_deserialization() {
    let json = r#"{"start": "100", "end": "200", "limit": 10, "offset": 5}"#;
    let params: QueryParams = serde_json::from_str(json).unwrap();

    assert_eq!(params.start, Some("100".to_string()));
    assert_eq!(params.end, Some("200".to_string()));
    assert_eq!(params.limit, Some(10));
    assert_eq!(params.offset, Some(5));
}

#[test]
fn test_query_params_optional() {
    let json = r#"{}"#;
    let params: QueryParams = serde_json::from_str(json).unwrap();

    assert_eq!(params.start, None);
    assert_eq!(params.end, None);
    assert_eq!(params.limit, None);
    assert_eq!(params.offset, None);
}
