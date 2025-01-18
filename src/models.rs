use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct StatementRequest {
    pub statement: String,
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<Parameter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>, // "INLINE" or "EXTERNAL_LINKS"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>, // "JSON_ARRAY", "ARROW_STREAM", or "CSV"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>, // e.g. "10s"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_wait_timeout: Option<String>, // "CONTINUE" or "CANCEL"
}

/// Parameter for parameterized SQL statements
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Parameter {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub param_type: Option<String>,
}

/// Response body for statement execution endpoints
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StatementStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<StatementManifest>,
    /// If `disposition=INLINE`, this might contain the result data inline (in `JSON_ARRAY`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<StatementResult>,
}

/// Status object returned by the Databricks API
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementStatus {
    /// Examples: "PENDING", "RUNNING", "FINISHED", "FAILED", "CANCELED", "CLOSED"
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Manifest contains metadata about the result set (schema, external links, etc.)
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementManifest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<ChunkInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    // ... potentially more fields in future describing schema, etc.
}

/// Info about a chunk when inlined or external
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub row_offset: Option<i64>,
    pub row_count: Option<i64>,
    pub byte_count: Option<i64>,
    pub chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_internal_link: Option<String>,
}

/// Represents the chunk data when `disposition = "INLINE"`
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementResult {
    #[serde(skip_serializing_if = "Option::is_none", rename = "data_array")]
    pub data_array: Option<Vec<Vec<Option<String>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_internal_link: Option<String>,
}

/// Represents a single presigned URL or reference to chunk data when `disposition = "EXTERNAL_LINKS"`
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalLink {
    pub file_format: String,
    pub url: String,
    pub expiration: String,
    // Possibly more fields
}

/// Response body for chunk retrieval (GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index})
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_internal_link: Option<String>,
    // For INLINE results:
    #[serde(skip_serializing_if = "Option::is_none", rename = "data_array")]
    pub data_array: Option<Vec<Vec<Option<String>>>>,

    // For EXTERNAL_LINKS results:
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,
}

/// Possible errors encountered by the Databricks client
#[derive(Error, Debug)]
pub enum DatabricksSqlError {
    #[error("HTTP client error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Not Found (404)")]
    NotFound,

    #[error("Unknown error: {0}")]
    Other(String),
}
