use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Request body when executing a SQL statement
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct StatementRequest {
    pub statement: String,
    pub warehouse_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_wait_timeout: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>, /// "INLINE" or "EXTERNAL_LINKS"

    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,  /// For controlling how many bytes of data to return (up to 100GB).

    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>, // "JSON_ARRAY", "ARROW_STREAM", or "CSV"
}

/// Top-level statement response
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

/// Execution status
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementStatus {
    /// Examples: "PENDING", "RUNNING", "FINISHED", "FAILED", "CANCELED", "CLOSED"
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Manifest with schema & chunk metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementManifest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<ChunkInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,
    // Additional fields, e.g. row_count, chunk_count, etc.
}

/// A single presigned link for EXTERNAL_LINKS mode
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExternalLink {
    /// The actual presigned URL
    #[serde(rename = "external_link")]
    pub external_link: String,
    pub expiration: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,

    pub next_chunk_index: Option<u32>,
}

/// Info about a chunk when inlined or external
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub row_offset: Option<i64>,
    pub row_count: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_count: Option<i64>,
    pub chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_internal_link: Option<String>,
}

/// Result object for `INLINE` or `EXTERNAL_LINKS`
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementResult {
    /// Inline data if `disposition=INLINE`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_array: Option<Vec<Vec<Option<String>>>>,

    /// External links if `disposition=EXTERNAL_LINKS`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,

    /// If there's a next chunk, we get an index to request it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
}

/// A single chunk response for /result/chunks/{chunk_index}
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_array: Option<Vec<Vec<Option<String>>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<u32>,
}



/// Custom error type
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
