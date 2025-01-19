use serde::{Deserialize, Serialize};

use crate::api::v2_models::ExternalLink;

/// High-level page of data: inline rows + external links + next chunk index
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabricksSqlResultPage {
    pub data_array: Option<Vec<Vec<Option<String>>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,

    pub next_chunk_index: Option<u32>,
}

/// A small struct used internally to unify data_array, external_links, and next_chunk_index.
/// This helps handle the disparate responses captured in https://docs.databricks.com/api/workspace/statementexecution/getstatement
/// Where raw Databricks V2 API requests return the next chunk idx in different placed depending on what you call.
pub struct SqlChunkResult {
    pub(crate) data_array: Vec<Vec<Option<String>>>,
    pub(crate) external_links: Vec<ExternalLink>,
    pub(crate) next_chunk_index: Option<u32>,
}