use std::time::Duration;

use crate::api::v2_models::{
    DatabricksSqlError, StatementRequest, StatementResponse
};
use crate::api::v2_sql_statements::V2SqlStatements;
use crate::api_client::v2_client::V2Client;
use crate::models::{SqlChunkResult, DatabricksSqlResultPage};

/// Higher-level service layer built on top of `V2Client`.
/// Provides convenience operations like polling, chunk iteration, and external link fetching.
#[derive(Debug, Clone)]
pub struct DatabricksSqlWarehouseService {
    client_v2: V2Client,
}

impl DatabricksSqlWarehouseService {
    pub fn new(client: V2Client) -> Self {
        Self { client_v2: client }
    }

    /// Returns a reference to the low-level client (if you need direct calls).
    pub fn client(&self) -> &V2Client {
        &self.client_v2
    }

    // ------------------------------------------------------------------------
    // Polling / Execution methods
    // ------------------------------------------------------------------------

    /// Polls a statement until it reaches a terminal state (FINISHED, FAILED, CANCELED, etc.).
    /// Returns the final StatementResponse once it’s in a terminal state.
    pub async fn poll_statement_until_done(
        &self,
        statement_id: &str,
        poll_interval: Duration,
        max_polls: usize,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        let mut polls = 0;
        loop {
            let resp = self.client_v2.get_statement(statement_id).await?;
            if let Some(status) = &resp.status {
                match status.state.as_str() {
                    "PENDING" | "RUNNING" => {
                        polls += 1;
                        if polls >= max_polls {
                            return Err(DatabricksSqlError::Other(
                                "Max polls reached before statement finished.".into(),
                            ));
                        }
                        tokio::time::sleep(poll_interval).await;
                    }
                    // Terminal states
                    "SUCCEEDED" | "FINISHED" | "FAILED" | "CANCELED" | "CLOSED" => {
                        return Ok(resp);
                    }
                    s => {
                        return Err(DatabricksSqlError::Other(format!(
                            "Unknown statement state: {}",
                            s
                        )));
                    }
                }
            } else {
                return Err(DatabricksSqlError::Other(
                    "No status in statement response.".to_string(),
                ));
            }
        }
    }

    /// Execute a statement in asynchronous mode (wait_timeout=0s),
    /// then poll until completion or failure.
    pub async fn execute_and_poll(
        &self,
        request: &StatementRequest,
        poll_interval: Duration,
        max_polls: usize,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        // Force asynchronous mode
        let async_req = StatementRequest {
            wait_timeout: Some("0s".to_string()),
            ..request.clone()
        };

        // 1) Kick off
        let execute_resp = self.client_v2.execute_statement(&async_req).await?;
        let statement_id = execute_resp
            .statement_id
            .ok_or_else(|| DatabricksSqlError::Other("No statement ID returned.".to_string()))?;

        // 2) Poll
        self.poll_statement_until_done(&statement_id, poll_interval, max_polls)
            .await
    }

    /// Execute a statement with a custom `byte_limit`, wait_timeout=0s, then poll until done.
    /// Allows the caller to specify how many bytes of data to generate (up to 100GB).
    pub async fn execute_and_poll_with_byte_limit(
        &self,
        statement: &str,
        warehouse_id: &str,
        byte_limit: i64,
        poll_interval: Duration,
        max_polls: usize,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        let req = StatementRequest {
            statement: statement.to_string(),
            warehouse_id: warehouse_id.to_string(),
            wait_timeout: Some("0s".to_string()),
            byte_limit: Some(byte_limit),
            disposition: Some("EXTERNAL_LINKS".to_string()),
            ..Default::default()
        };

        let execute_resp = self.client_v2.execute_statement(&req).await?;
        let statement_id = execute_resp
            .statement_id
            .ok_or_else(|| DatabricksSqlError::Other("No statement ID returned.".to_string()))?;

        // Poll for final
        self.poll_statement_until_done(&statement_id, poll_interval, max_polls)
            .await
    }

    /// Execute a statement synchronously (with a certain wait_timeout).
    /// If the statement finishes within that time, you'll get the results inline.
    /// Otherwise, it continues asynchronously, and we poll.
    pub async fn execute_with_hybrid_poll(
        &self,
        mut request: StatementRequest,
        poll_interval: Duration,
        max_polls: usize,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        // Example: set wait_timeout = "10s" and on_wait_timeout = "CONTINUE"
        // so we get a "hybrid" approach: wait up to 10s, else get statement_id for later polling.
        if request.wait_timeout.is_none() {
            request.wait_timeout = Some("10s".into());
        }
        if request.on_wait_timeout.is_none() {
            request.on_wait_timeout = Some("CONTINUE".into());
        }

        let resp = self.client_v2.execute_statement(&request).await?;

        // If the statement is done or failed, we have everything.
        if let Some(status) = &resp.status {
            match status.state.as_str() {
                "FINISHED" | "FAILED" | "CANCELED" | "CLOSED" => Ok(resp),
                _ => {
                    if let Some(statement_id) = &resp.statement_id {
                        self.poll_statement_until_done(statement_id, poll_interval, max_polls)
                            .await
                    } else {
                        Err(DatabricksSqlError::Other(
                            "No statement ID in hybrid response.".to_string(),
                        ))
                    }
                }
            }
        } else {
            Ok(resp)
        }
    }

    // ------------------------------------------------------------------------
    // Chunk / Pagination methods
    // ------------------------------------------------------------------------

    /// Public method: fetch one "page" (chunk) of data for a statement.
    /// If `chunk_index=None`, fetch chunk 0 (via `get_statement`).
    /// If `chunk_index=Some(n)`, fetch chunk n (via `get_statement_result_chunk`).
    ///
    /// **Behavior**:
    /// - If the chunk has `data_array`, we treat it as INLINE data.
    /// - If the chunk has `external_links`, we treat it as EXTERNAL_LINKS,
    ///   download them, and merge them into `data_array`.
    /// - The returned `SqlResultPage` contains the final rows plus `next_chunk_index`.
    pub async fn get_statement_page(
        &self,
        statement_id: &str,
        chunk_index: Option<u32>,
    ) -> Result<DatabricksSqlResultPage, DatabricksSqlError> {
        let chunk = if let Some(idx) = chunk_index {
            // subsequent chunk
            self._get_chunk_response_subsequent_chunk(statement_id, idx).await?
        } else {
            // first chunk
            self._get_chunk_response_first_chunk(statement_id).await?
        };

        println!("Chunk Data: External Links: {:?}", chunk.external_links);

        // Assuming Databricks guarantees mutual exclusion between inline chunks and external links.
        // If the chunk has inline data and no external links,
        // it is an inline data object with data => continue with `_fetch_page_inline`
        if !chunk.data_array.is_empty() && chunk.external_links.is_empty() {
            println!("Chunk Action: Inline Fetch");
            self._fetch_page_inline(chunk).await
        }
        // Else if the chunk has external_links => `_fetch_page_external`
        else if !chunk.external_links.is_empty() {
            println!("Chunk Action: External Fetch");
            self._fetch_page_external(chunk).await
        } else {
            println!("Chunk Action: Empty Res Fetch");
            Ok(DatabricksSqlResultPage {
                data_array: None,
                external_links: None,
                next_chunk_index: chunk.next_chunk_index,
            })
        }
    }

    /// Fetch **all** external links from each page/chunk, parse them as JSON,
    /// and return a concatenated Vec of rows. The statement must have `disposition=EXTERNAL_LINKS`
    /// and `format=JSON_ARRAY`.
    ///
    /// Internally calls `get_statement_page` repeatedly until no more chunks.
    pub async fn fetch_all_external_links_as_json(
        &self,
        statement_id: &str,
    ) -> Result<Vec<Vec<Option<String>>>, DatabricksSqlError> {
        let mut next_index: Option<u32> = None;
        let mut all_rows = Vec::new();

        // Page through the statement chunks
        loop {
            let page = self.get_statement_page(statement_id, next_index).await?;

            // If there are rows in data_array, accumulate them
            if let Some(rows) = page.data_array {
                all_rows.extend(rows);
            }

            // Move to the next chunk
            if let Some(idx) = page.next_chunk_index {
                next_index = Some(idx);
            } else {
                break;
            }
        }

        Ok(all_rows)
    }

    /// For the **first** chunk (chunk_index=None), we call `get_statement`.
    /// Then unify any `data_array` in `result` plus any `external_links` from both
    /// `manifest.external_links` and `result.external_links`.
    ///
    /// We also handle an edge case: if `next_chunk_index` is missing from the top-level `result`,
    /// but exists in an external link’s `next_chunk_index` field, we use that instead.
    async fn _get_chunk_response_first_chunk(
        &self,
        statement_id: &str,
    ) -> Result<SqlChunkResult, DatabricksSqlError> {
        let stmt_resp = self.client_v2.get_statement(statement_id).await?;

        let data_array = stmt_resp
            .result
            .as_ref()
            .and_then(|r| r.data_array.clone())
            .unwrap_or_default();

        let mut next_idx = stmt_resp
            .result
            .as_ref()
            .and_then(|r| r.next_chunk_index);

        // external links can appear in `manifest` or `result`
        let mut combined = stmt_resp
            .manifest
            .as_ref()
            .and_then(|m| m.external_links.clone())
            .unwrap_or_default();

        if let Some(r_links) = stmt_resp.result.as_ref().and_then(|r| r.external_links.clone()) {
            combined.extend(r_links);
        }

        // EDGE CASE: If top-level `next_chunk_index` is None, but an external link has
        // next_chunk_index, we use the first we find
        if next_idx.is_none() {
            for link in &combined {
                if let Some(n) = link.next_chunk_index {
                    next_idx = Some(n);
                    break;
                }
            }
        }

        Ok(SqlChunkResult {
            data_array,
            external_links: combined,
            next_chunk_index: next_idx,
        })
    }

    /// For a **subsequent** chunk (chunk_index=Some(n)), we call `get_statement_result_chunk`.
    /// Then unify its `data_array` or `external_links`, plus a possible `next_chunk_index`.
    ///
    /// We also handle an edge case: if the chunk response has no top-level `next_chunk_index`,
    /// but an external link has it in its `next_chunk_index` field, we use that instead.
    async fn _get_chunk_response_subsequent_chunk(
        &self,
        statement_id: &str,
        idx: u32,
    ) -> Result<SqlChunkResult, DatabricksSqlError> {
        let chunk_resp = self.client_v2.get_statement_result_chunk(statement_id, idx).await?;
        let data_array = chunk_resp.data_array.unwrap_or_default();
        let external_links = chunk_resp.external_links.unwrap_or_default();

        let mut next_idx = chunk_resp.next_chunk_index;

        // EDGE CASE: If Databricks returned no top-level next_chunk_index, but an external link
        // has it in link.next_chunk_index, we use the first one we find.
        if next_idx.is_none() {
            for link in &external_links {
                if let Some(n) = link.next_chunk_index {
                    next_idx = Some(n);
                    break;
                }
            }
        }

        Ok(SqlChunkResult {
            data_array,
            external_links,
            next_chunk_index: next_idx,
        })
    }

    /// If the chunk has inline data, just wrap it in a `SqlResultPage`.
    async fn _fetch_page_inline(
        &self,
        chunk: SqlChunkResult,
    ) -> Result<DatabricksSqlResultPage, DatabricksSqlError> {
        Ok(DatabricksSqlResultPage {
            data_array: Some(chunk.data_array),
            external_links: None,
            next_chunk_index: chunk.next_chunk_index,
        })
    }

    /// If the chunk has external links, we fetch each link from the data layer
    /// (which does JSON parsing) and merge them into a single array of rows.
    async fn _fetch_page_external(
        &self,
        chunk: SqlChunkResult,
    ) -> Result<DatabricksSqlResultPage, DatabricksSqlError> {
        let mut merged_data = Vec::new();
        let mut next_chunk_index = None;

        // For each external link, call the data-layer method
        println!("Num External Links {:?}", chunk.external_links);
        for link in &chunk.external_links {
            let rows = self
                .client_v2
                .fetch_json_external_link(&link.external_link)
                .await?;
            merged_data.extend(rows);
            next_chunk_index = link.next_chunk_index;
            println!("Chunk Data: Next Index: {:?}", next_chunk_index);
        }

        Ok(DatabricksSqlResultPage {
            data_array: Some(merged_data),
            // These have been consumed, we are ready to consume the next set of external links
            external_links: Option::from(chunk.external_links),
            next_chunk_index,
        })
    }
}

// ------------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    fn build_service_from_env() -> DatabricksSqlWarehouseService {
        let base_url = env::var("DATABRICKS_WORKSPACE_BASE_URL")
            .expect("DATABRICKS_WORKSPACE_BASE_URL not set");
        let token = env::var("DATABRICKS_TOKEN")
            .expect("DATABRICKS_TOKEN not set");
        let client = V2Client::new(&base_url, &token);
        DatabricksSqlWarehouseService::new(client)
    }

    fn get_warehouse_id() -> String {
        env::var("DATABRICKS_SQL_WAREHOUSE_ID")
            .expect("DATABRICKS_SQL_WAREHOUSE_ID not set")
    }

    /// 1) Basic test: poll_statement_until_done
    #[tokio::test]
    async fn test_poll_statement_until_done() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        let request = StatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id,
            wait_timeout: Some("0s".to_string()), // asynchronous
            ..Default::default()
        };

        let resp = service.client().execute_statement(&request).await?;
        let statement_id = resp
            .statement_id
            .ok_or("No statement_id returned!")?;

        let final_resp = service
            .poll_statement_until_done(&statement_id, Duration::from_secs(1), 10)
            .await?;
        println!("Final state = {:?}", final_resp.status.as_ref().map(|s| &s.state));
        Ok(())
    }

    /// 2) Test synchronous + fallback (execute_with_hybrid_poll)
    #[tokio::test]
    async fn test_execute_with_hybrid_poll() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        let request = StatementRequest {
            statement: "SELECT 3".to_string(),
            warehouse_id,
            ..Default::default()
        };

        let final_resp = service
            .execute_with_hybrid_poll(request, Duration::from_secs(1), 10)
            .await?;
        println!(
            "Hybrid Poll => Final State: {:?}",
            final_resp.status.map(|s| s.state)
        );
        Ok(())
    }

    /// 3) Test chunked ingestion with `get_statement_page` in inline mode (small data).
    #[tokio::test]
    async fn test_controlled_get_statement_page_inline() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        let request = StatementRequest {
            statement: "SELECT * FROM range(0,5000,1)".to_string(),
            warehouse_id,
            wait_timeout: Some("0s".to_string()),
            ..Default::default()
        };

        let resp = service.client().execute_statement(&request).await?;
        let statement_id = resp.statement_id.ok_or("No statement ID!")?;

        service
            .poll_statement_until_done(&statement_id, Duration::from_secs(1), 20)
            .await?;

        let mut total_rows = 0;
        let mut next_index: Option<u32> = None;
        loop {
            let page = service.get_statement_page(&statement_id, next_index).await?;
            if let Some(rows) = &page.data_array {
                println!("Fetched {} rows in this chunk (INLINE).", rows.len());
                total_rows += rows.len();
            }
            if let Some(idx) = page.next_chunk_index {
                next_index = Some(idx);
            } else {
                break;
            }
        }

        assert_eq!(
            total_rows, 5000,
            "Expected 5000 rows from range(0,5000,1) but got {}",
            total_rows
        );
        Ok(())
    }

    /// 4) Test fetching all JSON external links, then asserts row counts
    #[tokio::test]
    async fn test_fetch_all_json_external_links_row_check() -> Result<(), Box<dyn std::error::Error>>
    {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        let statement = "SELECT * FROM range(0,200,1)";

        // EXTERNAL_LINKS mode, wait_timeout=0s
        let request = StatementRequest {
            statement: statement.to_string(),
            warehouse_id: warehouse_id.clone(),
            disposition: Some("EXTERNAL_LINKS".to_string()),
            wait_timeout: Some("0s".to_string()),
            ..Default::default()
        };

        // 1) Execute & poll
        let execute_resp = service.client().execute_statement(&request).await?;
        let statement_id = execute_resp.statement_id.ok_or("No statement ID!")?;

        service
            .poll_statement_until_done(&statement_id, Duration::from_secs(1), 30)
            .await?;

        // 2) Download everything
        let all_rows = service.fetch_all_external_links_as_json(&statement_id).await?;
        let row_count = all_rows.len();

        println!("Fetched {} total rows from external links", row_count);
        assert_eq!(row_count, 200, "Expected 200 total rows, got {}", row_count);

        Ok(())
    }

    /// 5) Demonstrates **controlled client-side pagination** of EXTERNAL_LINKS:
    ///    chunk-by-chunk, verifying we handle next_chunk_index from either the chunk or the link.
    #[tokio::test]
    async fn test_controlled_external_link_pagination() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        // We'll generate 5000 rows
        let statement = "SELECT * FROM range(0,50000,1)";

        let request = StatementRequest {
            statement: statement.to_string(),
            warehouse_id: warehouse_id.clone(),
            disposition: Some("EXTERNAL_LINKS".to_string()),
            wait_timeout: Some("0s".to_string()),
            ..Default::default()
        };

        let resp = service.client().execute_statement(&request).await?;
        let statement_id = resp.statement_id.ok_or("No statement ID!")?;

        service
            .poll_statement_until_done(&statement_id, Duration::from_secs(1), 30)
            .await?;

        let mut total_rows = 0;
        let mut next_index: Option<u32> = None;

        loop {
            println!("Fetching chunk_index={:?}", next_index);
            let page = service.get_statement_page(&statement_id, next_index).await?;

            let chunk_size = page.data_array.as_ref().map(|v| v.len()).unwrap_or(0);
            println!(
                "  -> Fetched {} rows in this chunk, next_chunk_index={:?}",
                chunk_size,
                page.next_chunk_index
            );

            total_rows += chunk_size;

            if let Some(idx) = page.next_chunk_index {
                next_index = Some(idx);
            } else {
                break;
            }
        }

        println!("Fetched {} total rows across all pages", total_rows);
        assert_eq!(
            total_rows, 50000,
            "Expected 5000 rows from range(0,5000,1), but got {}",
            total_rows
        );

        Ok(())
    }
}
