use std::time::Duration;

use serde::{Deserialize, Serialize};
use crate::api::v2_sql_statements::V2SqlStatements;

use crate::api_client::v2_client::V2Client;
use crate::models::*;

/// Higher-level service layer built on top of `DatabricksSqlWarehouseClient`.
/// Provides combinatorial / convenience operations like polling until completion.
/// May use a combination of API operations at different versions to achieve results.
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

    /// Polls a statement until it reaches a terminal state:
    /// FINISHED, FAILED, CANCELED, or CLOSED.
    ///
    /// You can specify a poll interval and a maximum number of polls.
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
                        continue;
                    }
                    // Terminal states
                    "SUCCEEDED" | "FINISHED" | "FAILED" | "CANCELED" | "CLOSED" => {
                        return Ok(resp);
                    }
                    _ => {
                        return Err(DatabricksSqlError::Other(format!(
                            "Unknown statement state: {}",
                            status.state
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

        // Step 1: Kick off the statement
        let execute_resp = self.client_v2.execute_statement(&async_req).await?;
        let statement_id = match execute_resp.statement_id {
            Some(id) => id,
            None => {
                return Err(DatabricksSqlError::Other(
                    "No statement ID returned in async execute response.".to_string(),
                ));
            }
        };

        // Step 2: Poll until done
        self.poll_statement_until_done(&statement_id, poll_interval, max_polls)
            .await
    }

    /// Execute a statement synchronously (with a certain wait_timeout).
    /// If the statement finishes within that time, you'll get the results inline.
    /// Otherwise, it continues asynchronously (on_wait_timeout = CONTINUE), and we poll.
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
                // Already finished or failed within that wait_timeout
                "FINISHED" | "FAILED" | "CANCELED" | "CLOSED" => {
                    return Ok(resp);
                }
                // Otherwise, we have a statement ID and must poll
                _ => {
                    if let Some(statement_id) = &resp.statement_id {
                        return self
                            .poll_statement_until_done(statement_id, poll_interval, max_polls)
                            .await;
                    } else {
                        return Err(DatabricksSqlError::Other(
                            "No statement ID returned in hybrid mode.".to_string(),
                        ));
                    }
                }
            }
        }
        Ok(resp)
    }

    /// Fetches one "page" (chunk) of data for a given statement. If `chunk_index` is `None`,
    /// we fetch the first chunk (index=0) by calling `get_statement`. If `chunk_index` is `Some(N)`,
    /// we fetch chunk N using `get_statement_result_chunk`.
    ///
    /// Returns a `SqlResultPage` which includes:
    ///  - The inline data array (if `disposition=INLINE`)
    ///  - Any external links (if `disposition=EXTERNAL_LINKS`)
    ///  - The `next_chunk_index` if there is another chunk to fetch, or `None` if not
    ///
    /// Upstream callers can keep track of `SqlResultPage.next_chunk_index` to decide when to
    /// request the next chunk, controlling ingestion rate or parallel fetches.
    pub async fn get_statement_page(
        &self,
        statement_id: &str,
        chunk_index: Option<u32>,
    ) -> Result<SqlResultPage, DatabricksSqlError> {
        // 1) If no chunk index is given, fetch the statement (chunk=0).
        if let Some(idx) = chunk_index {
            // 2) If chunk index is provided, fetch the chunk directly
            let chunk_resp = self.client_v2.get_statement_result_chunk(statement_id, idx).await?;
            Ok(SqlResultPage {
                data_array: chunk_resp.data_array,
                external_links: chunk_resp.external_links,
                next_chunk_index: chunk_resp.next_chunk_index,
            })
        } else {
            // No chunk index => fetch the statement's first chunk
            let statement_resp = self.client_v2.get_statement(statement_id).await?;
            // Build a SqlResultPage from statement_resp.result or statement_resp.manifest
            let inline_data = statement_resp
                .result
                .as_ref()
                .and_then(|r| r.data_array.clone());
            let next_chunk = statement_resp
                .result
                .as_ref()
                .and_then(|r| r.next_chunk_index);
            let external_links = statement_resp
                .manifest
                .as_ref()
                .and_then(|m| m.external_links.clone());

            Ok(SqlResultPage {
                data_array: inline_data,
                external_links,
                next_chunk_index: next_chunk,
            })
        }
    }
}


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

    /// 1. Test poll_statement_until_done manually
    /// Creates an async statement (small) and then polls until completion.
    #[tokio::test]
    async fn test_poll_statement_until_done() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        // Create a statement with wait_timeout=0s so it returns immediately
        let request = StatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id,
            wait_timeout: Some("0s".to_string()), // asynchronous
            ..Default::default()
        };

        let resp = service.client().execute_statement(&request).await?;
        let statement_id = resp.statement_id
            .ok_or("No statement_id returned!")?;

        // mnually poll until done
        let final_resp = service.poll_statement_until_done(&statement_id, Duration::from_secs(2), 10).await?;
        println!("Final state = {:?}", final_resp.status.as_ref().map(|s| &s.state));
        Ok(())
    }

    /// 2. Test execute_and_poll
    /// This is effectively the same as above, but uses the service method to do it in one call.
    #[tokio::test]
    async fn test_execute_and_poll() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        let request = StatementRequest {
            statement: "SELECT 2".to_string(),
            warehouse_id,
            ..Default::default()
        };

        // By default, `execute_and_poll` sets wait_timeout=0s, then calls poll.
        let final_resp = service
            .execute_and_poll(&request, Duration::from_secs(2), 10)
            .await?;

        println!("Statement ID: {:?}", final_resp.statement_id);
        println!("Final State: {:?}", final_resp.status.map(|s| s.state));
        Ok(())
    }

    /// 3. Test execute_with_hybrid_poll
    /// Attempt synchronous up to 10s, then fallback to poll if needed.
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
            .execute_with_hybrid_poll(request, Duration::from_secs(2), 10)
            .await?;

        println!("Hybrid Poll => Final State: {:?}", final_resp.status.map(|s| s.state));
        Ok(())
    }

    /// 4. Test get_statement_page for chunked ingestion
    /// We'll create a statement that might have multiple chunks, then fetch them in a loop.
    #[tokio::test]
    async fn test_get_statement_page() -> Result<(), Box<dyn std::error::Error>> {
        let service = build_service_from_env();
        let warehouse_id = get_warehouse_id();

        // Possibly make a bigger statement, e.g., range(10000), so it might produce multiple chunks.
        let request = StatementRequest {
            statement: "SELECT * FROM range(0,100,1)".to_string(),
            warehouse_id,
            wait_timeout: Some("0s".to_string()),
            ..Default::default()
        };

        let resp = service.client().execute_statement(&request).await?;
        let statement_id = resp.statement_id
            .ok_or("No statement ID returned!")?;

        service.poll_statement_until_done(&statement_id, Duration::from_secs(2), 20).await?;

        let mut next_index: Option<u32> = None;
        let mut total_rows = 0;

        loop {
            let page = service.get_statement_page(&statement_id, next_index).await?;
            if let Some(rows) = &page.data_array {
                println!("Fetched {} rows in this chunk", rows.len());
                total_rows += rows.len();
            }
            if let Some(links) = &page.external_links {
                println!("External links: {:?}", links);
            }
            if let Some(idx) = page.next_chunk_index {
                println!("Next chunk index: {}", idx);
                next_index = Some(idx);
            } else {
                println!("No more chunks!");
                break;
            }
        }

        println!("Total rows fetched: {}", total_rows);
        Ok(())
    }
}
