use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::client::*;
use crate::models::*;

/// Higher-level service layer built on top of `DatabricksSqlWarehouseClient`.
/// Provides combinatorial / convenience operations like polling until completion.
#[derive(Debug, Clone)]
pub struct DatabricksSqlWarehouseService {
    client: DatabricksSqlWarehouseClient,
}

impl DatabricksSqlWarehouseService {
    pub fn new(client: DatabricksSqlWarehouseClient) -> Self {
        Self { client }
    }

    /// Returns a reference to the low-level client (if you need direct calls).
    pub fn client(&self) -> &DatabricksSqlWarehouseClient {
        &self.client
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
            let resp = self.client.get_statement(statement_id).await?;
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
                        // Unrecognized or new state
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
        // Create a new StatementRequest with the desired wait_timeout value
        let async_req = StatementRequest {
            wait_timeout: Some("0s".to_string()),
            ..request.clone()
        };


        // Step 1: Kick off the statement
        let execute_resp = self.client.execute_statement(&async_req).await?;
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

        let resp = self.client.execute_statement(&request).await?;

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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_execute_example() -> Result<(), Box<dyn std::error::Error>> {
        let client = DatabricksSqlWarehouseClient::new(
            &std::env::var("DATABRICKS_WORKSPACE_BASE_URL")?,
            &std::env::var("DATABRICKS_TOKEN")?,
        );

        let service = DatabricksSqlWarehouseService::new(client.clone());

        let request = StatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id: std::env::var("DATABRICKS_SQL_WAREHOUSE_ID")?,
            ..Default::default()
        };

        // Execute synchronously up to 10s, fallback to poll
        let response = service
            .execute_with_hybrid_poll(request, Duration::from_secs(2), 10)
            .await?;

        println!("Statement ID: {:?}", response.statement_id);
        if let Some(status) = &response.status {
            println!("Final State: {}", status.state);
            if let Some(err) = &status.error {
                println!("Error: {}", err);
            }
        }

        // If disposition=INLINE and the statement FINISHED within 10s,
        // you might see inline data here:
        if let Some(result) = response.result {
            if let Some(rows) = result.data_array {
                println!("Inline rows: {:?}", rows);
            }
        }

        // Alternatively, if disposition=EXTERNAL_LINKS, you might see external links in `manifest.external_links`.
        if let Some(manifest) = &response.manifest {
            if let Some(links) = &manifest.external_links {
                println!("External links: {:?}", links);
            }
        }

        Ok(())
    }
}
