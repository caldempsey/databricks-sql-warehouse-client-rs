use reqwest::Client;
use crate::models::*;

/// Low-level Databricks SQL Warehouse client that directly calls the REST endpoints.
#[derive(Debug, Clone)]
pub struct DatabricksSqlWarehouseClient {
    base_url: String,
    token: String,
    http_client: Client,
}

impl DatabricksSqlWarehouseClient {
    /// Creates a new client with the given Databricks workspace URL and access token.
    ///
    /// Example `base_url`: `https://<your-workspace>.cloud.databricks.com`
    pub fn new(base_url: &str, token: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http_client: Client::new(),
        }
    }

    /// POST /api/2.0/sql/statements
    /// Execute a SQL statement and optionally wait for its results.
    pub async fn execute_statement(
        &self,
        request: &StatementRequest,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        let url = format!("{}/api/2.0/sql/statements", self.base_url);

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(&self.token)
            .json(request)
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// GET /api/2.0/sql/statements/{statement_id}
    /// Poll for the statement's status, plus the first chunk of results if available.
    pub async fn get_statement(
        &self,
        statement_id: &str,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        let url = format!("{}/api/2.0/sql/statements/{}", self.base_url, statement_id);

        let resp = self
            .http_client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        self.handle_response(resp).await
    }

    /// GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
    /// Fetch a chunk of results for a completed statement.
    pub async fn get_statement_result_chunk(
        &self,
        statement_id: &str,
        chunk_index: u32,
    ) -> Result<ChunkResponse, DatabricksSqlError> {
        let url = format!(
            "{}/api/2.0/sql/statements/{}/result/chunks/{}",
            self.base_url, statement_id, chunk_index
        );

        let resp = self
            .http_client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        // Capture the status code before consuming the response
        let status = resp.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(DatabricksSqlError::NotFound);
        }
        if !status.is_success() {
            let body = resp.text().await?;
            return Err(DatabricksSqlError::ApiError(format!(
                "HTTP {}: {}",
                status.as_str(),
                body
            )));
        }

        // Deserialize the JSON body into ChunkResponse
        let chunk = resp.json::<ChunkResponse>().await?;
        Ok(chunk)
    }

    /// POST /api/2.0/sql/statements/{statement_id}/cancel
    /// Request that an executing statement be canceled.
    pub async fn cancel_statement(
        &self,
        statement_id: &str,
    ) -> Result<(), DatabricksSqlError> {
        let url = format!(
            "{}/api/2.0/sql/statements/{}/cancel",
            self.base_url, statement_id
        );

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(&self.token)
            .send()
            .await?;

        // Capture the status code before consuming the response
        let status = resp.status();

        if !status.is_success() {
            let body = resp.text().await?;
            return Err(DatabricksSqlError::ApiError(format!(
                "HTTP {}: {}",
                status,
                body
            )));
        }

        // Cancel response is empty. Successful response means the request was accepted.
        Ok(())
    }


    /// Helper to handle a StatementResponse or error from Databricks.
    async fn handle_response(
        &self,
        resp: reqwest::Response,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        let status = resp.status();
        let text_body = resp.text().await?;

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(DatabricksSqlError::NotFound);
        }
        if !status.is_success() {
            return Err(DatabricksSqlError::ApiError(format!(
                "HTTP {}: {}",
                status, text_body
            )));
        }

        // If success, parse the JSON from the text_body
        let statement_response = serde_json::from_str(&text_body)
            .map_err(|e| DatabricksSqlError::ApiError(format!("JSON parse error: {}", e)))?;

        Ok(statement_response)
    }
}
