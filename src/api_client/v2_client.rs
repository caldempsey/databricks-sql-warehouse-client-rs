use reqwest::Client;
use crate::api::v2_models::{
    ChunkResponse, DatabricksSqlError, StatementRequest, StatementResponse,
};
use crate::api::v2_sql_statements::V2SqlStatements;

/// Low-level Databricks SQL Warehouse client that directly calls the REST endpoints.
#[derive(Debug, Clone)]
pub struct V2Client {
    base_url: String,
    token: String,
    http_client: Client,
}

impl V2Client {
    /// Creates a new client with the given Databricks workspace URL and access token.
    ///
    /// Example base_url: https://<your-workspace>.cloud.databricks.com
    pub fn new(base_url: &str, token: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http_client: Client::new(),
        }
    }

    /// POST /api/2.0/sql/statements
    /// Execute a SQL statement and optionally wait for results.
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

    /// Helper to handle a StatementResponse or error from Databricks.
    async fn handle_response(
        &self,
        resp: reqwest::Response,
    ) -> Result<StatementResponse, DatabricksSqlError> {
        // Capture the status code and headers for debugging
        let status = resp.status();
        let headers = resp.headers().clone();

        // Read the response body as text
        let text_body = resp.text().await?;

        // Debug print: Status code and headers
        eprintln!("Response Status: {}", status);
        eprintln!("Response Headers: {:#?}", headers);

        // Debug print: Full body of the response
        eprintln!("Response Body: {}", text_body);

        // Handle specific HTTP status cases
        if status == reqwest::StatusCode::NOT_FOUND {
            eprintln!("Error: Resource not found (404)");
            return Err(DatabricksSqlError::NotFound);
        }
        if !status.is_success() {
            eprintln!("Error: Non-success status code {}", status);
            return Err(DatabricksSqlError::ApiError(format!(
                "HTTP {}: {}",
                status, text_body
            )));
        }

        // If success, attempt to parse JSON into StatementResponse
        let statement_response = serde_json::from_str(&text_body)
            .map_err(|e| {
                eprintln!("JSON parse error: {}", e);
                DatabricksSqlError::ApiError(format!("JSON parse error: {}", e))
            })?;

        eprintln!("Successfully parsed StatementResponse");
        Ok(statement_response)
    }
}

impl V2SqlStatements for V2Client {
    /// GET /api/2.0/sql/statements/{statement_id}
    /// Poll for the statement's status, plus the first chunk of results if available.
    async fn get_statement(
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
    async fn get_statement_result_chunk(
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
    async fn cancel_statement(
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
                status, body
            )));
        }

        Ok(())
    }

    /// Fetch data from a presigned URL that returns JSON_ARRAY format.
    /// The response is expected to be an array-of-arrays, e.g.:
    /// [
    ///   [ "val1", "val2", null ],
    ///   [ "val3", "val4", null ]
    /// ]
    async fn fetch_json_external_link(
        &self,
        presigned_url: &str,
    ) -> Result<Vec<Vec<Option<String>>>, DatabricksSqlError> {
        // Use a separate Client, so we don't accidentally send the Databricks token
        let resp = Client::new()
            .get(presigned_url)
            .send()
            .await
            .map_err(DatabricksSqlError::from)?;

        // Extract the status and body immediately
        let status = resp.status();
        let body = resp.text().await?; // Consume `resp` here

        // Check the status after consuming the response body
        if !status.is_success() {
            return Err(DatabricksSqlError::ApiError(format!(
                "GET {} failed (HTTP {}): {}",
                presigned_url,
                status.as_str(),
                body
            )));
        }

        // Parse the JSON body
        let data = serde_json::from_str::<Vec<Vec<Option<String>>>>(&body)
            .map_err(|e| DatabricksSqlError::ApiError(format!("JSON parse error: {}", e)))?;


        Ok(data)
    }
}
