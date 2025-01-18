use crate::models::{ChunkResponse, DatabricksSqlError, StatementResponse};

pub trait ApiVersion2 {
    /// GET /api/2.0/sql/statements/{statement_id}
    /// Poll for the statement's status, plus the first chunk of results if available.
    async fn get_statement(
        &self,
        statement_id: &str,
    ) -> Result<StatementResponse, DatabricksSqlError>;

    /// GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
    /// Fetch a chunk of results for a completed statement.
    async fn get_statement_result_chunk(
        &self,
        statement_id: &str,
        chunk_index: u32,
    ) -> Result<ChunkResponse, DatabricksSqlError>;

    /// POST /api/2.0/sql/statements/{statement_id}/cancel
    /// Request that an executing statement be canceled.
    async fn cancel_statement(
        &self,
        statement_id: &str,
    ) -> Result<(), DatabricksSqlError>;
}
