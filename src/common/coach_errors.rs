use anyhow::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoachError {
    #[error("Drill cancelled")]
    Cancelled,
    #[error("Client error - {0:#}")]
    // https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
    Client(Error),
    #[error("Invariant error - {0}")]
    Invariant(String),
}

impl From<anyhow::Error> for CoachError {
    fn from(e: anyhow::Error) -> Self {
        CoachError::Client(e)
    }
}

impl From<qdrant_client::QdrantError> for CoachError {
    fn from(e: qdrant_client::QdrantError) -> Self {
        Self::Client(e.into())
    }
}
