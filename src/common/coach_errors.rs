use anyhow::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoachError {
    #[error("Drill cancelled")]
    Cancelled,
    #[error("Client error - {0}")]
    Client(Error),
    #[error("Invariant error - {0}")]
    Invariant(String),
}

impl From<anyhow::Error> for CoachError {
    fn from(e: anyhow::Error) -> Self {
        CoachError::Client(e)
    }
}
