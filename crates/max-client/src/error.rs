use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("MAX session was not found")]
    SessionNotFound,
    #[error("{0}")]
    Protocol(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
