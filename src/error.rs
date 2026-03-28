use crate::counter::CounterError;

#[derive(Debug, thiserror::Error, PartialEq, Clone)]
pub enum DistkitError {
    #[error("Invalid Redis key: {0}")]
    InvalidRedisKey(String),
    #[error("Counter Error: {0}")]
    CounterError(#[from] CounterError),
}
