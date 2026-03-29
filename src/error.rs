use crate::counter::CounterError;

#[derive(Debug, thiserror::Error, PartialEq, Clone)]
pub enum DistkitError {
    #[error("Invalid Redis key: {0}")]
    InvalidRedisKey(String),
    #[error("Counter Error: {0}")]
    CounterError(#[from] CounterError),
    #[error("Redis Error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Mutex poisoned: {0}")]
    MutexPoisoned(&'static str),
    #[error("Custom Error: {0}")]
    CustomError(String),
}
