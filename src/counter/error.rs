/// Counter-specific error type.
///
/// Currently empty; reserved for future counter error variants.
#[derive(Debug, thiserror::Error, PartialEq, Clone)]
pub enum CounterError {
    /// Error when attempting to commit state to Redis.
    #[error("Failed to commit state: {0}")]
    CommitToRedisFailed(String),
}
