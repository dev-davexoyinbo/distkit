use crate::{DistkitError, RedisKey};

/// Async interface for distributed counter operations.
///
/// Both [`StrictCounter`](super::StrictCounter) and
/// [`LaxCounter`](super::LaxCounter) implement this trait, so generic code
/// can work with either.
#[async_trait::async_trait]
pub trait CounterTrait {
    /// Increments the counter by `count` and returns the new total.
    ///
    /// A negative `count` decrements. Passing `0` returns the current value
    /// without modification.
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Decrements the counter by `count` and returns the new total.
    ///
    /// Equivalent to `inc(key, -count)`. Counters can go negative.
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Returns the current value of the counter, or `0` if the key does not
    /// exist.
    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError>;

    /// Sets the counter to an exact value, overwriting any previous state.
    /// Returns the value that was set.
    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Deletes the counter and returns the value it held before deletion.
    /// Returns `0` if the key did not exist.
    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError>;

    /// Removes all counters under the current prefix.
    async fn clear(&self) -> Result<(), DistkitError>;
}
