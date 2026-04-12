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
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let key = RedisKey::try_from("visits".to_string())?;
    /// assert_eq!(counter.inc(&key, 1).await?, 1);
    /// assert_eq!(counter.inc(&key, 9).await?, 10);
    /// // Negative count is the same as calling dec.
    /// assert_eq!(counter.inc(&key, -3).await?, 7);
    /// # Ok(())
    /// # }
    /// ```
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Decrements the counter by `count` and returns the new total.
    ///
    /// Equivalent to `inc(key, -count)`. Counters can go negative.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let key = RedisKey::try_from("tokens".to_string())?;
    /// counter.set(&key, 10).await?;
    /// assert_eq!(counter.dec(&key, 3).await?, 7);
    /// // Counters can go negative.
    /// assert_eq!(counter.dec(&key, 100).await?, -93);
    /// # Ok(())
    /// # }
    /// ```
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Returns the current value of the counter, or `0` if the key does not
    /// exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let key = RedisKey::try_from("visits".to_string())?;
    /// // A key that does not exist returns 0.
    /// assert_eq!(counter.get(&key).await?, 0);
    /// counter.inc(&key, 5).await?;
    /// assert_eq!(counter.get(&key).await?, 5);
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &RedisKey) -> Result<i64, DistkitError>;

    /// Sets the counter to an exact value, overwriting any previous state.
    /// Returns the value that was set.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let key = RedisKey::try_from("inventory".to_string())?;
    /// counter.inc(&key, 1000).await?;
    /// // Overwrite with an authoritative count.
    /// assert_eq!(counter.set(&key, 850).await?, 850);
    /// assert_eq!(counter.get(&key).await?, 850);
    /// # Ok(())
    /// # }
    /// ```
    async fn set(&self, key: &RedisKey, count: i64) -> Result<i64, DistkitError>;

    /// Deletes the counter and returns the value it held before deletion.
    /// Returns `0` if the key did not exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let key = RedisKey::try_from("session".to_string())?;
    /// counter.set(&key, 42).await?;
    /// assert_eq!(counter.del(&key).await?, 42);
    /// // After deletion the key reads back as 0.
    /// assert_eq!(counter.get(&key).await?, 0);
    /// // Deleting a non-existent key returns 0.
    /// assert_eq!(counter.del(&key).await?, 0);
    /// # Ok(())
    /// # }
    /// ```
    async fn del(&self, key: &RedisKey) -> Result<i64, DistkitError>;

    /// Removes all counters under the current prefix.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, counter::CounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_counter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.set(&k1, 10).await?;
    /// counter.set(&k2, 20).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, 0);
    /// assert_eq!(counter.get(&k2).await?, 0);
    /// # Ok(())
    /// # }
    /// ```
    async fn clear(&self) -> Result<(), DistkitError>;

    /// Returns `(key, value)` for each key in `keys`, in the same order.
    /// A missing key returns `(key, 0)`.
    async fn get_all<'k>(&self, keys: &[&'k RedisKey]) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError>;

    /// Sets each `(key, count)` pair and returns `(key, count)` in the same
    /// order. Semantics match `set` for each individual key.
    async fn set_all<'k>(&self, updates: &[(&'k RedisKey, i64)]) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError>;
}
