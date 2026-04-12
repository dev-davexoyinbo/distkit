//! Instance-aware counters that track each running instance's contribution separately.
//!
//! This module provides [`StrictInstanceAwareCounter`] (immediate consistency) and
//! [`LaxInstanceAwareCounter`] (eventual consistency with in-memory buffering). Both
//! implement [`InstanceAwareCounterTrait`].

#[cfg(test)]
mod tests;

mod strict_instance_aware_counter;
pub use strict_instance_aware_counter::*;

mod lax_instance_aware_counter;
pub use lax_instance_aware_counter::*;
use uuid::Uuid;

use crate::{CounterComparator, DistkitError, RedisKey};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Async interface for instance-aware distributed counter operations.
///
/// [`StrictInstanceAwareCounter`] implements this trait, allowing generic
/// code to work with any instance-aware counter implementation.
#[async_trait::async_trait]
pub trait InstanceAwareCounterTrait {
    /// Returns this instance's unique identifier.
    ///
    /// Each counter instance is assigned a UUID on construction. The ID is
    /// used to namespace per-instance contributions in Redis and to distinguish
    /// live instances from dead ones during cleanup.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let id = counter.instance_id();
    /// assert!(!id.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn instance_id(&self) -> &str;

    /// Increments the counter for `key` by `count` (stale-aware).
    ///
    /// Returns `(cumulative, instance_count)` where `cumulative` is the sum
    /// across all live instances and `instance_count` is this instance's slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// let (cumulative_a, slice_a) = server_a.inc(&key, 3).await?;
    /// assert_eq!(cumulative_a, 3);
    /// assert_eq!(slice_a, 3);
    /// let (cumulative_b, slice_b) = server_b.inc(&key, 5).await?;
    /// assert_eq!(cumulative_b, 8); // both contributions
    /// assert_eq!(slice_b, 5);      // only server_b's slice
    /// # Ok(())
    /// # }
    /// ```
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Conditionally increments this instance's contribution for `key` by
    /// `count` when the cumulative total satisfies `comparator`.
    ///
    /// Returns `(cumulative, instance_count)` after evaluation. If the
    /// condition fails, the returned values reflect the current state and no
    /// increment is applied.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.set(&key, 10).await?;
    ///
    /// assert_eq!(
    ///     counter.inc_if(&key, CounterComparator::Eq(10), 5).await?,
    ///     (15, 15)
    /// );
    /// assert_eq!(
    ///     counter.inc_if(&key, CounterComparator::Lt(10), 5).await?,
    ///     (15, 15)
    /// );
    /// assert_eq!(
    ///     counter.inc_if(&key, CounterComparator::Nil, 5).await?,
    ///     (20, 20)
    /// );
    /// # Ok(())
    /// # }
    /// ```
    async fn inc_if(
        &self,
        key: &RedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<(i64, i64), DistkitError>;

    /// Decrements the counter for `key` by `count` (stale-aware).
    ///
    /// Equivalent to `inc(key, -count)`. Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 10).await?;
    /// let (cumulative, slice) = counter.dec(&key, 4).await?;
    /// assert_eq!(cumulative, 6);
    /// assert_eq!(slice, 6);
    /// # Ok(())
    /// # }
    /// ```
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Sets the cumulative total for `key` to `count`, bumping the epoch.
    ///
    /// All other instances see their stored count as stale on their next
    /// operation. The calling instance becomes sole owner of the entire count.
    /// Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // Epoch bumps; all previous per-instance contributions are cleared.
    /// let (cumulative, slice) = server_a.set(&key, 100).await?;
    /// assert_eq!(cumulative, 100);
    /// assert_eq!(slice, 100); // server_a now owns the entire count
    /// # Ok(())
    /// # }
    /// ```
    async fn set(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Conditionally sets the cumulative total for `key` to `count` when the
    /// cumulative total satisfies `comparator`.
    ///
    /// Returns `(cumulative, instance_count)` after evaluation. If the
    /// condition fails, the returned values reflect the current state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// counter.set(&key, 10).await?;
    ///
    /// assert_eq!(
    ///     counter.set_if(&key, CounterComparator::Gt(5), 40).await?,
    ///     (40, 40)
    /// );
    /// assert_eq!(
    ///     counter.set_if(&key, CounterComparator::Eq(10), 99).await?,
    ///     (40, 40)
    /// );
    /// assert_eq!(
    ///     counter.set_if(&key, CounterComparator::Nil, 12).await?,
    ///     (12, 12)
    /// );
    /// # Ok(())
    /// # }
    /// ```
    async fn set_if(
        &self,
        key: &RedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<(i64, i64), DistkitError>;

    /// Sets only this instance's contribution for `key` to `count`, without
    /// bumping the epoch.
    ///
    /// Other instances' slices are preserved. Returns `(new_cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // No epoch bump: server_b's slice is not evicted.
    /// let (cumulative, slice) = server_a.set_on_instance(&key, 7).await?;
    /// assert_eq!(slice, 7);
    /// assert_eq!(cumulative, 12); // server_a: 7 + server_b: 5
    /// # Ok(())
    /// # }
    /// ```
    async fn set_on_instance(&self, key: &RedisKey, count: i64)
    -> Result<(i64, i64), DistkitError>;

    /// Conditionally sets this instance's contribution for `key` to `count`
    /// when the current instance slice satisfies `comparator`.
    ///
    /// Returns `(cumulative, instance_count)` after evaluation. If the
    /// condition fails, the returned values reflect the current state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.set_on_instance(&key, 7).await?;
    /// server_b.set_on_instance(&key, 5).await?;
    ///
    /// assert_eq!(
    ///     server_a
    ///         .set_on_instance_if(&key, CounterComparator::Eq(7), 9)
    ///         .await?,
    ///     (14, 9)
    /// );
    /// assert_eq!(
    ///     server_a
    ///         .set_on_instance_if(&key, CounterComparator::Gt(10), 50)
    ///         .await?,
    ///     (14, 9)
    /// );
    /// assert_eq!(
    ///     server_a
    ///         .set_on_instance_if(&key, CounterComparator::Nil, 11)
    ///         .await?,
    ///     (16, 11)
    /// );
    /// # Ok(())
    /// # }
    /// ```
    async fn set_on_instance_if(
        &self,
        key: &RedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<(i64, i64), DistkitError>;

    /// Returns `(cumulative, instance_count)` for `key`.
    ///
    /// A missing key returns `(0, 0)`. Dead-instance cleanup runs as a side
    /// effect.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// // A missing key returns (0, 0).
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// counter.inc(&key, 5).await?;
    /// assert_eq!(counter.get(&key).await?, (5, 5));
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Deletes `key` globally, bumping the epoch to invalidate all instances.
    ///
    /// Returns `(old_cumulative, old_instance_count)`. After deletion, a
    /// subsequent `inc` starts fresh from `0`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// let (old_cumulative, _) = server_a.del(&key).await?;
    /// assert_eq!(old_cumulative, 10);
    /// // After deletion both instances start fresh from 0.
    /// assert_eq!(server_b.inc(&key, 1).await?, (1, 1));
    /// # Ok(())
    /// # }
    /// ```
    async fn del(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Removes only this instance's contribution for `key`, without bumping
    /// the epoch.
    ///
    /// Returns `(new_cumulative, removed_count)`. Other instances are not
    /// affected.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Only server_a's slice is removed; server_b is unaffected.
    /// let (new_cumulative, removed) = server_a.del_on_instance(&key).await?;
    /// assert_eq!(removed, 3);
    /// assert_eq!(new_cumulative, 7); // server_b's slice remains
    /// # Ok(())
    /// # }
    /// ```
    async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Clears all keys and all instance state from Redis.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.inc(&k1, 10).await?;
    /// counter.inc(&k2, 20).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, (0, 0));
    /// assert_eq!(counter.get(&k2).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear(&self) -> Result<(), DistkitError>;

    /// Removes only this instance's contributions for all keys, without
    /// affecting other instances.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_strict_icounters().await?;
    /// let key = RedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Only server_a's contributions are removed; server_b's slice survives.
    /// server_a.clear_on_instance().await?;
    /// assert_eq!(server_b.get(&key).await?, (7, 7));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear_on_instance(&self) -> Result<(), DistkitError>;

    /// Returns `(key, cumulative, instance_count)` for each key in `keys`, in
    /// the same order. A missing key returns `(key, 0, 0)`.
    async fn get_all<'k>(
        &self,
        keys: &[&'k RedisKey],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Returns `(key, instance_count)` for each key in `keys`, in the same
    /// order. Pure-local: no Redis round-trip, no staleness check. A key
    /// with no local contribution returns `(key, 0)`.
    async fn get_all_on_instance<'k>(
        &self,
        keys: &[&'k RedisKey],
    ) -> Result<Vec<(&'k RedisKey, i64)>, DistkitError>;

    /// Increments each `(key, delta)` pair for this instance and returns
    /// `(key, cumulative, instance_count)` in the same order.
    ///
    /// Duplicate keys are processed sequentially in input order, so later
    /// entries observe earlier same-call updates.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    ///
    /// let results = counter.inc_all(&[(&k1, 3), (&k2, 5)]).await?;
    ///
    /// assert_eq!(results, vec![(&k1, 3, 3), (&k2, 5, 5)]);
    /// # Ok(())
    /// # }
    /// ```
    async fn inc_all<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Conditionally increments each `(key, delta)` pair when the cumulative
    /// total satisfies the corresponding comparator.
    ///
    /// Each tuple is `(key, comparator, delta)`. Evaluation is per-item,
    /// results preserve input order, and duplicate keys are processed
    /// sequentially in input order. Use [`CounterComparator::Nil`] for
    /// unconditional entries in a mixed batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.set(&k1, 10).await?;
    ///
    /// let results = counter
    ///     .inc_all_if(&[
    ///         (&k1, CounterComparator::Eq(10), 5),
    ///         (&k2, CounterComparator::Nil, 2),
    ///     ])
    ///     .await?;
    ///
    /// assert_eq!(results, vec![(&k1, 15, 15), (&k2, 2, 2)]);
    /// # Ok(())
    /// # }
    /// ```
    async fn inc_all_if<'k>(
        &self,
        updates: &[(&'k RedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Sets each `(key, count)` pair globally, bumping the epoch. Semantics
    /// match `set` for each individual key. Returns `(key, cumulative, instance_count)`
    /// in the same order.
    async fn set_all<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Conditionally sets each `(key, count)` pair globally when the
    /// cumulative total satisfies the corresponding comparator.
    ///
    /// Each tuple is `(key, comparator, count)`. Evaluation is per-item and
    /// results preserve input order. Use [`CounterComparator::Nil`] for
    /// unconditional entries in a mixed batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    /// counter.set(&k1, 10).await?;
    ///
    /// let results = counter
    ///     .set_all_if(&[
    ///         (&k1, CounterComparator::Eq(10), 15),
    ///         (&k2, CounterComparator::Nil, 20),
    ///     ])
    ///     .await?;
    ///
    /// assert_eq!(results, vec![(&k1, 15, 15), (&k2, 20, 20)]);
    /// # Ok(())
    /// # }
    /// ```
    async fn set_all_if<'k>(
        &self,
        updates: &[(&'k RedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Sets this instance's contribution for each `(key, count)` pair without
    /// bumping the epoch. Other instances' slices are preserved. Returns
    /// `(key, cumulative, instance_count)` in the same order.
    async fn set_all_on_instance<'k>(
        &self,
        updates: &[(&'k RedisKey, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;

    /// Conditionally sets this instance's contribution for each `(key, count)`
    /// pair when the current instance slice satisfies the corresponding
    /// comparator.
    ///
    /// Each tuple is `(key, comparator, count)`. Evaluation is per-item and
    /// results preserve input order. Use [`CounterComparator::Nil`] for
    /// unconditional entries in a mixed batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{CounterComparator, RedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::strict_icounter().await?;
    /// let k1 = RedisKey::try_from("a".to_string())?;
    /// let k2 = RedisKey::try_from("b".to_string())?;
    ///
    /// let results = counter
    ///     .set_all_on_instance_if(&[
    ///         (&k1, CounterComparator::Nil, 5),
    ///         (&k2, CounterComparator::Eq(0), 7),
    ///     ])
    ///     .await?;
    ///
    /// assert_eq!(results, vec![(&k1, 5, 5), (&k2, 7, 7)]);
    /// # Ok(())
    /// # }
    /// ```
    async fn set_all_on_instance_if<'k>(
        &self,
        updates: &[(&'k RedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k RedisKey, i64, i64)>, DistkitError>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn generate_instance_id() -> String {
    Uuid::new_v4().to_string()
}
