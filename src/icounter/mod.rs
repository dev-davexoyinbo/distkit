#[cfg(test)]
mod tests;

mod strict_instance_aware_counter;
pub use strict_instance_aware_counter::*;

mod lax_instance_aware_counter;
pub use lax_instance_aware_counter::{LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions};
use uuid::Uuid;

use crate::{DistkitError, RedisKey};

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
    fn instance_id(&self) -> &str;

    /// Increments the counter for `key` by `count` (stale-aware).
    ///
    /// Returns the new cumulative total.
    async fn inc(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Decrements the counter for `key` by `count` (stale-aware).
    ///
    /// Returns the new cumulative total.
    async fn dec(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Sets the cumulative total for `key` to `count`, bumping the epoch.
    ///
    /// Returns the new cumulative total.
    async fn set(&self, key: &RedisKey, count: i64) -> Result<(i64, i64), DistkitError>;

    /// Sets only this instance's contribution for `key` to `count`, without
    /// bumping the epoch.
    ///
    /// Returns `(new_cumulative, instance_count)`.
    async fn set_on_instance(&self, key: &RedisKey, count: i64)
    -> Result<(i64, i64), DistkitError>;

    /// Returns `(cumulative, instance_count)` for `key`.
    async fn get(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Deletes `key` globally, bumping the epoch to invalidate all instances.
    ///
    /// Returns the cumulative total before deletion.
    async fn del(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Removes only this instance's contribution for `key`, without bumping
    /// the epoch.
    ///
    /// Returns `(new_cumulative, removed_count)`.
    async fn del_on_instance(&self, key: &RedisKey) -> Result<(i64, i64), DistkitError>;

    /// Clears all keys and all instance state from Redis.
    async fn clear(&self) -> Result<(), DistkitError>;

    /// Removes only this instance's contributions for all keys, without
    /// affecting other instances.
    async fn clear_on_instance(&self) -> Result<(), DistkitError>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn generate_instance_id() -> String {
    Uuid::new_v4().to_string()
}
