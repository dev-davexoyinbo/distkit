//! Lax (buffered) instance-aware distributed counter.
//!
//! Buffers `inc` deltas locally and flushes them in bulk to a backing
//! [`StrictInstanceAwareCounter`] on a configurable interval. Epoch-bumping
//! operations (`set`, `del`, `clear`) flush any pending delta first, then
//! delegate immediately to the strict counter.
//!
//! Reads on an instance always include that instance's own writes: both the
//! buffered delta and any delta that a flush has sent to Redis but not yet
//! settled. A flush that fails returns its deltas to the buffer and the next
//! flush retries them, so a delta is never lost. A flush interrupted by a
//! connection failure can apply twice (the reply is lost, so the retry cannot
//! know the delta landed); flushes favor not losing data.

use std::sync::Arc;
use std::{collections::HashMap, time::Duration, time::Instant};

use dashmap::DashMap;
use redis::aio::ConnectionManager;

use crate::{
    ActivityTracker, CounterComparator, DistkitRedisKey, EPOCH_CHANGE_INTERVAL,
    common::mutex_lock,
    error::DistkitError,
    icounter::{
        InstanceAwareCounterTrait, StrictInstanceAwareCounter, StrictInstanceAwareCounterOptions,
    },
};

const MAX_BATCH_SIZE: usize = 100;

/// The name the per-key state mutex reports when it is poisoned.
const STATE_LOCK: &str = "lax_icounter:state";

// ---------------------------------------------------------------------------
// Per-key in-memory state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SingleStore {
    state: std::sync::Mutex<StoreState>,
}

/// The local state of one key.
///
/// Invariant: every unit this instance has written is in exactly one of
/// `delta` (not sent to Redis yet), `in_flight` (sent, reply not applied
/// yet), or the settled values, and every move between those buckets happens
/// inside one critical section of the `state` mutex. A reader computing
/// `cumulative + delta + in_flight` therefore never sees a unit in zero
/// places (a short read) or in two places (a double count). The mutex is
/// never held across an await.
#[derive(Debug)]
struct StoreState {
    /// When this key's local values last changed from a flush, a settle or a
    /// refresh. Reads treat the local values as fresh while this is younger
    /// than `allowed_lag`, and a refresh snapshot is discarded when it
    /// changed while the snapshot was fetched.
    last_flush: Instant,
    /// This instance's writes that no flush has sent yet.
    delta: i64,
    /// This instance's writes that a flush sent to Redis and whose reply is
    /// not applied yet.
    in_flight: i64,
    /// The last settled cumulative total (all instances) from the strict
    /// counter.
    cumulative: i64,
    /// The last settled count of this instance's slice from the strict
    /// counter.
    instance_count: i64,
}

impl StoreState {
    /// The local estimate: `(cumulative, instance_count)`, each including
    /// this instance's unsent and in-flight writes.
    fn local_estimate(&self) -> (i64, i64) {
        (
            self.cumulative + self.delta + self.in_flight,
            self.instance_count + self.delta + self.in_flight,
        )
    } // end function local_estimate
}

impl SingleStore {
    fn new(cumulative: i64, instance_count: i64) -> Self {
        Self {
            state: std::sync::Mutex::new(StoreState {
                last_flush: Instant::now(),
                delta: 0,
                in_flight: 0,
                cumulative,
                instance_count,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options for constructing a [`LaxInstanceAwareCounter`].
#[derive(Debug, Clone)]
pub struct LaxInstanceAwareCounterOptions {
    /// Redis key prefix used to namespace all counter keys.
    pub prefix: DistkitRedisKey,
    /// Redis connection manager.
    pub connection_manager: ConnectionManager,
    /// Milliseconds without a heartbeat before an instance is considered dead.
    /// Default: 30 000.
    pub dead_instance_threshold_ms: u64,
    /// How often the background flush loop ticks. Default: 20 ms.
    pub flush_interval: Duration,
    /// How old a key's pending delta must be before it is included in a flush.
    /// Default: 20 ms.
    pub allowed_lag: Duration,
    /// How often a background task on the backing strict counter reaps dead
    /// instances and deletes counter records that no instance holds anymore,
    /// such as records written by versions before 0.7.2. `None` (the
    /// default) spawns no task.
    pub cleanup_interval: Option<Duration>,
}

impl LaxInstanceAwareCounterOptions {
    /// Creates options with sensible defaults.
    ///
    /// Defaults: `dead_instance_threshold_ms = 30_000`, `flush_interval = 20 ms`,
    /// `allowed_lag = 20 ms`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, icounter::LaxInstanceAwareCounterOptions};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    /// let opts = LaxInstanceAwareCounterOptions::new(prefix, conn);
    /// assert_eq!(opts.dead_instance_threshold_ms, 30_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(prefix: DistkitRedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            prefix,
            connection_manager,
            dead_instance_threshold_ms: 30_000,
            flush_interval: Duration::from_millis(20),
            allowed_lag: Duration::from_millis(20),
            cleanup_interval: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Counter struct
// ---------------------------------------------------------------------------

/// Buffered instance-aware distributed counter.
///
/// `inc` calls are accumulated locally and flushed in bulk to a backing
/// [`StrictInstanceAwareCounter`] every `flush_interval`. Global
/// epoch-bumping operations (`set`, `del`, `clear`) are forwarded
/// immediately after draining any pending delta.
///
/// Reads on an instance always include that instance's own buffered and
/// in-flight writes, so a flush in progress never makes them disappear.
#[derive(Debug)]
pub struct LaxInstanceAwareCounter {
    strict: Arc<StrictInstanceAwareCounter>,
    local_store: DashMap<DistkitRedisKey, SingleStore>,
    activity: Arc<ActivityTracker>,
    flush_interval: Duration,
    allowed_lag: Duration,
    reset_locks: DashMap<DistkitRedisKey, Arc<tokio::sync::Mutex<()>>>,
    /// Serializes send-to-Redis plus settle-reply sequences, so replies apply
    /// in Redis order. Without it, a `flush_key` from `set` or `del` racing
    /// the background flush on one key could apply an older reply after a
    /// newer flush already settled, and reads would go short until the next
    /// refresh. Read-only refreshes do not take it; they discard a snapshot
    /// that a flush overlapped (see [`Self::apply_refresh`]).
    flush_lock: tokio::sync::Mutex<()>,
}

impl LaxInstanceAwareCounter {
    /// Creates a new lax instance-aware counter and spawns its background flush task.
    ///
    /// `inc` deltas are buffered locally and flushed to the backing
    /// [`StrictInstanceAwareCounter`] every `flush_interval`. The background
    /// task holds a [`Weak`](std::sync::Weak) reference and stops automatically
    /// when the counter is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use distkit::{DistkitRedisKey, icounter::{LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions, InstanceAwareCounterTrait}};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let redis_url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    /// let client = redis::Client::open(redis_url)?;
    /// let conn = client.get_connection_manager().await?;
    /// let prefix = DistkitRedisKey::try_from("my_app".to_string())?;
    /// let counter = LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions::new(prefix, conn));
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(options: LaxInstanceAwareCounterOptions) -> Arc<Self> {
        Self::new_inner(options, EPOCH_CHANGE_INTERVAL)
    }

    /// Test seam: a counter whose activity epochs advance every
    /// `epoch_interval`, so idle-wake behavior fits a test's time budget.
    #[cfg(test)]
    pub(crate) fn new_with_epoch_interval(
        options: LaxInstanceAwareCounterOptions,
        epoch_interval: Duration,
    ) -> Arc<Self> {
        Self::new_inner(options, epoch_interval)
    }

    fn new_inner(options: LaxInstanceAwareCounterOptions, epoch_interval: Duration) -> Arc<Self> {
        let LaxInstanceAwareCounterOptions {
            prefix,
            connection_manager,
            dead_instance_threshold_ms,
            flush_interval,
            allowed_lag,
            cleanup_interval,
        } = options;

        let strict =
            StrictInstanceAwareCounter::new_as_lax_backend(StrictInstanceAwareCounterOptions {
                prefix,
                connection_manager,
                dead_instance_threshold_ms,
                cleanup_interval,
            });

        let counter = Arc::new(Self {
            strict,
            local_store: DashMap::default(),
            activity: ActivityTracker::new(epoch_interval),
            flush_interval,
            allowed_lag,
            reset_locks: DashMap::default(),
            flush_lock: tokio::sync::Mutex::new(()),
        });

        counter.run_flush_task();
        counter
    } // end function new_inner

    // -----------------------------------------------------------------------
    // Background flush task
    // -----------------------------------------------------------------------

    fn run_flush_task(self: &Arc<Self>) {
        let flush_interval = self.flush_interval;
        let mut is_active_watch = self.activity.subscribe();
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            interval.tick().await; // skip first immediate tick

            loop {
                let is_active = {
                    let Some(counter) = weak.upgrade() else { break };
                    counter.activity.get_is_active()
                };

                if !is_active {
                    // Drain every buffered delta before the task parks, so a
                    // write made while the tracker went inactive does not
                    // wait for the next activity epoch. A failed drain does
                    // not park: the loop retries on its normal cadence until
                    // the drain succeeds.
                    let drained = {
                        let Some(counter) = weak.upgrade() else { break };
                        counter.drain().await
                    };

                    match drained {
                        Ok(()) => {
                            if is_active_watch.changed().await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            tracing::error!("lax_icounter:flush_task: drain failed: {err}");
                        }
                    }
                }

                interval.tick().await;

                let Some(counter) = weak.upgrade() else { break };
                if let Err(err) = counter.flush().await {
                    tracing::error!("lax_icounter:flush_task: flush failed: {err}");
                }
            }
        });
    } // end function run_flush_task

    /// Sends every delta that is at least `allowed_lag` old.
    async fn flush(&self) -> Result<(), DistkitError> {
        let guard = self.flush_lock.lock().await;
        let pending = self.collect_deltas(&guard, true);
        self.send_deltas(&guard, pending).await
    } // end function flush

    /// Sends every buffered delta, whatever its age.
    async fn drain(&self) -> Result<(), DistkitError> {
        let guard = self.flush_lock.lock().await;
        let pending = self.collect_deltas(&guard, false);
        self.send_deltas(&guard, pending).await
    } // end function drain

    /// Acquire (or create) the per-key reset lock and return an `Arc` to it.
    fn get_or_create_reset_lock(&self, key: &DistkitRedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.reset_locks.get(key) {
            return Arc::clone(&lock);
        }

        self.reset_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    } // end function get_or_create_reset_lock

    /// Moves each key's buffered delta into its in-flight bucket and returns
    /// the amounts to send. With `only_stale`, a key whose state changed less
    /// than `allowed_lag` ago is left for a later flush.
    fn collect_deltas(
        &self,
        _flush_guard: &tokio::sync::MutexGuard<'_, ()>,
        only_stale: bool,
    ) -> Vec<(DistkitRedisKey, i64)> {
        let now = Instant::now();

        self.local_store
            .iter()
            .filter_map(|store| {
                let mut state = mutex_lock(&store.state, STATE_LOCK).ok()?;

                if only_stale && now.duration_since(state.last_flush) < self.allowed_lag {
                    return None;
                }

                if state.delta == 0 {
                    return None;
                }

                let sent = state.delta;
                state.delta = 0;
                state.in_flight += sent;
                state.last_flush = Instant::now();

                Some((store.key().clone(), sent))
            })
            .collect()
    } // end function collect_deltas

    /// [`Self::collect_deltas`] for the given keys only, whatever their age.
    /// Used before an epoch-bumping operation on those keys.
    fn collect_keys<'k>(
        &self,
        _flush_guard: &tokio::sync::MutexGuard<'_, ()>,
        keys: impl IntoIterator<Item = &'k DistkitRedisKey>,
    ) -> Vec<(DistkitRedisKey, i64)> {
        keys.into_iter()
            .filter_map(|key| {
                let store = self.local_store.get(key)?;
                let mut state = mutex_lock(&store.state, STATE_LOCK).ok()?;

                if state.delta == 0 {
                    return None;
                }

                let sent = state.delta;
                state.delta = 0;
                state.in_flight += sent;
                state.last_flush = Instant::now();

                Some((key.clone(), sent))
            })
            .collect()
    } // end function collect_keys

    /// Sends already-collected deltas to the strict counter in chunks of
    /// [`MAX_BATCH_SIZE`] and settles each chunk's replies as it succeeds.
    /// On an error, every amount that was not confirmed returns to its key's
    /// buffer, so the next flush retries it and reads stay exact throughout.
    /// The caller holds `flush_lock`, so replies apply in Redis order.
    async fn send_deltas(
        &self,
        _flush_guard: &tokio::sync::MutexGuard<'_, ()>,
        mut batch: Vec<(DistkitRedisKey, i64)>,
    ) -> Result<(), DistkitError> {
        while !batch.is_empty() {
            let chunk_len = batch.len().min(MAX_BATCH_SIZE);
            let chunk: Vec<(&DistkitRedisKey, CounterComparator, i64)> = batch[..chunk_len]
                .iter()
                .map(|(key, sent)| (key, CounterComparator::Nil, *sent))
                .collect();

            match self.strict.inc_if_batch(&chunk).await {
                Ok(results) => {
                    for ((key, sent), (result_key, cumulative, instance_count, _, _)) in
                        batch[..chunk_len].iter().zip(results)
                    {
                        debug_assert_eq!(key, result_key);
                        self.settle_key(key, *sent, cumulative, instance_count);
                    }

                    batch.drain(..chunk_len);
                }
                Err(err) => {
                    // This chunk is unconfirmed and the later chunks were
                    // never sent: give all of it back to the buffers.
                    for (key, sent) in &batch {
                        self.restore_key(key, *sent);
                    }

                    return Err(err);
                }
            }
        }

        Ok(())
    } // end function send_deltas

    /// Applies a flush reply: the settled values replace the local ones and
    /// the sent amount leaves the in-flight bucket, in one critical section.
    /// A reply for a key that was deleted meanwhile is dropped; the delete
    /// bumped the epoch, so the reply's values are void.
    fn settle_key(&self, key: &DistkitRedisKey, sent: i64, cumulative: i64, instance_count: i64) {
        let Some(store) = self.local_store.get(key) else {
            return;
        };
        let Ok(mut state) = mutex_lock(&store.state, STATE_LOCK) else {
            return;
        };

        state.cumulative = cumulative;
        state.instance_count = instance_count;
        state.in_flight -= sent;
        state.last_flush = Instant::now();
    } // end function settle_key

    /// Returns an unconfirmed amount from the in-flight bucket to the
    /// buffer, so the next flush sends it again. The amount of a key that
    /// was deleted meanwhile is dropped, as the delete reset the key.
    fn restore_key(&self, key: &DistkitRedisKey, sent: i64) {
        let Some(store) = self.local_store.get(key) else {
            return;
        };
        let Ok(mut state) = mutex_lock(&store.state, STATE_LOCK) else {
            return;
        };

        state.delta += sent;
        state.in_flight -= sent;
    } // end function restore_key

    /// Seeds `local_store` with authoritative values from the strict counter
    /// (an epoch-bumping operation's reply), keeping any buffered delta.
    fn seed_local(&self, key: &DistkitRedisKey, cumulative: i64, instance_count: i64) {
        match self.local_store.get(key) {
            Some(store) => {
                let Ok(mut state) = mutex_lock(&store.state, STATE_LOCK) else {
                    return;
                };

                state.cumulative = cumulative;
                state.instance_count = instance_count;
                state.last_flush = Instant::now();
            }
            None => {
                self.local_store
                    .entry(key.clone())
                    .or_insert_with(|| SingleStore::new(cumulative, instance_count));
            }
        }
    } // end function seed_local

    /// Applies a read-refresh snapshot, unless the key changed while the
    /// snapshot was fetched. `captured` is the key's `last_flush` from
    /// before the fetch (`None`: the key did not exist). The snapshot is
    /// discarded when `last_flush` changed, when the key has an in-flight
    /// amount (the snapshot may already include it, and the settle brings
    /// equally fresh values), or when the key appeared or disappeared
    /// meanwhile. The local values stay exact for this instance's own
    /// writes either way; a later read refreshes again.
    fn apply_refresh(
        &self,
        key: &DistkitRedisKey,
        captured: Option<Instant>,
        cumulative: i64,
        instance_count: i64,
    ) {
        match self.local_store.get(key) {
            Some(store) => {
                let Ok(mut state) = mutex_lock(&store.state, STATE_LOCK) else {
                    return;
                };

                if captured == Some(state.last_flush) && state.in_flight == 0 {
                    state.cumulative = cumulative;
                    state.instance_count = instance_count;
                    state.last_flush = Instant::now();
                }
            }
            None => {
                if captured.is_none() {
                    self.local_store
                        .entry(key.clone())
                        .or_insert_with(|| SingleStore::new(cumulative, instance_count));
                }
            }
        }
    } // end function apply_refresh

    async fn refresh_local_if_needed(&self, key: &DistkitRedisKey) -> Result<(), DistkitError> {
        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let captured: Option<Instant> = match self.local_store.get(key) {
            Some(store) => {
                let state = mutex_lock(&store.state, STATE_LOCK)?;

                if state.last_flush.elapsed() < self.allowed_lag {
                    return Ok(());
                }

                Some(state.last_flush)
            }
            None => None,
        };

        let (cumulative, instance_count) = self.strict.get(key).await?;

        self.apply_refresh(key, captured, cumulative, instance_count);

        Ok(())
    } // end function refresh_local_if_needed

    /// Fetches stale/missing keys from the strict counter in a single batched
    /// round-trip, then updates `local_store` for each key whose state did
    /// not change while the fetch was in the air (see [`Self::apply_refresh`]).
    async fn batch_refresh_stale(&self, keys: &[&DistkitRedisKey]) -> Result<(), DistkitError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut captured: HashMap<&DistkitRedisKey, Option<Instant>> =
            HashMap::with_capacity(keys.len());
        let mut to_fetch: Vec<&DistkitRedisKey> = Vec::new();

        for key in keys {
            if captured.contains_key(*key) {
                continue;
            }

            match self.local_store.get(*key) {
                Some(store) => {
                    let state = mutex_lock(&store.state, STATE_LOCK)?;

                    if state.last_flush.elapsed() >= self.allowed_lag {
                        captured.insert(*key, Some(state.last_flush));
                        to_fetch.push(*key);
                    }
                }
                None => {
                    captured.insert(*key, None);
                    to_fetch.push(*key);
                }
            }
        }

        if to_fetch.is_empty() {
            return Ok(());
        }

        let results = self.strict.get_batch(&to_fetch).await?;

        for (key, cumulative, instance_count) in results {
            let Some(captured_at) = captured.get(key).copied() else {
                continue;
            };

            self.apply_refresh(key, captured_at, cumulative, instance_count);
        }

        Ok(())
    } // end function batch_refresh_stale

    /// Runs `op` on the key's state: directly when the cached entry is
    /// younger than `allowed_lag`, otherwise after one refresh from the
    /// strict counter. `op` runs inside the state's critical section, so a
    /// comparator check and its update are atomic on this instance.
    async fn with_fresh_store<R>(
        &self,
        key: &DistkitRedisKey,
        op: impl FnOnce(&mut StoreState) -> R,
    ) -> Result<R, DistkitError> {
        if let Some(store) = self.local_store.get(key) {
            let mut state = mutex_lock(&store.state, STATE_LOCK)?;

            if state.last_flush.elapsed() < self.allowed_lag {
                return Ok(op(&mut state));
            }
        }

        self.refresh_local_if_needed(key).await?;

        // A key can leave the store between the refresh and here only
        // through `del`, which resets it; it then starts fresh at zero.
        let store = self
            .local_store
            .entry(key.clone())
            .or_insert_with(|| SingleStore::new(0, 0));
        let mut state = mutex_lock(&store.state, STATE_LOCK)?;

        Ok(op(&mut state))
    } // end function with_fresh_store
}

// ---------------------------------------------------------------------------
// InstanceAwareCounterTrait
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl InstanceAwareCounterTrait for LaxInstanceAwareCounter {
    /// Returns this instance's unique identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// assert!(!counter.instance_id().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn instance_id(&self) -> &str {
        self.strict.instance_id()
    }

    /// Buffers `count` locally and returns the updated local estimate without
    /// a Redis round-trip.
    ///
    /// On the first call for a key, the current value is fetched from the
    /// backing [`StrictInstanceAwareCounter`] to seed the local cache.
    /// Subsequent calls accumulate locally and a background flush sends them
    /// after `flush_interval` (default 20 ms). Reads on this instance include
    /// the buffered amount and any amount a flush has in the air, so a write
    /// is never invisible to its own instance. Returns
    /// `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// // All three calls are sub-microsecond; no Redis round-trip until flush.
    /// let (c1, s1) = counter.inc(&key, 1).await?;
    /// let (c2, s2) = counter.inc(&key, 1).await?;
    /// let (c3, s3) = counter.inc(&key, 1).await?;
    /// assert_eq!(c3, 3);
    /// assert_eq!(s3, 3);
    /// # Ok(())
    /// # }
    /// ```
    async fn inc(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        Ok(self.inc_if(key, CounterComparator::Nil, count).await?.0)
    }

    async fn inc_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.activity.signal();

        self.with_fresh_store(key, |state| {
            let current = state.local_estimate();

            if !comparator.matches(current.0) {
                return (current, current);
            }

            state.delta += count;

            ((current.0 + count, current.1 + count), current)
        })
        .await
    }

    /// Decrements the counter locally. Equivalent to `inc(key, -count)`.
    ///
    /// Returns `(cumulative, instance_count)` as a local estimate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 10).await?;
    /// let (cumulative, slice) = counter.dec(&key, 4).await?;
    /// assert_eq!(cumulative, 6);
    /// assert_eq!(slice, 6);
    /// # Ok(())
    /// # }
    /// ```
    async fn dec(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        self.inc(key, -count).await
    }

    /// Flushes any pending delta for `key` to the backing strict counter,
    /// then calls `strict.set`, which bumps the epoch and makes the change
    /// immediately visible to all instances.
    ///
    /// Returns `(cumulative, instance_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 5).await?; // buffered locally
    /// // Pending delta flushed first; then strict.set takes over.
    /// let (cumulative, slice) = counter.set(&key, 100).await?;
    /// assert_eq!(cumulative, 100);
    /// assert_eq!(slice, 100);
    /// # Ok(())
    /// # }
    /// ```
    async fn set(&self, key: &DistkitRedisKey, count: i64) -> Result<(i64, i64), DistkitError> {
        Ok(self.set_if(key, CounterComparator::Nil, count).await?.0)
    }

    async fn set_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        let current = self.get(key).await?;
        if !comparator.matches(current.0) {
            return Ok((current, current));
        }

        self.activity.signal();

        let flush_guard = self.flush_lock.lock().await;
        let pending = self.collect_keys(&flush_guard, std::iter::once(key));
        self.send_deltas(&flush_guard, pending).await?;

        let (cumulative, instance_count) = self.strict.set(key, count).await?;

        self.seed_local(key, cumulative, instance_count);

        Ok(((cumulative, instance_count), current))
    }

    /// Adjusts the local delta so this instance's contribution reaches `count`,
    /// without a Redis round-trip and without bumping the epoch.
    ///
    /// On the first call for a key the current value is fetched from Redis to
    /// seed the cache. Returns `(cumulative, instance_count)` as a local estimate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 10).await?;
    /// server_b.inc(&key, 5).await?;
    /// // Adjusts server_a's local delta to reach 7; no epoch bump.
    /// let (cumulative, slice) = server_a.set_on_instance(&key, 7).await?;
    /// assert_eq!(slice, 7);
    /// // Local estimate only includes server_a's contribution;
    /// // server_b's buffered delta has not been flushed to Redis yet.
    /// assert_eq!(cumulative, 7);
    /// # Ok(())
    /// # }
    /// ```
    async fn set_on_instance(
        &self,
        key: &DistkitRedisKey,
        count: i64,
    ) -> Result<(i64, i64), DistkitError> {
        Ok(self
            .set_on_instance_if(key, CounterComparator::Nil, count)
            .await?
            .0)
    }

    async fn set_on_instance_if(
        &self,
        key: &DistkitRedisKey,
        comparator: CounterComparator,
        count: i64,
    ) -> Result<((i64, i64), (i64, i64)), DistkitError> {
        self.activity.signal();

        self.with_fresh_store(key, |state| {
            let current = state.local_estimate();

            if !comparator.matches(current.1) {
                return (current, current);
            }

            // The instance's contribution is its settled slice plus its
            // unsent and in-flight writes; the delta makes their sum `count`.
            state.delta = count - state.instance_count - state.in_flight;

            (
                (state.cumulative - state.instance_count + count, count),
                current,
            )
        })
        .await
    }

    /// Returns `(cumulative, instance_count)`, each including this instance's
    /// buffered and in-flight writes.
    ///
    /// On the first call for a key the current value is fetched from Redis to
    /// seed the local cache. A later read within `allowed_lag` of the last
    /// flush, settle or refresh returns the local estimate without a Redis
    /// round-trip; an older read refreshes the other instances' part from
    /// Redis first.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// counter.inc(&key, 5).await?;
    /// // Returns local estimate (buffered delta included).
    /// assert_eq!(counter.get(&key).await?, (5, 5));
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        self.with_fresh_store(key, |state| state.local_estimate())
            .await
    }

    /// Flushes the pending delta for `key`, then deletes the key globally and
    /// bumps the epoch. Returns `(old_cumulative, old_instance_count)`.
    ///
    /// After deletion, a subsequent `inc` or `get` starts fresh from `0`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// counter.inc(&key, 5).await?; // buffered
    /// let (old_cumulative, _) = counter.del(&key).await?;
    /// assert_eq!(old_cumulative, 5);
    /// assert_eq!(counter.get(&key).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    async fn del(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let flush_guard = self.flush_lock.lock().await;
        let pending = self.collect_keys(&flush_guard, std::iter::once(key));
        self.send_deltas(&flush_guard, pending).await?;

        let result = self.strict.del(key).await?;
        self.local_store.remove(key);

        Ok(result)
    }

    /// Flushes the pending delta for `key`, then removes only this instance's
    /// contribution without bumping the epoch.
    ///
    /// Returns `(new_cumulative, removed_count)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Flush server_a's pending delta, then remove only its slice.
    /// let (new_cumulative, removed) = server_a.del_on_instance(&key).await?;
    /// assert_eq!(removed, 3);
    /// // Redis cumulative is 0 — server_b's delta is still buffered locally.
    /// assert_eq!(new_cumulative, 0);
    /// // Server_b's local view still shows its own buffered total.
    /// assert_eq!(server_b.get(&key).await?, (7, 7));
    /// # Ok(())
    /// # }
    /// ```
    async fn del_on_instance(&self, key: &DistkitRedisKey) -> Result<(i64, i64), DistkitError> {
        self.activity.signal();

        let flush_guard = self.flush_lock.lock().await;
        let pending = self.collect_keys(&flush_guard, std::iter::once(key));
        self.send_deltas(&flush_guard, pending).await?;

        let result = self.strict.del_on_instance(key).await?;
        self.seed_local(key, result.0, 0);

        Ok(result)
    }

    /// Flushes all pending deltas, then removes all keys and instance state from
    /// Redis. Clears the local store.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let counter = distkit::__doctest_helpers::lax_icounter().await?;
    /// let k1 = DistkitRedisKey::try_from("a".to_string())?;
    /// let k2 = DistkitRedisKey::try_from("b".to_string())?;
    /// counter.inc(&k1, 10).await?;
    /// counter.inc(&k2, 20).await?;
    /// counter.clear().await?;
    /// assert_eq!(counter.get(&k1).await?, (0, 0));
    /// assert_eq!(counter.get(&k2).await?, (0, 0));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear(&self) -> Result<(), DistkitError> {
        self.activity.signal();

        let flush_guard = self.flush_lock.lock().await;
        let pending = self.collect_deltas(&flush_guard, false);
        self.send_deltas(&flush_guard, pending).await?;

        self.strict.clear().await?;
        self.local_store.clear();

        Ok(())
    }

    /// Flushes all pending deltas, then removes only this instance's contributions
    /// from Redis. Clears the local store for this instance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use distkit::{DistkitRedisKey, icounter::InstanceAwareCounterTrait};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let (server_a, server_b) = distkit::__doctest_helpers::two_lax_icounters().await?;
    /// let key = DistkitRedisKey::try_from("connections".to_string())?;
    /// server_a.inc(&key, 3).await?;
    /// server_b.inc(&key, 7).await?;
    /// // Flush + remove only server_a's contributions; server_b's slice survives.
    /// server_a.clear_on_instance().await?;
    /// assert_eq!(server_b.get(&key).await?, (7, 7));
    /// # Ok(())
    /// # }
    /// ```
    async fn clear_on_instance(&self) -> Result<(), DistkitError> {
        self.activity.signal();

        let flush_guard = self.flush_lock.lock().await;
        let pending = self.collect_deltas(&flush_guard, false);
        self.send_deltas(&flush_guard, pending).await?;

        self.strict.clear_on_instance().await?;
        self.local_store.clear();

        Ok(())
    }

    async fn get_all<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        self.batch_refresh_stale(keys).await?;

        keys.iter()
            .map(|key| {
                let estimate = match self.local_store.get(*key) {
                    Some(store) => mutex_lock(&store.state, STATE_LOCK)?.local_estimate(),
                    // The key was deleted while refreshing: it starts at zero.
                    None => (0, 0),
                };

                Ok((*key, estimate.0, estimate.1))
            })
            .collect()
    } // end function get_all

    async fn get_all_on_instance<'k>(
        &self,
        keys: &[&'k DistkitRedisKey],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64)>, DistkitError> {
        self.activity.signal();

        self.batch_refresh_stale(keys).await?;

        Ok(keys
            .iter()
            .map(|key| {
                let slice = self
                    .local_store
                    .get(*key)
                    .and_then(|store| {
                        mutex_lock(&store.state, STATE_LOCK)
                            .ok()
                            .map(|state| state.local_estimate().1)
                    })
                    .unwrap_or(0);

                (*key, slice)
            })
            .collect())
    } // end function get_all_on_instance

    async fn inc_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .inc_all_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new_state, _)| (key, new_state.0, new_state.1))
            .collect())
    }

    async fn inc_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let keys: Vec<&DistkitRedisKey> = updates.iter().map(|(key, _, _)| *key).collect();
        self.batch_refresh_stale(&keys).await?;

        updates
            .iter()
            .map(|(key, comparator, count)| {
                let store = self
                    .local_store
                    .entry((*key).clone())
                    .or_insert_with(|| SingleStore::new(0, 0));
                let mut state = mutex_lock(&store.state, STATE_LOCK)?;
                let current = state.local_estimate();

                if comparator.matches(current.0) {
                    state.delta += *count;

                    Ok((*key, (current.0 + count, current.1 + count), current))
                } else {
                    Ok((*key, current, current))
                }
            })
            .collect()
    }

    async fn set_all<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .set_all_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new_state, _)| (key, new_state.0, new_state.1))
            .collect())
    }

    async fn set_all_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let keys: Vec<&DistkitRedisKey> = updates.iter().map(|(key, _, _)| *key).collect();
        self.batch_refresh_stale(&keys).await?;

        let mut current_map: HashMap<DistkitRedisKey, (i64, i64)> =
            HashMap::with_capacity(updates.len());
        let mut matched_updates: Vec<(&DistkitRedisKey, i64)> = Vec::new();

        for (key, comparator, count) in updates {
            let current = match self.local_store.get(*key) {
                Some(store) => mutex_lock(&store.state, STATE_LOCK)?.local_estimate(),
                None => (0, 0),
            };
            current_map.insert((*key).clone(), current);

            if comparator.matches(current.0) {
                matched_updates.push((*key, *count));
            }
        }

        let mut applied_map: HashMap<DistkitRedisKey, (i64, i64)> =
            HashMap::with_capacity(matched_updates.len());
        if !matched_updates.is_empty() {
            // The matched keys' pending deltas must land before the set,
            // or a later flush would add them on top of the set value.
            let flush_guard = self.flush_lock.lock().await;
            let pending =
                self.collect_keys(&flush_guard, matched_updates.iter().map(|(key, _)| *key));
            self.send_deltas(&flush_guard, pending).await?;

            let batch = self.strict.set_batch(&matched_updates).await?;

            for (key, cumulative, instance_count) in &batch {
                self.seed_local(key, *cumulative, *instance_count);
                applied_map.insert((*key).clone(), (*cumulative, *instance_count));
            }
        }

        Ok(updates
            .iter()
            .map(|(key, _, _)| {
                let (cumulative, instance_count) = applied_map
                    .get(*key)
                    .copied()
                    .or_else(|| current_map.get(*key).copied())
                    .unwrap_or((0, 0));
                let (old_cumulative, old_instance_count) =
                    current_map.get(*key).copied().unwrap_or((0, 0));
                (
                    *key,
                    (cumulative, instance_count),
                    (old_cumulative, old_instance_count),
                )
            })
            .collect())
    } // end function set_all_if

    async fn set_all_on_instance<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, i64, i64)>, DistkitError> {
        let conditional_updates: Vec<(&DistkitRedisKey, CounterComparator, i64)> = updates
            .iter()
            .map(|(key, count)| (*key, CounterComparator::Nil, *count))
            .collect();

        Ok(self
            .set_all_on_instance_if(&conditional_updates)
            .await?
            .into_iter()
            .map(|(key, new_state, _)| (key, new_state.0, new_state.1))
            .collect())
    }

    async fn set_all_on_instance_if<'k>(
        &self,
        updates: &[(&'k DistkitRedisKey, CounterComparator, i64)],
    ) -> Result<Vec<(&'k DistkitRedisKey, (i64, i64), (i64, i64))>, DistkitError> {
        if updates.is_empty() {
            return Ok(vec![]);
        }

        self.activity.signal();

        let keys: Vec<&DistkitRedisKey> = updates.iter().map(|(key, _, _)| *key).collect();
        self.batch_refresh_stale(&keys).await?;

        updates
            .iter()
            .map(|(key, comparator, count)| {
                let store = self
                    .local_store
                    .entry((*key).clone())
                    .or_insert_with(|| SingleStore::new(0, 0));
                let mut state = mutex_lock(&store.state, STATE_LOCK)?;
                let current = state.local_estimate();

                if comparator.matches(current.1) {
                    state.delta = count - state.instance_count - state.in_flight;

                    Ok((
                        *key,
                        (state.cumulative - state.instance_count + count, *count),
                        current,
                    ))
                } else {
                    Ok((*key, current, current))
                }
            })
            .collect()
    } // end function set_all_on_instance_if
}
