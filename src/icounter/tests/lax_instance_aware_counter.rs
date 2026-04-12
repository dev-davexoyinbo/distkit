use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::icounter::{
    InstanceAwareCounterTrait, LaxInstanceAwareCounter, LaxInstanceAwareCounterOptions,
};

use super::common::{make_connection, run_id};
use crate::RedisKey;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const FLUSH_MS: u64 = 10;
const THRESHOLD_MS: u64 = 300;

/// Returns a unique prefix string for the given test name.
fn unique_prefix(name: &str) -> String {
    format!("{}_{}", run_id(), name)
}

async fn make_lax_from_prefix(prefix: &str) -> Arc<LaxInstanceAwareCounter> {
    let conn = make_connection().await;
    LaxInstanceAwareCounter::new(LaxInstanceAwareCounterOptions {
        prefix: RedisKey::from(prefix.to_string()),
        connection_manager: conn,
        dead_instance_threshold_ms: THRESHOLD_MS,
        flush_interval: Duration::from_millis(FLUSH_MS),
        allowed_lag: Duration::from_millis(FLUSH_MS),
    })
}

async fn make_lax(name: &str) -> Arc<LaxInstanceAwareCounter> {
    make_lax_from_prefix(&unique_prefix(name)).await
}

/// Two lax counters sharing the same Redis prefix (different instance IDs).
async fn make_lax_pair(
    name: &str,
) -> (
    Arc<LaxInstanceAwareCounter>,
    Arc<LaxInstanceAwareCounter>,
    String,
) {
    let prefix = unique_prefix(name);
    let c1 = make_lax_from_prefix(&prefix).await;
    let c2 = make_lax_from_prefix(&prefix).await;
    (c1, c2, prefix)
}

fn key(name: &str) -> RedisKey {
    RedisKey::from(name.to_string())
}

// ---------------------------------------------------------------------------
// Basic inc / get
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inc_returns_local_estimate() {
    let c = make_lax("inc_returns_local_estimate").await;
    let k = key("hits");

    // First inc seeds from strict counter, subsequent ones are pure-local.
    let (cum1, _) = c.inc(&k, 5).await.unwrap();
    let (cum2, _) = c.inc(&k, 3).await.unwrap();

    assert_eq!(cum1, 5);
    assert_eq!(cum2, 8);
}

#[tokio::test]
async fn get_returns_local_estimate_including_pending_delta() {
    let c = make_lax("get_with_pending").await;
    let k = key("hits");

    c.inc(&k, 10).await.unwrap();
    c.inc(&k, 5).await.unwrap();

    let (cum, _) = c.get(&k).await.unwrap();
    assert_eq!(cum, 15);
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flush_sends_delta_to_strict() {
    let (c1, _c2, prefix) = make_lax_pair("flush_sends_delta").await;
    let k = key("hits");

    c1.inc(&k, 20).await.unwrap();

    // Wait for background flush (several intervals).
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // A fresh counter has no local state — its get() goes straight to strict.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 20);
}

#[tokio::test]
async fn two_instances_cumulate_after_flush() {
    let (c1, c2, prefix) = make_lax_pair("two_instances_cumulate").await;
    let k = key("hits");

    c1.inc(&k, 10).await.unwrap();
    c2.inc(&k, 7).await.unwrap();

    // Wait for both background flushes.
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // A fresh reader has no local cache — it sees the combined strict total.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 17);
}

// ---------------------------------------------------------------------------
// set / del flush pending delta first
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_flushes_pending_delta_then_sets() {
    let (c1, c2, _prefix) = make_lax_pair("set_flushes_delta").await;
    let k = key("hits");

    // c1 accumulates a delta without flushing.
    c1.inc(&k, 5).await.unwrap();

    // set() must flush the pending 5 first, then set the global value to 100.
    c1.set(&k, 100).await.unwrap();

    // c2 has no local state for this key — its get() goes to strict.
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 100);
}

#[tokio::test]
async fn del_flushes_pending_delta_then_deletes() {
    let (c1, c2, _prefix) = make_lax_pair("del_flushes_delta").await;
    let k = key("hits");

    c1.inc(&k, 15).await.unwrap();

    // del() must drain the pending delta then delete globally.
    let (old_cum, _) = c1.del(&k).await.unwrap();

    // The old cumulative returned should be 15 (after the pending delta was flushed).
    assert_eq!(old_cum, 15);

    // c2 has no local state — it goes to strict and sees 0.
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 0);
}

// ---------------------------------------------------------------------------
// Dead-instance cleanup through the lax wrapper
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_instance_cleaned_through_lax_wrapper() {
    let (c1, _c2, prefix) = make_lax_pair("dead_instance_lax").await;
    let k = key("hits");

    // c1 contributes 30, flush so the strict counter has the value.
    c1.inc(&k, 30).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // Let c1 go stale (beyond THRESHOLD_MS).
    sleep(Duration::from_millis(THRESHOLD_MS + 50)).await;

    // A fresh reader triggers dead-instance cleanup inside the strict counter.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 0, "dead instance contribution should be cleaned up");
}

// ---------------------------------------------------------------------------
// clear / clear_on_instance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clear_flushes_and_wipes() {
    let (c1, _c2, prefix) = make_lax_pair("clear_flushes_and_wipes").await;
    let k = key("hits");

    c1.inc(&k, 50).await.unwrap();
    c1.clear().await.unwrap();

    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 0);
}

#[tokio::test]
async fn clear_on_instance_removes_only_this_instance() {
    let (c1, c2, prefix) = make_lax_pair("clear_on_instance_lax").await;
    let k = key("hits");

    c1.inc(&k, 20).await.unwrap();
    c2.inc(&k, 10).await.unwrap();
    // Flush both to the strict counter.
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // Remove only c1's contribution.
    c1.clear_on_instance().await.unwrap();

    // A fresh reader has no local cache — it sees the actual strict state (10).
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 10, "only c1's contribution should be removed");
}

// ---------------------------------------------------------------------------
// dec
// ---------------------------------------------------------------------------

/// dec subtracts from the local estimate without a Redis round-trip.
#[tokio::test]
async fn dec_returns_local_estimate() {
    let c = make_lax("dec_returns_local_estimate").await;
    let k = key("hits");

    c.inc(&k, 10).await.unwrap();
    let (cum, instance) = c.dec(&k, 3).await.unwrap();

    assert_eq!(cum, 7);
    assert_eq!(instance, 7);
}

// ---------------------------------------------------------------------------
// get on unknown key
// ---------------------------------------------------------------------------

/// get on a key that was never written returns (0, 0).
#[tokio::test]
async fn get_on_unknown_key_returns_zero() {
    let c = make_lax("get_unknown_zero").await;
    let k = key("ghost");

    let (cum, instance) = c.get(&k).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(instance, 0);
}

// ---------------------------------------------------------------------------
// del_on_instance
// ---------------------------------------------------------------------------

/// del_on_instance flushes the pending delta then removes only this instance's
/// contribution; other instances' slices are unaffected.
#[tokio::test]
async fn del_on_instance_removes_only_this_instance_contribution() {
    let (c1, c2, prefix) = make_lax_pair("del_on_instance_lax").await;
    let k = key("hits");

    c1.inc(&k, 20).await.unwrap();
    c2.inc(&k, 10).await.unwrap();
    // Flush both so strict counter has c1=20, c2=10, cumulative=30.
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // del_on_instance removes c1's slice (20); c2's contribution (10) survives.
    let (new_cum, removed) = c1.del_on_instance(&k).await.unwrap();
    assert_eq!(removed, 20);
    assert_eq!(new_cum, 10);

    // A fresh reader fetches from strict and sees only c2's slice.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 10);
}

// ---------------------------------------------------------------------------
// set_on_instance
// ---------------------------------------------------------------------------

/// set_on_instance adjusts the local delta so this instance's contribution
/// reaches the target without bumping the epoch.
#[tokio::test]
async fn set_on_instance_adjusts_local_count() {
    let c = make_lax("set_on_instance_adjusts").await;
    let k = key("hits");

    // Accumulate a delta, then override the instance slice via set_on_instance.
    c.inc(&k, 10).await.unwrap();
    let (cum, instance) = c.set_on_instance(&k, 7).await.unwrap();
    assert_eq!(instance, 7);
    assert_eq!(cum, 7);

    // get reflects the overwritten local state.
    let (get_cum, get_instance) = c.get(&k).await.unwrap();
    assert_eq!(get_cum, 7);
    assert_eq!(get_instance, 7);
}

// ---------------------------------------------------------------------------
// clear_on_instance with unflushed delta
// ---------------------------------------------------------------------------

/// clear_on_instance flushes a pending delta before removing this instance's
/// contributions so the flush and delete cancel out, leaving 0 for readers.
#[tokio::test]
async fn clear_on_instance_flushes_pending_delta() {
    let (c1, _c2, prefix) = make_lax_pair("clear_on_instance_pending_delta").await;
    let k = key("hits");

    // Accumulate a delta without waiting for the background flush.
    c1.inc(&k, 50).await.unwrap();

    // clear_on_instance must flush the 50 first, then remove c1's instance slice.
    c1.clear_on_instance().await.unwrap();

    // The flush committed 50 and the delete immediately removed it — net 0.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 0);
}

// ---------------------------------------------------------------------------
// Stale-cumulative divergence — both instances must see the same cumulative
// ---------------------------------------------------------------------------

/// After both instances have fully flushed, every subsequent get() must return
/// the ground-truth cumulative (3). Only the instance counts differ (1 vs 2).
///
/// The early flusher (c1) is stuck with a stale cumulative until get() triggers
/// a re-fetch from strict — this test will FAIL until that re-fetch is
/// implemented in get().
///
/// Timeline:
///   1. c1.inc(k, 1)  — c1 seeds from strict (0,0); delta=1
///   2. c1 flushes    — strict: c1=1, cum=1; c1.local: cum=1, inst=1, δ=0
///   3. c2.inc(k, 1)  — c2 seeds from strict (cum=1, inst_c2=0); delta=1
///   4. c2.inc(k, 1)  — delta=2
///   5. c2 flushes    — strict: c1=1, c2=2, cum=3; c2.local: cum=3, inst=2, δ=0
///   6. c1.get()  → (3, 1) — re-fetched: cache is 100 ms old, allowed_lag=10 ms
///   7. c2.get()  → (3, 2) — fresh from c2's own flush
///   8. fresh.get() → (3, 0) — ground truth from strict
#[tokio::test]
async fn instances_see_different_cumulatives_after_sequential_flushes() {
    let (c1, c2, prefix) = make_lax_pair("stale_cumulative_divergence").await;
    let k = key("hits");

    // Step 1: c1 increments once.
    c1.inc(&k, 1).await.unwrap();

    // Step 2: Wait for c1's background flush to commit to strict.
    // After this: strict has c1=1, cum=1; c1.local: cum=1, inst=1, δ=0.
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // Steps 3 & 4: c2 increments twice, seeding from strict (cum=1, inst_c2=0).
    c2.inc(&k, 1).await.unwrap();
    c2.inc(&k, 1).await.unwrap();

    // Step 5: Wait for c2's background flush to commit to strict.
    // After this: strict has c1=1, c2=2, cum=3; c2.local: cum=3, inst=2, δ=0.
    // c1 is now idle (δ=0); its cache is ~100 ms old — well past allowed_lag (10 ms).
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // Step 6: c1's cache has expired (100 ms >> allowed_lag=10 ms).
    // get() must re-fetch from strict and return the current ground-truth cumulative.
    let (c1_cum, c1_inst) = c1.get(&k).await.unwrap();
    assert_eq!(c1_cum, 3, "c1 must re-fetch from strict — cache is stale");
    assert_eq!(c1_inst, 1, "c1's own instance slice is unchanged");

    // Step 7: c2's cumulative is fresh from its own flush.
    let (c2_cum, c2_inst) = c2.get(&k).await.unwrap();
    assert_eq!(c2_cum, 3, "c2 sees fresh cumulative from its own flush");
    assert_eq!(c2_inst, 2);

    // Step 8: A fresh reader has no local cache — always fetches directly from strict.
    let reader = make_lax_from_prefix(&prefix).await;
    let (reader_cum, reader_inst) = reader.get(&k).await.unwrap();
    assert_eq!(reader_cum, 3, "fresh reader sees ground-truth total");
    assert_eq!(reader_inst, 0, "fresh reader has no instance contribution");
}

/// Mirror of `instances_see_different_cumulatives_after_sequential_flushes`:
/// c2 flushes first this time, so c2 ends up with the stale cache.
///
/// This test will FAIL until get() implements a staleness re-fetch.
///
/// Timeline:
///   1. c2.inc(k, 1) × 2 — c2 seeds from strict (0,0); delta=2
///   2. c2 flushes        — strict: c2=2, cum=2; c2.local: cum=2, inst=2, δ=0
///   3. c1.inc(k, 1)      — c1 seeds from strict (cum=2, inst_c1=0); delta=1
///   4. c1 flushes        — strict: c1=1, c2=2, cum=3; c1.local: cum=3, inst=1, δ=0
///                           c2 is idle (δ=0), cache frozen at cum=2
///   5. c2.get()  → (3, 2) — must re-fetch; cache is ~100 ms old
///   6. c1.get()  → (3, 1) — fresh from c1's own flush
///   7. fresh.get() → (3, 0)
#[tokio::test]
async fn early_flusher_sees_stale_cumulative_from_reversed_flush_order() {
    let (c1, c2, prefix) = make_lax_pair("stale_cumulative_reversed").await;
    let k = key("hits");

    // Steps 1 & 2: c2 increments twice and flushes first.
    c2.inc(&k, 1).await.unwrap();
    c2.inc(&k, 1).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;
    // strict: c2=2, cum=2; c2.local: cum=2, inst=2, δ=0

    // Step 3: c1 increments after c2 has already flushed, seeding from strict (cum=2).
    c1.inc(&k, 1).await.unwrap();

    // Step 4: c1 flushes. strict: c1=1, c2=2, cum=3; c1.local: cum=3, inst=1.
    // c2 is idle (δ=0) — the flush task skips it every tick.
    // Use a longer wait so c1's contribution is definitely visible in Redis
    // before c2.get() triggers a staleness re-fetch.
    sleep(Duration::from_millis(FLUSH_MS * 15)).await;

    // Step 5: c2's cache is ~100 ms old (>>  allowed_lag=10 ms).
    // get() must re-fetch from strict and return the current ground-truth cumulative.
    let (c2_cum, c2_inst) = c2.get(&k).await.unwrap();
    assert_eq!(c2_cum, 3, "c2 must re-fetch from strict — cache is stale");
    assert_eq!(c2_inst, 2, "c2's own instance slice is unchanged");

    // Step 6: c1's cumulative is fresh from its own flush.
    let (c1_cum, c1_inst) = c1.get(&k).await.unwrap();
    assert_eq!(c1_cum, 3, "c1 sees fresh cumulative from its own flush");
    assert_eq!(c1_inst, 1);

    // Step 7: Ground truth.
    let reader = make_lax_from_prefix(&prefix).await;
    let (reader_cum, reader_inst) = reader.get(&k).await.unwrap();
    assert_eq!(reader_cum, 3, "fresh reader sees ground-truth total");
    assert_eq!(reader_inst, 0);
}

// ---------------------------------------------------------------------------
// get_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_all_empty_returns_empty() {
    let c = make_lax("get_all_empty").await;
    assert_eq!(c.get_all(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn get_all_unknown_keys_return_zero_zero() {
    let c = make_lax("get_all_unknown").await;
    let k1 = key("a");
    let k2 = key("b");
    assert_eq!(c.get_all(&[&k1, &k2]).await.unwrap(), vec![(&k1, 0, 0), (&k2, 0, 0)]);
}

#[tokio::test]
async fn get_all_returns_correct_values_after_inc() {
    let c = make_lax("get_all_after_inc").await;
    let k1 = key("a");
    let k2 = key("b");
    c.inc(&k1, 5).await.unwrap();
    c.inc(&k2, 10).await.unwrap();
    let results = c.get_all(&[&k1, &k2]).await.unwrap();
    assert_eq!(results, vec![(&k1, 5, 5), (&k2, 10, 10)]);
}

#[tokio::test]
async fn get_all_preserves_input_order() {
    let c = make_lax("get_all_order").await;
    let k1 = key("a");
    let k2 = key("b");
    let k3 = key("c");
    c.inc(&k1, 1).await.unwrap();
    c.inc(&k2, 2).await.unwrap();
    c.inc(&k3, 3).await.unwrap();
    let results = c.get_all(&[&k3, &k1, &k2]).await.unwrap();
    assert_eq!(results, vec![(&k3, 3, 3), (&k1, 1, 1), (&k2, 2, 2)]);
}

/// A fresh reader with no local cache must batch-fetch from the strict counter
/// and return the written values — not stale zeros.
#[tokio::test]
async fn get_all_fetches_stale_keys_from_redis() {
    let (writer, _, prefix) = make_lax_pair("get_all_stale").await;
    let k = key("hits");

    writer.inc(&k, 42).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    let reader = make_lax_from_prefix(&prefix).await;
    let results = reader.get_all(&[&k]).await.unwrap();
    assert_eq!(results[0].1, 42);
}

// ---------------------------------------------------------------------------
// get_all_on_instance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_all_on_instance_empty_returns_empty() {
    let c = make_lax("goi_empty").await;
    assert_eq!(c.get_all_on_instance(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn get_all_on_instance_unknown_keys_return_zero() {
    let c = make_lax("goi_unknown").await;
    let k1 = key("a");
    let k2 = key("b");
    // No Redis call — purely local; missing keys are 0.
    assert_eq!(c.get_all_on_instance(&[&k1, &k2]).await.unwrap(), vec![(&k1, 0), (&k2, 0)]);
}

#[tokio::test]
async fn get_all_on_instance_returns_instance_count_plus_delta() {
    let c = make_lax("goi_delta").await;
    let k = key("hits");
    c.inc(&k, 5).await.unwrap();
    // instance_count is seeded from strict (0) + delta (5) = 5.
    assert_eq!(c.get_all_on_instance(&[&k]).await.unwrap(), vec![(&k, 5)]);
}

#[tokio::test]
async fn get_all_on_instance_unaffected_by_other_instances() {
    let (c1, c2, _) = make_lax_pair("goi_isolation").await;
    let k = key("hits");

    c2.inc(&k, 100).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // c1 has no local contribution for this key; result must be 0.
    assert_eq!(c1.get_all_on_instance(&[&k]).await.unwrap(), vec![(&k, 0)]);
}

// ---------------------------------------------------------------------------
// set_all_on_instance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_all_on_instance_empty_returns_empty() {
    let c = make_lax("soi_empty").await;
    assert_eq!(c.set_all_on_instance(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn set_all_on_instance_basic_correctness() {
    let c = make_lax("soi_basic").await;
    let k1 = key("a");
    let k2 = key("b");
    let results = c.set_all_on_instance(&[(&k1, 7), (&k2, 3)]).await.unwrap();
    assert_eq!(results, vec![(&k1, 7, 7), (&k2, 3, 3)]);
}

/// Other instances' slices must not be overwritten by set_all_on_instance.
#[tokio::test]
async fn set_all_on_instance_preserves_other_slices() {
    let (c1, c2, prefix) = make_lax_pair("soi_preserves").await;
    let k = key("hits");

    c2.inc(&k, 10).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 10)).await;

    // c1 sets its own slice to 4. c2's slice (10) must survive.
    c1.set_all_on_instance(&[(&k, 4)]).await.unwrap();
    // Long enough for c1 to flush delta=4 but well under the 300 ms dead-instance
    // threshold (c2 last heartbeat ~10 ms ago, c1 last heartbeat ~105 ms ago).
    sleep(Duration::from_millis(FLUSH_MS * 10)).await;

    // A fresh reader sees both contributions.
    let reader = make_lax_from_prefix(&prefix).await;
    let (cum, _) = reader.get(&k).await.unwrap();
    assert_eq!(cum, 14);
}

/// Stale keys are batch-refreshed before computing the delta.
#[tokio::test]
async fn set_all_on_instance_stale_keys_are_batch_refreshed() {
    let (c1, c2, _) = make_lax_pair("soi_stale_refresh").await;
    let k = key("hits");

    c2.inc(&k, 5).await.unwrap();
    sleep(Duration::from_millis(FLUSH_MS * 5)).await;

    // c1 has never touched this key — it is seeded from strict during set_all_on_instance.
    let results = c1.set_all_on_instance(&[(&k, 3)]).await.unwrap();
    // instance_count for c1 = 0 (fresh), cumulative from strict = 5.
    // Expected: (5 - 0 + 3, 3) = (8, 3).
    assert_eq!(results[0].2, 3, "instance slice set correctly");
}

// ---------------------------------------------------------------------------
// set_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_all_empty_returns_empty() {
    let c = make_lax("sa_empty").await;
    assert_eq!(c.set_all(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn set_all_basic_correctness() {
    let c = make_lax("sa_basic").await;
    let k1 = key("a");
    let k2 = key("b");
    let results = c.set_all(&[(&k1, 10), (&k2, 20)]).await.unwrap();
    assert_eq!(results, vec![(&k1, 10, 10), (&k2, 20, 20)]);
}

/// After set_all the new values must be visible to another instance immediately.
#[tokio::test]
async fn set_all_visible_to_other_instances() {
    let (c1, c2, _) = make_lax_pair("sa_visible").await;
    let k = key("hits");

    c1.set_all(&[(&k, 99)]).await.unwrap();

    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 99);
}

/// set_all flushes any pending delta for the affected keys before calling strict.set.
#[tokio::test]
async fn set_all_flushes_pending_delta_first() {
    let (c1, c2, _) = make_lax_pair("sa_flush_first").await;
    let k = key("hits");

    // c1 accumulates delta=5 (not yet flushed).
    c1.inc(&k, 5).await.unwrap();
    // set_all must flush the pending delta before set, then set to 20.
    c1.set_all(&[(&k, 20)]).await.unwrap();

    // c2 must see 20, not 20+5.
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 20);
}
