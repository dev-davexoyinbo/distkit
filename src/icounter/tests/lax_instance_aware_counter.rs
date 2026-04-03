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
) -> (Arc<LaxInstanceAwareCounter>, Arc<LaxInstanceAwareCounter>, String) {
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
