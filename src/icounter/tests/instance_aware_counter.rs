use std::time::Duration;

use super::common::{key, make_counter, make_pair, make_pair_with_opts};

// ---------------------------------------------------------------------------
// Single-instance basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inc_returns_cumulative() {
    let c = make_counter("inc_returns_cumulative").await;
    let k = key("x");

    let v1 = c.inc(&k, 5).await.unwrap();
    assert_eq!(v1, 5);

    let v2 = c.inc(&k, 3).await.unwrap();
    assert_eq!(v2, 8);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn inc_by_zero_is_noop() {
    let c = make_counter("inc_by_zero").await;
    let k = key("x");

    c.inc(&k, 10).await.unwrap();
    let v = c.inc(&k, 0).await.unwrap();
    assert_eq!(v, 10);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn set_returns_count_as_cumulative() {
    let c = make_counter("set_returns_count").await;
    let k = key("x");

    let v = c.set(&k, 99).await.unwrap();
    assert_eq!(v, 99);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn set_on_instance_new_key() {
    let c = make_counter("set_on_instance_new").await;
    let k = key("x");

    let (cum, inst) = c.set_on_instance(&k, 42).await.unwrap();
    assert_eq!(cum, 42);
    assert_eq!(inst, 42);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn get_returns_zero_for_unknown_key() {
    let c = make_counter("get_zero_unknown").await;
    let k = key("never_set");

    let (cum, inst) = c.get(&k).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(inst, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn get_matches_cumulative_and_instance_count() {
    let c = make_counter("get_matches").await;
    let k = key("x");

    c.inc(&k, 7).await.unwrap();
    let (cum, inst) = c.get(&k).await.unwrap();
    assert_eq!(cum, 7);
    assert_eq!(inst, 7);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn del_returns_old_cumulative() {
    let c = make_counter("del_returns_old").await;
    let k = key("x");

    c.inc(&k, 42).await.unwrap();
    let old = c.del(&k).await.unwrap();
    assert_eq!(old, 42);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn del_resets_key() {
    let c = make_counter("del_resets").await;
    let k = key("x");

    c.inc(&k, 10).await.unwrap();
    c.del(&k).await.unwrap();

    let (cum, inst) = c.get(&k).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(inst, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn del_on_instance_removes_contribution() {
    let c = make_counter("del_on_inst").await;
    let k = key("x");

    c.inc(&k, 15).await.unwrap();
    let (new_cum, removed) = c.del_on_instance(&k).await.unwrap();
    assert_eq!(removed, 15);
    assert_eq!(new_cum, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn del_on_instance_on_unknown_key_returns_zeros() {
    let c = make_counter("del_on_inst_unknown").await;
    let k = key("nonexistent");

    let (cum, removed) = c.del_on_instance(&k).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(removed, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn clear_wipes_all_keys() {
    let c = make_counter("clear_wipes").await;
    let k1 = key("a");
    let k2 = key("b");

    c.inc(&k1, 5).await.unwrap();
    c.inc(&k2, 10).await.unwrap();
    c.clear().await.unwrap();

    let (cum1, _) = c.get(&k1).await.unwrap();
    let (cum2, _) = c.get(&k2).await.unwrap();
    assert_eq!(cum1, 0);
    assert_eq!(cum2, 0);
}

#[tokio::test]
async fn clear_on_instance_removes_contribution() {
    let c = make_counter("clear_on_inst").await;
    let k = key("x");

    c.inc(&k, 20).await.unwrap();
    c.clear_on_instance().await.unwrap();

    let (cum, inst) = c.get(&k).await.unwrap();
    assert_eq!(inst, 0);
    assert_eq!(cum, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn independent_keys_do_not_interfere() {
    let c = make_counter("independent_keys").await;
    let k1 = key("a");
    let k2 = key("b");

    c.inc(&k1, 5).await.unwrap();
    c.inc(&k2, 10).await.unwrap();

    let (cum1, _) = c.get(&k1).await.unwrap();
    let (cum2, _) = c.get(&k2).await.unwrap();
    assert_eq!(cum1, 5);
    assert_eq!(cum2, 10);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn prefix_isolation() {
    let c1 = make_counter("prefix_iso_a").await;
    let c2 = make_counter("prefix_iso_b").await;
    let k = key("x");

    c1.inc(&k, 100).await.unwrap();
    let (cum2, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum2, 0);

    c1.clear().await.unwrap();
    c2.clear().await.unwrap();
}

// ---------------------------------------------------------------------------
// Multi-instance cumulative
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_instances_cumulate_via_inc() {
    let (c1, c2) = make_pair("two_inc").await;
    let k = key("x");

    c1.inc(&k, 10).await.unwrap();
    let cum = c2.inc(&k, 20).await.unwrap();
    assert_eq!(cum, 30);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn two_instances_cumulate_via_set_on_instance() {
    let (c1, c2) = make_pair("two_set_on_inst").await;
    let k = key("x");

    let (cum1, _) = c1.set_on_instance(&k, 5).await.unwrap();
    assert_eq!(cum1, 5);

    let (cum2, _) = c2.set_on_instance(&k, 10).await.unwrap();
    assert_eq!(cum2, 15);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn del_on_instance_only_removes_caller_contribution() {
    let (c1, c2) = make_pair("del_on_inst_caller").await;
    let k = key("x");

    c1.inc(&k, 30).await.unwrap();
    c2.inc(&k, 50).await.unwrap();

    let (new_cum, removed) = c1.del_on_instance(&k).await.unwrap();
    assert_eq!(removed, 30);
    assert_eq!(new_cum, 50);

    c1.clear().await.unwrap();
}

// ---------------------------------------------------------------------------
// Stale epoch detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_instance_resets_to_delta_after_epoch_bump() {
    let (c1, c2) = make_pair("stale_epoch").await;
    let k = key("x");

    c1.set_on_instance(&k, 5).await.unwrap(); // c1=5, cumulative=5
    c2.set_on_instance(&k, 10).await.unwrap(); // c2=10, cumulative=15

    // c1 bumps epoch: cumulative=3, c2's stored count becomes stale
    let cum = c1.set(&k, 3).await.unwrap();
    assert_eq!(cum, 3);

    // c2 is stale (local_epoch != redis_epoch).
    // inc(5): stale → new inst_count=5 (not 10+5), cumulative = 3+5 = 8
    let cum = c2.inc(&k, 5).await.unwrap();
    assert_eq!(cum, 8);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn set_increments_epoch_on_each_call() {
    let c = make_counter("epoch_monotonic").await;
    let k = key("x");

    c.set(&k, 10).await.unwrap();
    let v = c.set(&k, 20).await.unwrap();
    assert_eq!(v, 20);

    let (cum, _) = c.get(&k).await.unwrap();
    assert_eq!(cum, 20);

    c.clear().await.unwrap();
}

// ---------------------------------------------------------------------------
// Dead instance cleanup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_instance_counts_removed_from_cumulative() {
    // 100ms threshold so test runs quickly
    let (c1, c2) = make_pair_with_opts("dead_test", 100).await;
    let k = key("hits");

    c1.inc(&k, 40).await.unwrap();
    c2.inc(&k, 60).await.unwrap(); // cumulative = 100

    // c1 stops heartbeating; sleep past threshold
    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2's next op triggers delete_dead_instances: removes c1's 40
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 60);

    c2.clear().await.unwrap();
}
