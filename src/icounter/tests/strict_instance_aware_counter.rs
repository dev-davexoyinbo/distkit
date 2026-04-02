use std::time::Duration;

use super::common::{
    key, make_counter, make_n_counters, make_n_counters_with_opts, make_pair, make_pair_with_opts,
};

// ===========================================================================
// Single-instance basics
// ===========================================================================

#[tokio::test]
async fn inc_returns_cumulative() {
    let c = make_counter("inc_returns_cumulative").await;
    let k = key("x");

    assert_eq!(c.inc(&k, 5).await.unwrap().0, 5);
    assert_eq!(c.inc(&k, 3).await.unwrap().0, 8);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn inc_by_zero_is_noop() {
    let c = make_counter("inc_by_zero").await;
    let k = key("x");

    c.inc(&k, 10).await.unwrap();
    assert_eq!(c.inc(&k, 0).await.unwrap().0, 10);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn set_returns_count_as_cumulative() {
    let c = make_counter("set_returns_count").await;
    let k = key("x");

    assert_eq!(c.set(&k, 99).await.unwrap().0, 99);

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
    let (cum, inst) = c.get(&key("never_set")).await.unwrap();
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
    assert_eq!(c.del(&k).await.unwrap().0, 42);

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
    let (cum, removed) = c.del_on_instance(&key("nonexistent")).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(removed, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn clear_wipes_all_keys() {
    let c = make_counter("clear_wipes").await;
    let (k1, k2) = (key("a"), key("b"));

    c.inc(&k1, 5).await.unwrap();
    c.inc(&k2, 10).await.unwrap();
    c.clear().await.unwrap();

    assert_eq!(c.get(&k1).await.unwrap().0, 0);
    assert_eq!(c.get(&k2).await.unwrap().0, 0);
}

#[tokio::test]
async fn clear_on_instance_removes_contribution() {
    let c = make_counter("clear_on_inst").await;
    let k = key("x");

    c.inc(&k, 20).await.unwrap();
    c.clear_on_instance().await.unwrap();

    let (cum, inst) = c.get(&k).await.unwrap();
    assert_eq!(cum, 0);
    assert_eq!(inst, 0);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn independent_keys_do_not_interfere() {
    let c = make_counter("independent_keys").await;
    let (k1, k2) = (key("a"), key("b"));

    c.inc(&k1, 5).await.unwrap();
    c.inc(&k2, 10).await.unwrap();

    assert_eq!(c.get(&k1).await.unwrap().0, 5);
    assert_eq!(c.get(&k2).await.unwrap().0, 10);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn prefix_isolation() {
    let c1 = make_counter("prefix_iso_a").await;
    let c2 = make_counter("prefix_iso_b").await;
    let k = key("x");

    c1.inc(&k, 100).await.unwrap();
    assert_eq!(c2.get(&k).await.unwrap().0, 0);

    c1.clear().await.unwrap();
    c2.clear().await.unwrap();
}

// ===========================================================================
// A — N-Instance Cumulative Accuracy
// ===========================================================================

#[tokio::test]
async fn three_instances_cumulate_via_inc() {
    let cs = make_n_counters("three_inc", 3).await;
    let k = key("x");

    cs[0].inc(&k, 10).await.unwrap();
    cs[1].inc(&k, 20).await.unwrap();
    let (cum, _) = cs[2].inc(&k, 30).await.unwrap();

    assert_eq!(cum, 60);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn five_instances_set_on_instance_sum() {
    let cs = make_n_counters("five_set_on_inst", 5).await;
    let k = key("x");

    for (i, c) in cs.iter().enumerate() {
        c.set_on_instance(&k, (i + 1) as i64).await.unwrap();
    }

    // cumulative = 1+2+3+4+5 = 15
    let (cum, _) = cs[0].get(&k).await.unwrap();
    assert_eq!(cum, 15);

    // Each instance's own count is correct
    for (i, c) in cs.iter().enumerate() {
        let (_, inst) = c.get(&k).await.unwrap();
        assert_eq!(inst, (i + 1) as i64);
    }

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn ten_instances_each_inc_by_one() {
    let cs = make_n_counters("ten_inc_one", 10).await;
    let k = key("x");

    for c in &cs {
        c.inc(&k, 1).await.unwrap();
    }

    let (cum, _) = cs[0].get(&k).await.unwrap();
    assert_eq!(cum, 10);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn n_instances_del_on_instance_progressively_reaches_zero() {
    let cs = make_n_counters("del_on_inst_progress", 3).await;
    let k = key("x");
    let amounts = [10i64, 20, 30];

    for (c, &amt) in cs.iter().zip(amounts.iter()) {
        c.inc(&k, amt).await.unwrap();
    }

    // cumulative starts at 60; each del_on_instance reduces it by that instance's amount
    let expected_after = [50i64, 30, 0]; // 60-10, 50-20, 30-30
    for (c, &expected) in cs.iter().zip(expected_after.iter()) {
        let (new_cum, _) = c.del_on_instance(&k).await.unwrap();
        assert_eq!(new_cum, expected);
    }

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn n_instances_clear_on_instance_one_at_a_time() {
    let cs = make_n_counters("clear_on_inst_seq", 3).await;
    let k = key("x");
    let amounts = [10i64, 20, 30]; // total = 60

    for (c, &amt) in cs.iter().zip(amounts.iter()) {
        c.inc(&k, amt).await.unwrap();
    }

    // Remove each instance's contribution in turn; cumulative drops by their amount
    let expected_after = [50i64, 30, 0];
    for (c, &expected) in cs.iter().zip(expected_after.iter()) {
        c.clear_on_instance().await.unwrap();
        let (cum, _) = cs[2].get(&k).await.unwrap(); // ask any surviving instance
        assert_eq!(cum, expected);
    }

    cs[0].clear().await.unwrap();
}

// ===========================================================================
// B — Epoch Propagation Across Multiple Instances
// ===========================================================================

#[tokio::test]
async fn set_by_one_instance_makes_all_others_stale() {
    let cs = make_n_counters("set_makes_stale", 3).await;
    let k = key("x");

    cs[0].set_on_instance(&k, 100).await.unwrap(); // c0=100, cum=100
    cs[1].set_on_instance(&k, 100).await.unwrap(); // c1=100, cum=200
    cs[2].set_on_instance(&k, 100).await.unwrap(); // c2=100, cum=300

    // c0 globally sets; epoch bumps; cumulative = 50; c1 and c2 are now stale
    assert_eq!(cs[0].set(&k, 50).await.unwrap().0, 50);

    // c1 is stale → inst resets to 5, cumulative = 50+5 = 55
    assert_eq!(cs[1].inc(&k, 5).await.unwrap().0, 55);

    // c2 is stale → inst resets to 5, cumulative = 55+5 = 60
    assert_eq!(cs[2].inc(&k, 5).await.unwrap().0, 60);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn del_by_one_instance_makes_all_others_stale() {
    let cs = make_n_counters("del_makes_stale", 3).await;
    let k = key("x");

    cs[0].inc(&k, 100).await.unwrap();
    cs[1].inc(&k, 100).await.unwrap();
    cs[2].inc(&k, 100).await.unwrap(); // cumulative = 300

    // c0 deletes key globally; epoch bumps; cumulative = 0
    cs[0].del(&k).await.unwrap();

    // c1 and c2 are stale → each reset inst to 10; cumulative = 0+10+10 = 20
    cs[1].inc(&k, 10).await.unwrap();
    let (cum, _) = cs[2].inc(&k, 10).await.unwrap();
    assert_eq!(cum, 20);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn sequential_epoch_bumps_from_different_instances() {
    let cs = make_n_counters("seq_epoch_bumps", 3).await;
    let k = key("x");

    assert_eq!(cs[0].set(&k, 10).await.unwrap().0, 10);
    assert_eq!(cs[1].set(&k, 20).await.unwrap().0, 20);
    assert_eq!(cs[2].set(&k, 30).await.unwrap().0, 30);

    // Final cumulative is whatever the last set established
    let (cum, _) = cs[0].get(&k).await.unwrap();
    assert_eq!(cum, 30);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn instance_missed_multiple_epochs_still_resyncs() {
    let (c1, c2) = make_pair("missed_epochs").await;
    let k = key("x");

    c2.inc(&k, 50).await.unwrap(); // c2 active at epoch 0

    // c1 bumps epoch 3 times; c2 misses all of them
    c1.set(&k, 10).await.unwrap(); // epoch 1
    c1.set(&k, 20).await.unwrap(); // epoch 2
    c1.set(&k, 30).await.unwrap(); // epoch 3, cumulative = 30

    // c2's local_epoch=0, redis_epoch=3 → stale regardless of gap
    // c2 inc(5): stale → inst resets to 5, cumulative = 30+5 = 35
    let (cum, _) = c2.inc(&k, 5).await.unwrap();
    assert_eq!(cum, 35);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn brand_new_instance_after_epoch_bump_starts_fresh() {
    let cs = make_n_counters("new_inst_after_bump", 2).await;
    let k = key("x");

    // c0 sets; epoch = 1; cumulative = 100
    assert_eq!(cs[0].set(&k, 100).await.unwrap().0, 100);

    // c1 never operated (local_epoch=0); calls set_on_instance(50)
    // → stale (0≠1) → effective_old=0, delta=50 → cumulative = 100+50 = 150
    let (cum, inst) = cs[1].set_on_instance(&k, 50).await.unwrap();
    assert_eq!(inst, 50);
    assert_eq!(cum, 150);

    cs[0].clear().await.unwrap();
}

// ===========================================================================
// C — Complex Interleaved Operations
// ===========================================================================

#[tokio::test]
async fn interleaved_inc_and_set_across_three_instances() {
    let cs = make_n_counters("interleaved", 3).await;
    let k = key("x");

    cs[0].inc(&k, 10).await.unwrap(); // c0=10, cum=10
    cs[1].inc(&k, 5).await.unwrap(); // c1=5,  cum=15

    // c0 globally sets to 20; epoch bumps; cumulative=20; c1 is now stale
    assert_eq!(cs[0].set(&k, 20).await.unwrap().0, 20);

    // c1 stale → inst resets to 3; cumulative = 20+3 = 23
    assert_eq!(cs[1].inc(&k, 3).await.unwrap().0, 23);

    // c2 never op'd (local_epoch=0); redis_epoch=1 → stale → inst resets to 7; cumulative = 23+7 = 30
    assert_eq!(cs[2].inc(&k, 7).await.unwrap().0, 30);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn all_five_instances_del_on_instance_reaches_zero() {
    let cs = make_n_counters("all_del_on_inst", 5).await;
    let k = key("x");
    let amounts = [5i64, 10, 15, 20, 25];

    for (c, &amt) in cs.iter().zip(amounts.iter()) {
        c.inc(&k, amt).await.unwrap();
    }

    // Each calls del_on_instance; cumulative decreases by each amount
    for c in &cs {
        c.del_on_instance(&k).await.unwrap();
    }

    let (cum, _) = cs[0].get(&k).await.unwrap();
    assert_eq!(cum, 0);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn set_then_all_stale_instances_inc() {
    let cs = make_n_counters("set_then_stale_inc", 3).await;
    let k = key("x");

    // c1 and c2 each contribute first (will become stale)
    cs[1].set_on_instance(&k, 200).await.unwrap();
    cs[2].set_on_instance(&k, 300).await.unwrap();

    // c0 globally sets to 100; epoch bumps; c1 and c2 are now stale
    assert_eq!(cs[0].set(&k, 100).await.unwrap().0, 100);

    // c1 stale → inst resets to 10; cumulative = 100+10 = 110
    cs[1].inc(&k, 10).await.unwrap();
    // c2 stale → inst resets to 10; cumulative = 110+10 = 120
    let (cum, _) = cs[2].inc(&k, 10).await.unwrap();
    assert_eq!(cum, 120);

    // Verify each instance's stored count
    let (_, inst1) = cs[1].get(&k).await.unwrap();
    let (_, inst2) = cs[2].get(&k).await.unwrap();
    assert_eq!(inst1, 10);
    assert_eq!(inst2, 10);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn del_then_fresh_inc_from_all() {
    let cs = make_n_counters("del_fresh_inc_all", 3).await;
    let k = key("x");

    cs[0].inc(&k, 50).await.unwrap(); // c0=50, cum=50
    cs[1].inc(&k, 30).await.unwrap(); // c1=30, cum=80

    // c2 deletes globally; epoch bumps; cumulative=0; c0 and c1 are stale; c2 is fresh
    cs[2].del(&k).await.unwrap();

    // c0 stale → inst resets to 5; cum = 0+5 = 5
    cs[0].inc(&k, 5).await.unwrap();
    // c1 stale → inst resets to 7; cum = 5+7 = 12
    cs[1].inc(&k, 7).await.unwrap();
    // c2 fresh (did del, local_epoch = new_epoch) → HINCRBY; cum = 12+3 = 15
    let (cum, _) = cs[2].inc(&k, 3).await.unwrap();
    assert_eq!(cum, 15);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn round_robin_inc_across_five_instances() {
    let cs = make_n_counters("round_robin", 5).await;
    let k = key("x");

    // Each instance incs 1, round-robin, 10 rounds = 50 total increments
    for _ in 0..10 {
        for c in &cs {
            c.inc(&k, 1).await.unwrap();
        }
    }

    let (cum, _) = cs[0].get(&k).await.unwrap();
    assert_eq!(cum, 50);

    // Each instance contributed exactly 10
    for c in &cs {
        let (_, inst) = c.get(&k).await.unwrap();
        assert_eq!(inst, 10);
    }

    cs[0].clear().await.unwrap();
}

// ===========================================================================
// D — Dead Instance Scenarios
// ===========================================================================

#[tokio::test]
async fn multiple_dead_instances_all_cleaned_up() {
    // 5 instances; 4 die; 1 survives
    let mut cs = make_n_counters_with_opts("multi_dead", 5, 100).await;
    let k = key("x");

    for c in &cs {
        c.inc(&k, 10).await.unwrap(); // each contributes 10 → total = 50
    }

    // Drop the first 4 (stop heartbeating)
    let survivor = cs.pop().unwrap();
    drop(cs);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Survivor's next op triggers dead-instance cleanup; removes the 4×10
    let (cum, inst) = survivor.get(&k).await.unwrap();
    assert_eq!(inst, 10);
    assert_eq!(cum, 10);

    survivor.clear().await.unwrap();
}

#[tokio::test]
async fn dead_instance_cleaned_then_new_instance_joins() {
    let (c1, c2) = make_pair_with_opts("dead_then_new", 100).await;
    let k = key("x");

    c1.inc(&k, 40).await.unwrap(); // c1=40
    c2.inc(&k, 60).await.unwrap(); // c2=60, cum=100

    drop(c1); // c1 stops heartbeating

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 triggers cleanup: removes c1's 40; cumulative = 60
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 60);

    // A new instance joins the same prefix and contributes
    let mut opts = super::common::make_n_counters_with_opts("dead_then_new", 1, 100).await;
    let c3 = opts.pop().unwrap();
    let (cum, _) = c3.inc(&k, 20).await.unwrap();
    assert_eq!(cum, 80);

    c2.clear().await.unwrap();
}

#[tokio::test]
async fn dead_instance_with_multiple_keys_fully_cleaned() {
    let (c1, c2) = make_pair_with_opts("dead_multi_key", 100).await;
    let (k1, k2, k3) = (key("a"), key("b"), key("c"));

    // c1 contributes to all 3 keys
    c1.inc(&k1, 10).await.unwrap();
    c1.inc(&k2, 20).await.unwrap();
    c1.inc(&k3, 30).await.unwrap();

    // c2 contributes to none; just registers itself
    c2.get(&k1).await.unwrap();

    drop(c1); // c1 dies

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2's next ops clean up c1 from all 3 keys
    assert_eq!(c2.get(&k1).await.unwrap().0, 0);
    assert_eq!(c2.get(&k2).await.unwrap().0, 0);
    assert_eq!(c2.get(&k3).await.unwrap().0, 0);

    c2.clear().await.unwrap();
}

#[tokio::test]
async fn live_instance_not_cleaned_at_boundary() {
    // Both instances operate within the threshold; neither should be cleaned up
    let (c1, c2) = make_pair_with_opts("live_boundary", 500).await;
    let k = key("x");

    c1.inc(&k, 40).await.unwrap();
    c2.inc(&k, 60).await.unwrap(); // cumulative = 100

    // Sleep well within the 500ms threshold
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Both instances are still alive; cumulative should remain 100
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 100);

    c1.clear().await.unwrap();
}

// ===========================================================================
// E — Multi-Key, Multi-Instance
// ===========================================================================

#[tokio::test]
async fn three_instances_two_keys_independent() {
    let cs = make_n_counters("three_two_keys", 3).await;
    let (ka, kb) = (key("a"), key("b"));

    cs[0].inc(&ka, 1).await.unwrap();
    cs[1].inc(&ka, 2).await.unwrap();
    cs[2].inc(&ka, 3).await.unwrap();

    cs[0].inc(&kb, 10).await.unwrap();
    cs[1].inc(&kb, 20).await.unwrap();
    cs[2].inc(&kb, 30).await.unwrap();

    let (cum_a, _) = cs[0].get(&ka).await.unwrap();
    let (cum_b, _) = cs[0].get(&kb).await.unwrap();

    assert_eq!(cum_a, 6);
    assert_eq!(cum_b, 60);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn epoch_bump_on_one_key_does_not_affect_other() {
    let cs = make_n_counters("epoch_key_isolation", 3).await;
    let (k1, k2) = (key("k1"), key("k2"));

    // All 3 instances contribute to both keys
    for c in &cs {
        c.inc(&k1, 10).await.unwrap();
        c.inc(&k2, 10).await.unwrap();
    }
    // cumulative k1 = 30, k2 = 30

    // c0 bumps epoch on k1 by setting it to 5
    cs[0].set(&k1, 5).await.unwrap();
    // k1 epoch = 1; k2 epoch still = 0

    // c1 and c2 op on k2 — they should not be stale for k2
    // (local_epoch for k2 = 0 = redis_epoch for k2)
    let (cum_k2_after_c1, _) = cs[1].inc(&k2, 1).await.unwrap();
    let (cum_k2_after_c2, _) = cs[2].inc(&k2, 1).await.unwrap();

    // k2 was never epoch-bumped; increments are fresh; cumulative = 30+1+1 = 32
    assert_eq!(cum_k2_after_c1, 31);
    assert_eq!(cum_k2_after_c2, 32);

    // k1: c1 is stale → reset inst to 7; cumulative = 5+7 = 12
    let (cum_k1, _) = cs[1].inc(&k1, 7).await.unwrap();
    assert_eq!(cum_k1, 12);

    cs[0].clear().await.unwrap();
}

#[tokio::test]
async fn clear_on_instance_removes_all_keys_for_that_instance() {
    let (c1, c2) = make_pair("clear_on_inst_multi_key").await;
    let (ka, kb, kc) = (key("a"), key("b"), key("c"));

    // c1 contributes to 3 keys
    c1.inc(&ka, 10).await.unwrap();
    c1.inc(&kb, 20).await.unwrap();
    c1.inc(&kc, 30).await.unwrap();

    // c2 contributes a flat 5 to each
    c2.inc(&ka, 5).await.unwrap();
    c2.inc(&kb, 5).await.unwrap();
    c2.inc(&kc, 5).await.unwrap();

    // c1 clears its own contributions
    c1.clear_on_instance().await.unwrap();

    // c1's 10/20/30 removed; only c2's 5 remains per key
    assert_eq!(c2.get(&ka).await.unwrap().0, 5);
    assert_eq!(c2.get(&kb).await.unwrap().0, 5);
    assert_eq!(c2.get(&kc).await.unwrap().0, 5);

    c1.clear().await.unwrap();
}

// ===========================================================================
// F — Stale Edge Cases
// ===========================================================================

#[tokio::test]
async fn stale_del_on_instance_does_not_double_subtract() {
    let (c1, c2) = make_pair("stale_del_on_inst").await;
    let k = key("x");

    c1.inc(&k, 40).await.unwrap(); // c1=40, cum=40
    c2.inc(&k, 60).await.unwrap(); // c2=60, cum=100

    // c1 globally sets to 10; epoch bumps; cumulative=10; c2 is stale (its 60 not in new epoch)
    c1.set(&k, 10).await.unwrap();

    // c2 is stale; del_on_instance should NOT subtract 60 from cumulative (it isn't there)
    let (new_cum, _removed) = c2.del_on_instance(&k).await.unwrap();
    assert_eq!(new_cum, 10); // cumulative stays at 10

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn set_on_instance_from_stale_uses_full_count_as_delta() {
    let (c1, c2) = make_pair("stale_set_on_inst_delta").await;
    let k = key("x");

    // c2 contributes 50 at epoch 0
    c2.set_on_instance(&k, 50).await.unwrap(); // cum=50

    // c1 bumps epoch by setting to 100; cumulative=100; c2 is now stale
    c1.set(&k, 100).await.unwrap();

    // c2 calls set_on_instance(30); stale → effective_old=0, delta=30; cumulative = 100+30 = 130
    let (cum, inst) = c2.set_on_instance(&k, 30).await.unwrap();
    assert_eq!(inst, 30);
    assert_eq!(cum, 130);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn inc_stale_ignores_old_instance_count_in_cumulative() {
    let (c1, c2) = make_pair("stale_inc_ignore_old").await;
    let k = key("x");

    // c2 contributes a large amount at epoch 0
    c2.set_on_instance(&k, 100).await.unwrap(); // c2=100, cum=100

    // c1 globally sets to 5; epoch bumps; cumulative=5; c2's 100 is gone from cumulative
    c1.set(&k, 5).await.unwrap();

    // c2 is stale → inst resets to 3; cumulative = 5+3 = 8
    // (NOT 5+100+3=108 — old count is not in current cumulative)
    let (cum, _) = c2.inc(&k, 3).await.unwrap();
    assert_eq!(cum, 8);

    c1.clear().await.unwrap();
}

// ===========================================================================
// Existing 2-instance tests (retained)
// ===========================================================================

#[tokio::test]
async fn two_instances_cumulate_via_inc() {
    let (c1, c2) = make_pair("two_inc").await;
    let k = key("x");

    c1.inc(&k, 10).await.unwrap();
    assert_eq!(c2.inc(&k, 20).await.unwrap().0, 30);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn two_instances_cumulate_via_set_on_instance() {
    let (c1, c2) = make_pair("two_set_on_inst").await;
    let k = key("x");

    assert_eq!(c1.set_on_instance(&k, 5).await.unwrap().0, 5);
    assert_eq!(c2.set_on_instance(&k, 10).await.unwrap().0, 15);

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

#[tokio::test]
async fn stale_instance_resets_to_delta_after_epoch_bump() {
    let (c1, c2) = make_pair("stale_epoch").await;
    let k = key("x");

    c1.set_on_instance(&k, 5).await.unwrap(); // c1=5, cum=5
    c2.set_on_instance(&k, 10).await.unwrap(); // c2=10, cum=15

    assert_eq!(c1.set(&k, 3).await.unwrap().0, 3); // epoch bump; cum=3

    // c2 stale → inst resets to 5; cumulative = 3+5 = 8
    assert_eq!(c2.inc(&k, 5).await.unwrap().0, 8);

    c1.clear().await.unwrap();
}

#[tokio::test]
async fn set_increments_epoch_on_each_call() {
    let c = make_counter("epoch_monotonic").await;
    let k = key("x");

    c.set(&k, 10).await.unwrap();
    assert_eq!(c.set(&k, 20).await.unwrap().0, 20);
    assert_eq!(c.get(&k).await.unwrap().0, 20);

    c.clear().await.unwrap();
}

#[tokio::test]
async fn dead_instance_counts_removed_from_cumulative() {
    let (c1, c2) = make_pair_with_opts("dead_test", 100).await;
    let k = key("hits");

    c1.inc(&k, 40).await.unwrap();
    c2.inc(&k, 60).await.unwrap(); // cumulative = 100

    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(c1);

    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 60);

    c2.clear().await.unwrap();
}

// ===========================================================================
// Recovery
// ===========================================================================

/// An instance that went offline (declared dead, contribution removed) recovers
/// its prior count via `inc` when it comes back.
#[tokio::test]
async fn recovery_via_inc_after_downtime() {
    let (c1, c2) = make_pair_with_opts("recovery_inc", 100).await;
    let k = key("x");

    c1.inc(&k, 20).await.unwrap(); // c1 contributes 20; cumulative = 20

    // c1 goes offline: sleep past the dead threshold without refreshing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 triggers dead-instance cleanup — removes c1's 20.
    let (cum_after_cleanup, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum_after_cleanup, 0);

    // c1 comes back. inc(5) detects instance_created and recovers old count (20),
    // then adds the new delta (5): final cumulative = 25.
    let (cum, _) = c1.inc(&k, 5).await.unwrap();
    assert_eq!(cum, 25);

    let (total, c1_inst) = c1.get(&k).await.unwrap();
    assert_eq!(total, 25);
    assert_eq!(c1_inst, 25);

    c1.clear().await.unwrap();
}

/// Recovery also fires from `get` — a read that discovers the instance was cleaned up.
#[tokio::test]
async fn recovery_via_get_after_downtime() {
    let (c1, c2) = make_pair_with_opts("recovery_get", 100).await;
    let k = key("x");

    c1.inc(&k, 30).await.unwrap(); // cumulative = 30

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 cleans up c1.
    let (cum_after_cleanup, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum_after_cleanup, 0);

    // c1 calls get — triggers recovery of its 30.
    let (cum, inst) = c1.get(&k).await.unwrap();
    assert_eq!(cum, 30);
    assert_eq!(inst, 30);

    c1.clear().await.unwrap();
}

/// If an epoch bump happened while the instance was offline, its prior
/// contribution is stale — no recovery should occur.
#[tokio::test]
async fn no_recovery_when_epoch_bumped_during_downtime() {
    let (c1, c2) = make_pair_with_opts("no_recovery_epoch", 100).await;
    let k = key("x");

    c1.inc(&k, 20).await.unwrap(); // c1 contributes 20; cumulative = 20

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 bumps the epoch and cleans up c1.
    let (cum, _) = c2.set(&k, 100).await.unwrap();
    assert_eq!(cum, 100);

    // c1 comes back: local_epoch(0) != redis_epoch(1) → stale, no recovery.
    // The stale path resets inst_count to delta and adds it to cumulative.
    let (cum, _) = c1.inc(&k, 5).await.unwrap();
    assert_eq!(cum, 105);

    let (_, c1_inst) = c1.get(&k).await.unwrap();
    assert_eq!(c1_inst, 5);

    c1.clear().await.unwrap();
}

/// `mark_alive` (heartbeat path) re-adds the contribution when the instance
/// was cleaned up but the epoch hasn't changed.
#[tokio::test]
async fn mark_alive_recovery_restores_contribution() {
    let (c1, c2) = make_pair_with_opts("mark_alive_recovery", 100).await;
    let k = key("x");

    c1.inc(&k, 20).await.unwrap(); // c1 contributes 20; cumulative = 20

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 triggers dead-instance cleanup — c1's 20 is subtracted.
    let (cum_after_cleanup, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum_after_cleanup, 0);

    // Simulate the heartbeat firing on c1. mark_alive detects instance_created=1
    // and runs the batched epoch-safe recovery.
    c1.trigger_mark_alive().await.unwrap();

    // c1's 20 should be restored.
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 20);

    c1.clear().await.unwrap();
}

/// `mark_alive` must NOT restore a stale contribution when the epoch was
/// bumped while the instance was offline — doing so would double-count.
#[tokio::test]
async fn mark_alive_no_recovery_when_epoch_bumped() {
    let (c1, c2) = make_pair_with_opts("mark_alive_no_recovery_epoch", 100).await;
    let k = key("x");

    c1.inc(&k, 20).await.unwrap(); // c1 contributes 20; cumulative = 20

    tokio::time::sleep(Duration::from_millis(200)).await;

    // c2 bumps epoch and triggers cleanup — cumulative = 100, c1's 20 is gone.
    let (cum, _) = c2.set(&k, 100).await.unwrap();
    assert_eq!(cum, 100);

    // Heartbeat fires on c1; epoch mismatch means recovery is skipped.
    c1.trigger_mark_alive().await.unwrap();

    // Cumulative must remain 100, not 120.
    let (cum, _) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 100);

    c2.clear().await.unwrap();
}

/// An instance that is still live (never cleaned up) must NOT trigger
/// recovery — its contribution must not be double-counted.
#[tokio::test]
async fn no_recovery_for_live_instance() {
    let (c1, c2) = make_pair_with_opts("no_recovery_live", 1_000).await;
    let k = key("x");

    c1.inc(&k, 20).await.unwrap(); // cumulative = 20
    c1.inc(&k, 5).await.unwrap();  // instance is still live; no recovery; cumulative = 25

    let (cum, c1_inst) = c2.get(&k).await.unwrap();
    assert_eq!(cum, 25);
    assert_eq!(c1_inst, 0); // c2 contributed nothing

    let (_, c1_from_c1) = c1.get(&k).await.unwrap();
    assert_eq!(c1_from_c1, 25); // c1's own view

    c1.clear().await.unwrap();
}
