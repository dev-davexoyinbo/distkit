use crate::{CounterComparator, counter::CounterTrait};

use super::common::{key, make_lax_counter};

// ---------------------------------------------------------------------------
// inc
// ---------------------------------------------------------------------------

/// A key that has never been touched returns 0 (fetched fresh from Redis).
#[tokio::test]
async fn get_returns_zero_for_unknown_key() {
    let counter = make_lax_counter("lax_get_zero").await;
    let result = counter.get(&key("absent")).await.unwrap();
    assert_eq!(result, 0);
}

/// inc returns the new running total from the local store.
#[tokio::test]
async fn inc_positive_returns_new_total() {
    let counter = make_lax_counter("lax_inc_positive").await;
    let k = key("hits");

    let result = counter.inc(&k, 5).await.unwrap();
    assert_eq!(result, 5);
}

/// Multiple inc calls accumulate correctly in the local store.
#[tokio::test]
async fn inc_accumulates() {
    let counter = make_lax_counter("lax_inc_accumulates").await;
    let k = key("visits");

    counter.inc(&k, 3).await.unwrap();
    counter.inc(&k, 7).await.unwrap();
    let total = counter.inc(&k, 10).await.unwrap();

    assert_eq!(total, 20);
}

/// inc by zero returns the current value and leaves it unchanged.
#[tokio::test]
async fn inc_by_zero_is_noop() {
    let counter = make_lax_counter("lax_inc_zero").await;
    let k = key("noop");

    counter.inc(&k, 10).await.unwrap();
    let result = counter.inc(&k, 0).await.unwrap();

    assert_eq!(result, 10);
    assert_eq!(counter.get(&k).await.unwrap(), 10);
}

// ---------------------------------------------------------------------------
// dec
// ---------------------------------------------------------------------------

/// dec subtracts from the local counter.
#[tokio::test]
async fn dec_positive_subtracts() {
    let counter = make_lax_counter("lax_dec_positive").await;
    let k = key("stock");

    counter.inc(&k, 100).await.unwrap();
    let after_dec = counter.dec(&k, 30).await.unwrap();
    assert_eq!(after_dec, 70);
}

/// dec can drive the counter below zero (no floor).
#[tokio::test]
async fn dec_below_zero() {
    let counter = make_lax_counter("lax_dec_below_zero").await;
    let k = key("balance");

    counter.inc(&k, 10).await.unwrap();
    let result = counter.dec(&k, 25).await.unwrap();
    assert_eq!(result, -15);

    assert_eq!(counter.get(&k).await.unwrap(), -15);
}

/// dec by zero is a no-op.
#[tokio::test]
async fn dec_by_zero_is_noop() {
    let counter = make_lax_counter("lax_dec_zero").await;
    let k = key("still");

    counter.inc(&k, 55).await.unwrap();
    let result = counter.dec(&k, 0).await.unwrap();

    assert_eq!(result, 55);
}

// ---------------------------------------------------------------------------
// set
// ---------------------------------------------------------------------------

/// set returns the value it was given.
#[tokio::test]
async fn set_returns_the_value_that_was_set() {
    let counter = make_lax_counter("lax_set_return").await;
    let result = counter.set(&key("val"), 99).await.unwrap();
    assert_eq!(result, 99);
}

/// set overwrites whatever was accumulated before.
#[tokio::test]
async fn set_overrides_existing_value() {
    let counter = make_lax_counter("lax_set_override").await;
    let k = key("val");

    counter.inc(&k, 10).await.unwrap();
    counter.set(&k, 50).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 50);
}

/// set to zero is a valid reset.
#[tokio::test]
async fn set_zero_resets_counter() {
    let counter = make_lax_counter("lax_set_zero").await;
    let k = key("val");

    counter.inc(&k, 20).await.unwrap();
    counter.set(&k, 0).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 0);
}

/// set accepts negative values.
#[tokio::test]
async fn set_negative_value() {
    let counter = make_lax_counter("lax_set_negative").await;
    let k = key("val");

    counter.set(&k, -5).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), -5);
}

/// set works on a key that was never written before.
#[tokio::test]
async fn set_on_new_key() {
    let counter = make_lax_counter("lax_set_new").await;
    let k = key("fresh");

    counter.set(&k, 77).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 77);
}

// ---------------------------------------------------------------------------
// Mixed / isolation
// ---------------------------------------------------------------------------

/// Interleaved inc and dec produce the correct running total.
#[tokio::test]
async fn chained_inc_and_dec() {
    let counter = make_lax_counter("lax_chained").await;
    let k = key("score");

    counter.inc(&k, 10).await.unwrap(); // 10
    counter.dec(&k, 3).await.unwrap(); // 7
    counter.inc(&k, 5).await.unwrap(); // 12
    counter.dec(&k, 2).await.unwrap(); // 10

    assert_eq!(counter.get(&k).await.unwrap(), 10);
}

/// Two member keys on the same counter are independent.
#[tokio::test]
async fn independent_keys_do_not_interfere() {
    let counter = make_lax_counter("lax_independent").await;
    let k_a = key("alpha");
    let k_b = key("beta");

    counter.inc(&k_a, 42).await.unwrap();
    counter.inc(&k_b, 7).await.unwrap();

    assert_eq!(counter.get(&k_a).await.unwrap(), 42);
    assert_eq!(counter.get(&k_b).await.unwrap(), 7);
}

/// Counters with different prefixes do not share state.
#[tokio::test]
async fn different_prefixes_are_isolated() {
    let counter_a = make_lax_counter("lax_prefix_a").await;
    let counter_b = make_lax_counter("lax_prefix_b").await;
    let k = key("shared");

    counter_a.inc(&k, 100).await.unwrap();
    counter_b.inc(&k, 1).await.unwrap();

    assert_eq!(counter_a.get(&k).await.unwrap(), 100);
    assert_eq!(counter_b.get(&k).await.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Lax-specific behaviour
// ---------------------------------------------------------------------------

/// get reflects inc immediately on the same instance — no flush required.
#[tokio::test]
async fn inc_is_visible_locally_before_flush() {
    let counter = make_lax_counter("lax_local_visibility").await;
    let k = key("counter");

    counter.inc(&k, 7).await.unwrap();

    // No sleep — the local store must reflect the write right away.
    assert_eq!(counter.get(&k).await.unwrap(), 7);
}

/// After waiting longer than `allowed_lag` (20 ms), a brand-new counter
/// instance that must re-fetch from Redis sees the flushed value.
#[tokio::test]
async fn value_is_eventually_flushed_to_redis() {
    let prefix = "lax_eventual_flush";
    let k = key("counter");

    let counter = make_lax_counter(prefix).await;
    counter.inc(&k, 42).await.unwrap();

    // Give the background flush task several cycles to run (allowed_lag = 20 ms).
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // A fresh instance has no local state; its first get must hit Redis.
    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 42);
}

// ---------------------------------------------------------------------------
// del
// ---------------------------------------------------------------------------

/// A key with no local store entry (never touched) returns 0 without hitting Redis.
#[tokio::test]
async fn del_on_never_touched_key_returns_zero() {
    let counter = make_lax_counter("lax_del_never_touched").await;
    let result = counter.del(&key("ghost")).await.unwrap();
    assert_eq!(result, 0);
}

/// del before the flush runs returns the full value including the unflushed delta.
#[tokio::test]
async fn del_returns_value_including_unflushed_delta() {
    let counter = make_lax_counter("lax_del_unflushed").await;
    let k = key("val");

    counter.inc(&k, 10).await.unwrap();

    // Delete immediately — Redis still has 0, delta is 10; total must be 10.
    let returned = counter.del(&k).await.unwrap();
    assert_eq!(returned, 10);
}

/// del after the flush has run returns the committed Redis value.
#[tokio::test]
async fn del_returns_value_after_flush() {
    let counter = make_lax_counter("lax_del_after_flush").await;
    let k = key("val");

    counter.inc(&k, 10).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // After flush: Redis has 10, delta is 0; total must still be 10.
    let returned = counter.del(&k).await.unwrap();
    assert_eq!(returned, 10);
}

/// After del, get re-fetches from Redis and returns 0.
#[tokio::test]
async fn del_removes_the_key() {
    let counter = make_lax_counter("lax_del_removes").await;
    let k = key("val");

    counter.inc(&k, 5).await.unwrap();
    counter.del(&k).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 0);
}

/// A second del on the same key has no store entry and returns 0.
#[tokio::test]
async fn del_twice_returns_zero_on_second_call() {
    let counter = make_lax_counter("lax_del_twice").await;
    let k = key("val");

    counter.inc(&k, 7).await.unwrap();
    counter.del(&k).await.unwrap();

    let second = counter.del(&k).await.unwrap();
    assert_eq!(second, 0);
}

/// del purges the pending batch entry so no stale write reaches Redis after deletion.
#[tokio::test]
async fn del_cancels_pending_flush() {
    let prefix = "lax_del_cancel_flush";
    let k = key("val");

    let counter = make_lax_counter(prefix).await;
    counter.inc(&k, 42).await.unwrap();
    counter.del(&k).await.unwrap();

    // Wait for multiple flush cycles — the purged commit must not reach Redis.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 0);
}

/// del only removes the targeted key; sibling keys are unaffected.
#[tokio::test]
async fn del_does_not_affect_sibling_keys() {
    let counter = make_lax_counter("lax_del_sibling").await;
    let k_a = key("a");
    let k_b = key("b");

    counter.inc(&k_a, 5).await.unwrap();
    counter.inc(&k_b, 20).await.unwrap();
    counter.del(&k_a).await.unwrap();

    assert_eq!(counter.get(&k_b).await.unwrap(), 20);
}

/// After del, inc on the same key starts fresh from 0.
#[tokio::test]
async fn del_then_inc_starts_fresh() {
    let counter = make_lax_counter("lax_del_then_inc").await;
    let k = key("val");

    counter.inc(&k, 99).await.unwrap();
    counter.del(&k).await.unwrap();

    let result = counter.inc(&k, 1).await.unwrap();
    assert_eq!(result, 1);
}

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------

/// clear causes all member keys to return 0 afterwards.
#[tokio::test]
async fn clear_removes_all_member_keys() {
    let counter = make_lax_counter("lax_clear_all").await;
    let k_a = key("x");
    let k_b = key("y");
    let k_c = key("z");

    counter.inc(&k_a, 1).await.unwrap();
    counter.inc(&k_b, 2).await.unwrap();
    counter.inc(&k_c, 3).await.unwrap();

    counter.clear().await.unwrap();

    assert_eq!(counter.get(&k_a).await.unwrap(), 0);
    assert_eq!(counter.get(&k_b).await.unwrap(), 0);
    assert_eq!(counter.get(&k_c).await.unwrap(), 0);
}

/// clear on a prefix with no written keys does not error.
#[tokio::test]
async fn clear_on_empty_prefix_does_not_error() {
    let counter = make_lax_counter("lax_clear_empty").await;
    counter.clear().await.unwrap();
}

/// clear on one prefix does not affect a counter with a different prefix.
#[tokio::test]
async fn clear_does_not_affect_other_prefixes() {
    let counter_a = make_lax_counter("lax_clear_isolation_a").await;
    let counter_b = make_lax_counter("lax_clear_isolation_b").await;
    let k = key("val");

    counter_a.inc(&k, 10).await.unwrap();
    counter_b.inc(&k, 99).await.unwrap();

    counter_a.clear().await.unwrap();

    assert_eq!(counter_b.get(&k).await.unwrap(), 99);
}

/// clear is reflected immediately on the same instance without waiting for a flush.
#[tokio::test]
async fn clear_is_visible_immediately_on_same_instance() {
    let counter = make_lax_counter("lax_clear_immediate").await;
    let k_a = key("a");
    let k_b = key("b");

    counter.inc(&k_a, 5).await.unwrap();
    counter.inc(&k_b, 20).await.unwrap();
    counter.clear().await.unwrap();

    assert_eq!(counter.get(&k_a).await.unwrap(), 0);
    assert_eq!(counter.get(&k_b).await.unwrap(), 0);
}

/// clear purges the pending batch so no stale write reaches Redis afterwards.
#[tokio::test]
async fn clear_cancels_pending_flush() {
    let prefix = "lax_clear_cancel_flush";
    let k = key("val");

    let counter = make_lax_counter(prefix).await;
    counter.inc(&k, 42).await.unwrap();
    counter.clear().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 0);
}

/// After clear, inc on the same key starts fresh from 0.
#[tokio::test]
async fn clear_then_inc_starts_fresh() {
    let counter = make_lax_counter("lax_clear_then_inc").await;
    let k = key("val");

    counter.inc(&k, 99).await.unwrap();
    counter.clear().await.unwrap();

    let result = counter.inc(&k, 1).await.unwrap();
    assert_eq!(result, 1);
}

// ---------------------------------------------------------------------------
// Flush — additional operations
// ---------------------------------------------------------------------------

/// set is buffered as a corrective delta and eventually reaches Redis.
#[tokio::test]
async fn set_is_eventually_visible_to_fresh_instance() {
    let prefix = "lax_set_flush";
    let k = key("counter");

    let counter = make_lax_counter(prefix).await;
    counter.set(&k, 55).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 55);
}

/// dec is buffered and eventually visible to a fresh instance.
#[tokio::test]
async fn dec_is_eventually_visible_to_fresh_instance() {
    let prefix = "lax_dec_flush";
    let k = key("counter");

    // Seed a value and wait for it to commit to Redis.
    let counter = make_lax_counter(prefix).await;
    counter.set(&k, 100).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Now decrement and let the negative delta flush.
    counter.dec(&k, 30).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 70);
}

/// set after a partial flush re-fetches the stale cache and produces the
/// correct corrective delta so the final Redis value matches the target.
#[tokio::test]
async fn set_after_partial_flush_is_correct() {
    let prefix = "lax_set_after_flush";
    let k = key("counter");

    let counter = make_lax_counter(prefix).await;
    counter.inc(&k, 10).await.unwrap();

    // Wait for the flush task to commit inc to Redis and let the cache expire.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // set re-fetches remote_total (now 10) and stores delta = 50 - 10 = 40.
    counter.set(&k, 50).await.unwrap();
    assert_eq!(counter.get(&k).await.unwrap(), 50);

    // Wait for the corrective delta to flush.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let fresh = make_lax_counter(prefix).await;
    assert_eq!(fresh.get(&k).await.unwrap(), 50);
}

/// del after set (no prior inc) returns the value that was set.
#[tokio::test]
async fn del_after_set_returns_correct_value() {
    let counter = make_lax_counter("lax_del_after_set").await;
    let k = key("val");

    counter.set(&k, 77).await.unwrap();
    let returned = counter.del(&k).await.unwrap();
    assert_eq!(returned, 77);
}

// ---------------------------------------------------------------------------
// get_all / set_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_all_empty_returns_empty() {
    let counter = make_lax_counter("lax_get_all_empty").await;
    assert_eq!(counter.get_all(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn get_all_unknown_keys_return_zero() {
    let counter = make_lax_counter("lax_get_all_unknown").await;
    let k1 = key("a");
    let k2 = key("b");
    assert_eq!(
        counter.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 0), (&k2, 0)]
    );
}

#[tokio::test]
async fn get_all_returns_correct_values_after_inc() {
    let counter = make_lax_counter("lax_get_all_after_inc").await;
    let k1 = key("a");
    let k2 = key("b");
    counter.inc(&k1, 5).await.unwrap();
    counter.inc(&k2, 10).await.unwrap();
    assert_eq!(
        counter.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 5), (&k2, 10)]
    );
}

#[tokio::test]
async fn get_all_preserves_input_order() {
    let counter = make_lax_counter("lax_get_all_order").await;
    let k1 = key("a");
    let k2 = key("b");
    let k3 = key("c");
    counter.inc(&k1, 1).await.unwrap();
    counter.inc(&k2, 2).await.unwrap();
    counter.inc(&k3, 3).await.unwrap();
    assert_eq!(
        counter.get_all(&[&k3, &k1, &k2]).await.unwrap(),
        vec![(&k3, 3), (&k1, 1), (&k2, 2)]
    );
}

/// A fresh reader with no local cache must re-fetch from Redis and return the
/// value written by the first instance, not stale zeros.
#[tokio::test]
async fn get_all_fetches_stale_keys_from_redis() {
    let prefix = "lax_get_all_stale";
    let k = key("counter");

    let writer = make_lax_counter(prefix).await;
    writer.inc(&k, 42).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reader = make_lax_counter(prefix).await;
    assert_eq!(reader.get_all(&[&k]).await.unwrap(), vec![(&k, 42)]);
}

#[tokio::test]
async fn get_all_mixed_fresh_and_stale() {
    let prefix = "lax_get_all_mixed";
    let k1 = key("a");
    let k2 = key("b");

    let counter = make_lax_counter(prefix).await;
    counter.inc(&k1, 7).await.unwrap();
    counter.inc(&k2, 13).await.unwrap();
    // Wait for flush + cache expiry so both keys are stale on a fresh instance.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reader = make_lax_counter(prefix).await;
    let results = reader.get_all(&[&k1, &k2]).await.unwrap();
    assert_eq!(results, vec![(&k1, 7), (&k2, 13)]);
}

#[tokio::test]
async fn set_all_empty_returns_empty() {
    let counter = make_lax_counter("lax_set_all_empty").await;
    assert_eq!(counter.set_all(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn set_all_returns_target_values() {
    let counter = make_lax_counter("lax_set_all_returns").await;
    let k1 = key("a");
    let k2 = key("b");
    let results = counter.set_all(&[(&k1, 10), (&k2, 20)]).await.unwrap();
    assert_eq!(results, vec![(&k1, 10), (&k2, 20)]);
}

#[tokio::test]
async fn set_all_subsequent_get_all_is_consistent() {
    let counter = make_lax_counter("lax_set_all_consistent").await;
    let k1 = key("a");
    let k2 = key("b");
    counter.set_all(&[(&k1, 100), (&k2, 200)]).await.unwrap();
    assert_eq!(
        counter.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 100), (&k2, 200)]
    );
}

/// set_all is eventually visible to a fresh reader after the flush interval.
#[tokio::test]
async fn set_all_is_eventually_flushed_to_redis() {
    let prefix = "lax_set_all_flush";
    let k1 = key("a");
    let k2 = key("b");

    let counter = make_lax_counter(prefix).await;
    counter.set_all(&[(&k1, 55), (&k2, 77)]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reader = make_lax_counter(prefix).await;
    assert_eq!(
        reader.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 55), (&k2, 77)]
    );
}

#[tokio::test]
async fn set_all_on_new_keys_uses_zero_remote_total() {
    let counter = make_lax_counter("lax_set_all_new_keys").await;
    let k1 = key("x");
    let k2 = key("y");
    // Keys have never been written; remote_total is 0. delta = count - 0 = count.
    let results = counter.set_all(&[(&k1, 30), (&k2, 40)]).await.unwrap();
    assert_eq!(results, vec![(&k1, 30), (&k2, 40)]);
    assert_eq!(
        counter.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 30), (&k2, 40)]
    );
}

#[tokio::test]
async fn set_all_preserves_input_order() {
    let counter = make_lax_counter("lax_set_all_order").await;
    let k1 = key("a");
    let k2 = key("b");
    let k3 = key("c");
    let results = counter
        .set_all(&[(&k3, 30), (&k1, 10), (&k2, 20)])
        .await
        .unwrap();
    assert_eq!(results, vec![(&k3, 30), (&k1, 10), (&k2, 20)]);
}

#[tokio::test]
async fn inc_if_uses_all_comparators_against_local_view() {
    let cases = [
        ("eq", CounterComparator::Eq(10), true),
        ("lt", CounterComparator::Lt(11), true),
        ("gt", CounterComparator::Gt(10), false),
        ("ne", CounterComparator::Ne(9), true),
        ("nil", CounterComparator::Nil, true),
    ];

    for (suffix, comparator, should_apply) in cases {
        let counter = make_lax_counter(&format!("lax_inc_if_{suffix}")).await;
        let k = key("conditional");
        counter.set(&k, 10).await.unwrap();

        let result = counter.inc_if(&k, comparator, 2).await.unwrap();
        let expected = if should_apply { 12 } else { 10 };

        assert_eq!(result, expected);
        assert_eq!(counter.get(&k).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn inc_if_refreshes_stale_value_before_comparing() {
    let prefix = "lax_inc_if_refresh";
    let k = key("hits");

    let writer = make_lax_counter(prefix).await;
    writer.set(&k, 7).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reader = make_lax_counter(prefix).await;
    let result = reader
        .inc_if(&k, CounterComparator::Eq(7), 3)
        .await
        .unwrap();

    assert_eq!(result, 10);
    assert_eq!(reader.get(&k).await.unwrap(), 10);
}

#[tokio::test]
async fn set_all_if_returns_current_values_for_failed_conditions() {
    let counter = make_lax_counter("lax_set_all_if_failed").await;
    let k = key("hits");

    counter.inc(&k, 5).await.unwrap();
    let results = counter
        .set_all_if(&[(&k, CounterComparator::Gt(10), 20)])
        .await
        .unwrap();

    assert_eq!(results, vec![(&k, 5)]);
    assert_eq!(counter.get(&k).await.unwrap(), 5);
}

#[tokio::test]
async fn set_all_if_is_eventually_flushed_for_successful_updates() {
    let prefix = "lax_set_all_if_flush";
    let k1 = key("a");
    let k2 = key("b");

    let counter = make_lax_counter(prefix).await;
    let results = counter
        .set_all_if(&[
            (&k1, CounterComparator::Nil, 55),
            (&k2, CounterComparator::Gt(0), 77),
        ])
        .await
        .unwrap();
    assert_eq!(results, vec![(&k1, 55), (&k2, 0)]);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let reader = make_lax_counter(prefix).await;
    assert_eq!(
        reader.get_all(&[&k1, &k2]).await.unwrap(),
        vec![(&k1, 55), (&k2, 0)]
    );
}
