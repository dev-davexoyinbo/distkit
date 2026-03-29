use crate::counter::CounterTrait;

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
    counter.dec(&k, 3).await.unwrap();  // 7
    counter.inc(&k, 5).await.unwrap();  // 12
    counter.dec(&k, 2).await.unwrap();  // 10

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
