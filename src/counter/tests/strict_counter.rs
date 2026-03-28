use crate::counter::CounterTrait;

use super::common::{key, make_strict_counter};

/// A key that has never been written returns 0.
#[tokio::test]
async fn get_returns_zero_for_unknown_key() {
    let counter = make_strict_counter("test_get_zero").await;
    let result = counter.get(&key("absent")).await.unwrap();
    assert_eq!(result, 0);
}

/// inc by a positive amount returns the new total and the value is persisted.
#[tokio::test]
async fn inc_positive_returns_new_total() {
    let counter = make_strict_counter("test_inc_positive").await;
    let k = key("hits");

    let after_first = counter.inc(&k, 5).await.unwrap();
    assert_eq!(after_first, 5);

    let stored = counter.get(&k).await.unwrap();
    assert_eq!(stored, 5);
}

/// inc accumulates across multiple calls.
#[tokio::test]
async fn inc_accumulates() {
    let counter = make_strict_counter("test_inc_accumulates").await;
    let k = key("visits");

    counter.inc(&k, 3).await.unwrap();
    counter.inc(&k, 7).await.unwrap();
    let total = counter.inc(&k, 10).await.unwrap();

    assert_eq!(total, 20);
}

/// dec by a positive amount subtracts from the counter.
#[tokio::test]
async fn dec_positive_subtracts() {
    let counter = make_strict_counter("test_dec_positive").await;
    let k = key("stock");

    counter.inc(&k, 100).await.unwrap();
    let after_dec = counter.dec(&k, 30).await.unwrap();
    assert_eq!(after_dec, 70);
}

/// dec can drive a counter below zero (no floor).
#[tokio::test]
async fn dec_below_zero() {
    let counter = make_strict_counter("test_dec_below_zero").await;
    let k = key("balance");

    counter.inc(&k, 10).await.unwrap();
    let result = counter.dec(&k, 25).await.unwrap();
    assert_eq!(result, -15);

    let stored = counter.get(&k).await.unwrap();
    assert_eq!(stored, -15);
}

/// Chained inc and dec operations produce the correct running total.
#[tokio::test]
async fn chained_inc_and_dec() {
    let counter = make_strict_counter("test_chained").await;
    let k = key("score");

    counter.inc(&k, 10).await.unwrap(); // 10
    counter.dec(&k, 3).await.unwrap();  // 7
    counter.inc(&k, 5).await.unwrap();  // 12
    counter.dec(&k, 2).await.unwrap();  // 10

    let total = counter.get(&k).await.unwrap();
    assert_eq!(total, 10);
}

/// Two distinct member keys on the same counter are independent.
#[tokio::test]
async fn independent_keys_do_not_interfere() {
    let counter = make_strict_counter("test_independent").await;
    let k_a = key("alpha");
    let k_b = key("beta");

    counter.inc(&k_a, 42).await.unwrap();
    counter.inc(&k_b, 7).await.unwrap();

    assert_eq!(counter.get(&k_a).await.unwrap(), 42);
    assert_eq!(counter.get(&k_b).await.unwrap(), 7);
}

/// Counters with different prefixes do not share state even for the same member key.
#[tokio::test]
async fn different_prefixes_are_isolated() {
    let counter_a = make_strict_counter("test_prefix_a").await;
    let counter_b = make_strict_counter("test_prefix_b").await;
    let k = key("shared");

    counter_a.inc(&k, 100).await.unwrap();
    counter_b.inc(&k, 1).await.unwrap();

    assert_eq!(counter_a.get(&k).await.unwrap(), 100);
    assert_eq!(counter_b.get(&k).await.unwrap(), 1);
}

/// inc by zero returns the current value unchanged and does not error.
#[tokio::test]
async fn inc_by_zero_is_noop() {
    let counter = make_strict_counter("test_inc_zero").await;
    let k = key("noop");

    counter.inc(&k, 10).await.unwrap();
    let result = counter.inc(&k, 0).await.unwrap();

    assert_eq!(result, 10);
    assert_eq!(counter.get(&k).await.unwrap(), 10);
}

/// dec by zero is a no-op (dec calls inc with negated count).
#[tokio::test]
async fn dec_by_zero_is_noop() {
    let counter = make_strict_counter("test_dec_zero").await;
    let k = key("still");

    counter.inc(&k, 55).await.unwrap();
    let result = counter.dec(&k, 0).await.unwrap();

    assert_eq!(result, 55);
}

/// inc handles large i64 values without error.
#[tokio::test]
async fn inc_large_value() {
    let counter = make_strict_counter("test_large_value").await;
    let k = key("big");

    let large: i64 = 1_000_000_000_000i64;
    let result = counter.inc(&k, large).await.unwrap();
    assert_eq!(result, large);
}

/// get immediately after inc returns the same value inc returned.
#[tokio::test]
async fn get_matches_last_inc_return_value() {
    let counter = make_strict_counter("test_get_matches_inc").await;
    let k = key("counter");

    let inc_result = counter.inc(&k, 17).await.unwrap();
    let get_result = counter.get(&k).await.unwrap();

    assert_eq!(inc_result, get_result);
}
