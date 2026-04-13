use crate::{CounterComparator, counter::CounterTrait};

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
    counter.dec(&k, 3).await.unwrap(); // 7
    counter.inc(&k, 5).await.unwrap(); // 12
    counter.dec(&k, 2).await.unwrap(); // 10

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

// ---------------------------------------------------------------------------
// set
// ---------------------------------------------------------------------------

/// set returns the value it was given.
#[tokio::test]
async fn set_returns_the_value_that_was_set() {
    let counter = make_strict_counter("test_set_return").await;
    let k = key("val");

    let result = counter.set(&k, 99).await.unwrap();
    assert_eq!(result, 99);
}

/// set overwrites an existing counter value.
#[tokio::test]
async fn set_overrides_existing_value() {
    let counter = make_strict_counter("test_set_override").await;
    let k = key("val");

    counter.inc(&k, 10).await.unwrap();
    counter.set(&k, 50).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 50);
}

/// set to zero is a valid reset.
#[tokio::test]
async fn set_zero_resets_counter() {
    let counter = make_strict_counter("test_set_zero").await;
    let k = key("val");

    counter.inc(&k, 20).await.unwrap();
    counter.set(&k, 0).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 0);
}

/// set accepts negative values.
#[tokio::test]
async fn set_negative_value() {
    let counter = make_strict_counter("test_set_negative").await;
    let k = key("val");

    counter.set(&k, -5).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), -5);
}

/// set works on a key that was never written before.
#[tokio::test]
async fn set_on_new_key() {
    let counter = make_strict_counter("test_set_new").await;
    let k = key("fresh");

    counter.set(&k, 77).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 77);
}

// ---------------------------------------------------------------------------
// del
// ---------------------------------------------------------------------------

/// del returns the value the counter held before deletion.
#[tokio::test]
async fn del_returns_value_before_deletion() {
    let counter = make_strict_counter("test_del_return").await;
    let k = key("val");

    counter.inc(&k, 42).await.unwrap();
    let returned = counter.del(&k).await.unwrap();

    assert_eq!(returned, 42);
}

/// del on a key that was never set returns 0.
#[tokio::test]
async fn del_on_unknown_key_returns_zero() {
    let counter = make_strict_counter("test_del_unknown").await;

    let returned = counter.del(&key("ghost")).await.unwrap();
    assert_eq!(returned, 0);
}

/// after del, get returns 0.
#[tokio::test]
async fn del_removes_the_key() {
    let counter = make_strict_counter("test_del_removes").await;
    let k = key("val");

    counter.inc(&k, 10).await.unwrap();
    counter.del(&k).await.unwrap();

    assert_eq!(counter.get(&k).await.unwrap(), 0);
}

/// del only removes the targeted key; sibling keys are unaffected.
#[tokio::test]
async fn del_does_not_affect_sibling_keys() {
    let counter = make_strict_counter("test_del_sibling").await;
    let k_a = key("a");
    let k_b = key("b");

    counter.inc(&k_a, 5).await.unwrap();
    counter.inc(&k_b, 20).await.unwrap();
    counter.del(&k_a).await.unwrap();

    assert_eq!(counter.get(&k_b).await.unwrap(), 20);
}

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------

/// clear causes all member keys to return 0 afterwards.
#[tokio::test]
async fn clear_removes_all_member_keys() {
    let counter = make_strict_counter("test_clear_all").await;
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
    let counter = make_strict_counter("test_clear_empty").await;
    counter.clear().await.unwrap();
}

/// clear on one prefix does not affect a counter with a different prefix.
#[tokio::test]
async fn clear_does_not_affect_other_prefixes() {
    let counter_a = make_strict_counter("test_clear_isolation_a").await;
    let counter_b = make_strict_counter("test_clear_isolation_b").await;
    let k = key("val");

    counter_a.inc(&k, 10).await.unwrap();
    counter_b.inc(&k, 99).await.unwrap();

    counter_a.clear().await.unwrap();

    assert_eq!(counter_b.get(&k).await.unwrap(), 99);
}

#[tokio::test]
async fn inc_if_uses_all_comparators() {
    let cases = [
        ("eq", CounterComparator::Eq(10), true),
        ("lt", CounterComparator::Lt(11), true),
        ("gt", CounterComparator::Gt(10), false),
        ("ne", CounterComparator::Ne(9), true),
        ("nil", CounterComparator::Nil, true),
    ];

    for (suffix, comparator, should_apply) in cases {
        let counter = make_strict_counter(&format!("test_inc_if_{suffix}")).await;
        let k = key("conditional");
        counter.set(&k, 10).await.unwrap();

        let result = counter.inc_if(&k, comparator, 2).await.unwrap();
        let expected = if should_apply { (12, 10) } else { (10, 10) };

        assert_eq!(result, expected);
        assert_eq!(counter.get(&k).await.unwrap(), expected.0);
    }
}

#[tokio::test]
async fn inc_all_empty_and_inc_all_if_empty_return_empty() {
    let counter = make_strict_counter("test_inc_all_empty").await;
    assert_eq!(counter.inc_all(&[]).await.unwrap(), vec![]);
    assert_eq!(counter.inc_all_if(&[]).await.unwrap(), vec![]);
}

#[tokio::test]
async fn inc_all_supports_duplicate_keys_sequentially() {
    let counter = make_strict_counter("test_inc_all_duplicates").await;
    let k = key("hits");

    let results = counter.inc_all(&[(&k, 1), (&k, 2)]).await.unwrap();

    assert_eq!(results, vec![(&k, 1), (&k, 3)]);
    assert_eq!(counter.get(&k).await.unwrap(), 3);
}

#[tokio::test]
async fn inc_all_if_supports_partial_success_missing_keys_and_duplicates() {
    let counter = make_strict_counter("test_inc_all_if_ordered").await;
    let k1 = key("a");
    let k2 = key("b");

    counter.set(&k1, 0).await.unwrap();
    counter.set(&k2, 10).await.unwrap();

    let results = counter
        .inc_all_if(&[
            (&k1, CounterComparator::Eq(0), 1),
            (&k1, CounterComparator::Eq(1), 2),
            (&k2, CounterComparator::Gt(20), 5),
            (&k2, CounterComparator::Nil, 3),
        ])
        .await
        .unwrap();

    assert_eq!(
        results,
        vec![(&k1, 1, 0), (&k1, 3, 1), (&k2, 10, 10), (&k2, 13, 10)]
    );
    assert_eq!(counter.get(&k1).await.unwrap(), 3);
    assert_eq!(counter.get(&k2).await.unwrap(), 13);
}

#[tokio::test]
async fn set_if_uses_all_comparators() {
    let cases = [
        ("eq", CounterComparator::Eq(10), true),
        ("lt", CounterComparator::Lt(11), true),
        ("gt", CounterComparator::Gt(10), false),
        ("ne", CounterComparator::Ne(9), true),
        ("nil", CounterComparator::Nil, true),
    ];

    for (suffix, comparator, should_apply) in cases {
        let counter = make_strict_counter(&format!("test_set_if_{suffix}")).await;
        let k = key("conditional");
        counter.set(&k, 10).await.unwrap();

        let result = counter.set_if(&k, comparator, 99).await.unwrap();
        let expected = if should_apply { (99, 10) } else { (10, 10) };

        assert_eq!(result, expected);
        assert_eq!(counter.get(&k).await.unwrap(), expected.0);
    }
}

#[tokio::test]
async fn set_all_if_supports_partial_success_and_missing_keys() {
    let counter = make_strict_counter("test_set_all_if_partial").await;
    let k1 = key("a");
    let k2 = key("b");
    let k3 = key("c");

    counter.set(&k1, 10).await.unwrap();
    counter.set(&k2, 20).await.unwrap();

    let results = counter
        .set_all_if(&[
            (&k3, CounterComparator::Nil, 30),
            (&k1, CounterComparator::Gt(5), 11),
            (&k2, CounterComparator::Lt(10), 99),
        ])
        .await
        .unwrap();

    assert_eq!(results, vec![(&k3, 30, 0), (&k1, 11, 10), (&k2, 20, 20)]);
    assert_eq!(counter.get(&k1).await.unwrap(), 11);
    assert_eq!(counter.get(&k2).await.unwrap(), 20);
    assert_eq!(counter.get(&k3).await.unwrap(), 30);
}
