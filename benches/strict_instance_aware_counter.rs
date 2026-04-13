// Criterion benchmarks for `StrictInstanceAwareCounter`.
//
// Every operation issues at least one Redis Lua script call. Latency is
// dominated by the network round-trip to Redis plus the Lua execution.
//
// A single counter instance is shared across all bench functions; each
// function uses a distinct member key so there is no cross-bench state
// interference. Destructive operations (del, del_on_instance, clear,
// clear_on_instance) use `iter_batched` to re-seed the key before every
// measured call. `inc_batch_10` rebuilds its input Vec via `iter_batched`
// because the vec is drained after each call.

mod common;

use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use distkit::DistkitRedisKey;
use tokio::runtime::Runtime;

fn bench_strict_instance_aware_counter(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");
    let counter = rt.block_on(common::make_strict_icounter("strict_icounter"));

    let mut group = c.benchmark_group("strict_instance_aware_counter");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // inc
    let k = common::key("inc");
    group.bench_function("inc", |b| {
        b.to_async(&rt).iter(|| counter.inc(&k, 1));
    });

    // get — seed once so the key exists in Redis
    let k = common::key("get");
    rt.block_on(counter.inc(&k, 1)).expect("seed get failed");
    group.bench_function("get", |b| {
        b.to_async(&rt).iter(|| counter.get(&k));
    });

    // set — bumps the epoch on every call
    let k = common::key("set");
    group.bench_function("set", |b| {
        b.to_async(&rt).iter(|| counter.set(&k, 42));
    });

    // set_on_instance — no epoch bump
    let k = common::key("set_on_instance");
    group.bench_function("set_on_instance", |b| {
        b.to_async(&rt).iter(|| counter.set_on_instance(&k, 42));
    });

    // del — destructive; re-seed before each measured call
    let k = common::key("del");
    group.bench_function("del", |b| {
        b.to_async(&rt).iter_batched(
            || counter.inc(&k, 1),
            |_| counter.del(&k),
            BatchSize::SmallInput,
        );
    });

    // del_on_instance — destructive; re-seed before each measured call
    let k = common::key("del_on_instance");
    group.bench_function("del_on_instance", |b| {
        b.to_async(&rt).iter_batched(
            || counter.inc(&k, 1),
            |_| counter.del_on_instance(&k),
            BatchSize::SmallInput,
        );
    });

    // clear — destructive; re-seed before each measured call
    let k = common::key("clear");
    group.bench_function("clear", |b| {
        b.to_async(&rt).iter_batched(
            || counter.inc(&k, 1),
            |_| counter.clear(),
            BatchSize::SmallInput,
        );
    });

    // clear_on_instance — destructive; re-seed before each measured call
    let k = common::key("clear_on_instance");
    group.bench_function("clear_on_instance", |b| {
        b.to_async(&rt).iter_batched(
            || counter.inc(&k, 1),
            |_| counter.clear_on_instance(),
            BatchSize::SmallInput,
        );
    });

    // inc_batch_10 — pipeline 10 distinct keys in a single batch.
    // The Vec is rebuilt each iteration (inc_batch drains it). The allocation
    // is negligible compared to the Redis round-trip being measured.
    let batch_keys: Vec<DistkitRedisKey> = (0..10)
        .map(|i| common::key(&format!("batch_{i}")))
        .collect();
    group.bench_function("inc_batch_10", |b| {
        b.to_async(&rt).iter(|| async {
            let mut increments: Vec<(DistkitRedisKey, i64)> =
                batch_keys.iter().map(|k| (k.clone(), 1i64)).collect();
            counter.inc_batch(&mut increments, 50).await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_strict_instance_aware_counter);
criterion_main!(benches);
