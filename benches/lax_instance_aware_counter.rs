// Criterion benchmarks for `LaxInstanceAwareCounter`.
//
// Warm-path benchmarks (inc_warm, dec_warm, get_warm, set_on_instance_warm)
// measure the in-memory fast path: the DashMap store entry already exists and
// deltas accumulate without a Redis round-trip. Each key is seeded with a
// single `block_on(inc)` before the measurement loop so the entry is
// pre-populated.
//
// `set` is NOT warm: it flushes any pending delta and then calls
// `strict.set`, which bumps the epoch via a Redis Lua script.
//
// Destructive operations (del, del_on_instance, clear, clear_on_instance)
// use `iter_batched` to re-seed the store entry before every measured call.

mod common;

use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use distkit::icounter::InstanceAwareCounterTrait;
use tokio::runtime::Runtime;

fn bench_lax_instance_aware_counter(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to build Tokio runtime");
    let counter = rt.block_on(common::make_lax_icounter("lax_icounter"));

    let mut group = c.benchmark_group("lax_instance_aware_counter");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // inc_warm — buffered locally; pure atomics + async overhead
    let k = common::key("inc");
    rt.block_on(counter.inc(&k, 1)).expect("seed inc failed");
    group.bench_function("inc_warm", |b| {
        b.to_async(&rt).iter(|| counter.inc(&k, 1));
    });

    // dec_warm — delegates to inc(-1); same fast path
    let k = common::key("dec");
    rt.block_on(counter.inc(&k, 1)).expect("seed dec failed");
    group.bench_function("dec_warm", |b| {
        b.to_async(&rt).iter(|| counter.dec(&k, 1));
    });

    // get_warm — returns local estimate; no Redis round-trip
    let k = common::key("get");
    rt.block_on(counter.inc(&k, 1)).expect("seed get failed");
    group.bench_function("get_warm", |b| {
        b.to_async(&rt).iter(|| counter.get(&k));
    });

    // set_on_instance_warm — adjusts local delta to reach target; no Redis
    let k = common::key("set_on_instance");
    rt.block_on(counter.inc(&k, 1))
        .expect("seed set_on_instance failed");
    group.bench_function("set_on_instance_warm", |b| {
        b.to_async(&rt).iter(|| counter.set_on_instance(&k, 42));
    });

    // set — NOT warm: flushes pending delta then calls strict.set (Redis round-trip)
    let k = common::key("set");
    group.bench_function("set", |b| {
        b.to_async(&rt).iter(|| counter.set(&k, 42));
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

    group.finish();
}

criterion_group!(benches, bench_lax_instance_aware_counter);
criterion_main!(benches);
