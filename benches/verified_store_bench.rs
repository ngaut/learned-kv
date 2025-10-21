use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use learned_kv::VerifiedKvStore;
use std::collections::HashMap;

fn create_test_data(size: usize, key_len: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    let base = "a".repeat(key_len.saturating_sub(10));

    for i in 0..size {
        let key = format!("{}{:010}", base, i);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }

    data
}

fn lookup_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("verified_store_lookups");

    // Test with 1000 keys
    let data = create_test_data(1000, 64);
    let store: VerifiedKvStore<String, String> = VerifiedKvStore::new(data.clone()).unwrap();
    let test_key = format!("{}{}",  "a".repeat(54), "0000000500");

    group.bench_function("1k_keys_64_bytes", |b| {
        b.iter(|| {
            black_box(store.get(black_box(&test_key)).unwrap())
        })
    });

    group.finish();
}

fn key_length_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_impact");

    for key_len in [64, 128, 256, 512, 1024, 2048].iter() {
        let data = create_test_data(1000, *key_len);
        let store: VerifiedKvStore<String, String> = VerifiedKvStore::new(data.clone()).unwrap();
        let test_key = format!("{}{:010}", "a".repeat(key_len.saturating_sub(10)), 500);

        group.bench_with_input(
            BenchmarkId::new("verified", key_len),
            key_len,
            |b, _key_len| {
                b.iter(|| {
                    black_box(store.get(black_box(&test_key)).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn construction_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction");

    for size in [100, 1000, 10000].iter() {
        let data = create_test_data(*size, 64);

        group.bench_with_input(
            BenchmarkId::new("verified", size),
            size,
            |b, _size| {
                b.iter(|| {
                    black_box(VerifiedKvStore::new(black_box(data.clone())).unwrap())
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, lookup_benchmark, key_length_benchmark, construction_benchmark);
criterion_main!(benches);
