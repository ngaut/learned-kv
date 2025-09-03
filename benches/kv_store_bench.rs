use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use learned_kv::LearnedKvStore;
use std::collections::HashMap;

fn create_store_with_size(size: usize) -> LearnedKvStore<String, String> {
    let mut data = HashMap::new();
    for i in 0..size {
        let key = format!("key_{}_{:08x}", i, (i as u32).wrapping_mul(0x9e3779b9));
        let value = format!("value_{}", i);
        data.insert(key, value);
    }
    LearnedKvStore::new(data).unwrap()
}

fn bench_lookup_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_performance");
    
    // Test with different dataset sizes
    for size in [100, 1000, 10000].iter() {
        let store = create_store_with_size(*size);
        let all_keys: Vec<String> = store.keys().cloned().collect();
        let test_key = &all_keys[0];
        
        // Single key lookup benchmark
        group.bench_with_input(
            BenchmarkId::new("single_key", size),
            size,
            |b, _| {
                b.iter(|| {
                    black_box(store.get(black_box(test_key)).unwrap())
                })
            },
        );
        
        // Random key lookup benchmark
        group.bench_with_input(
            BenchmarkId::new("random_keys", size),
            size,
            |b, _| {
                let mut key_idx = 0;
                b.iter(|| {
                    let key = &all_keys[key_idx % all_keys.len()];
                    key_idx = key_idx.wrapping_add(1);
                    black_box(store.get(black_box(key)).unwrap())
                })
            },
        );
    }
    
    group.finish();
}

fn bench_construction_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction_performance");
    
    for size in [100, 1000, 10000].iter() {
        let mut data = HashMap::new();
        for i in 0..*size {
            let key = format!("key_{}_{:08x}", i, (i as u32).wrapping_mul(0x9e3779b9));
            let value = format!("value_{}", i);
            data.insert(key, value);
        }
        
        group.bench_with_input(
            BenchmarkId::new("construction", size),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(LearnedKvStore::new(black_box(data.clone())).unwrap())
                })
            },
        );
    }
    
    group.finish();
}

fn bench_key_length_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_length_impact");
    
    // Test with different key lengths up to 4096 bytes
    for key_len in [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096].iter() {
        let mut data = HashMap::new();
        let base_key = "a".repeat(*key_len - 8); // Reserve 8 chars for unique suffix
        
        for i in 0..1000 {
            let key = format!("{}{:08}", base_key, i);
            let value = format!("value_{}", i);
            data.insert(key, value);
        }
        
        let store = LearnedKvStore::new(data).unwrap();
        let all_keys: Vec<String> = store.keys().cloned().collect();
        let test_key = &all_keys[0];
        
        group.bench_with_input(
            BenchmarkId::new("lookup_by_key_length", key_len),
            key_len,
            |b, _| {
                b.iter(|| {
                    black_box(store.get(black_box(test_key)).unwrap())
                })
            },
        );
    }
    
    group.finish();
}

fn bench_key_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_patterns");
    
    let key_len = 1024; // Test with 1KB keys
    let patterns: [(&str, fn(usize, usize) -> HashMap<String, String>); 4] = [
        ("random", generate_random_keys),
        ("sequential", generate_sequential_keys), 
        ("repeated", generate_repeated_pattern_keys),
        ("mixed_content", generate_mixed_content_keys),
    ];
    
    for (pattern_name, generator) in patterns.iter() {
        let data = generator(key_len, 1000);
        let store = LearnedKvStore::new(data).unwrap();
        let all_keys: Vec<String> = store.keys().cloned().collect();
        let test_key = &all_keys[0];
        
        group.bench_with_input(
            BenchmarkId::new("lookup_by_pattern", pattern_name),
            pattern_name,
            |b, _| {
                b.iter(|| {
                    black_box(store.get(black_box(test_key)).unwrap())
                })
            },
        );
    }
    
    group.finish();
}

fn generate_random_keys(key_len: usize, count: usize) -> HashMap<String, String> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut data = HashMap::new();
    for i in 0..count {
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        let hash = hasher.finish();
        
        let mut key = format!("{:016x}", hash);
        while key.len() < key_len {
            (hash.wrapping_mul(0x9e3779b9)).hash(&mut hasher);
            let new_hash = hasher.finish();
            key.push_str(&format!("{:016x}", new_hash));
        }
        key.truncate(key_len);
        
        data.insert(key, format!("value_{}", i));
    }
    data
}

fn generate_sequential_keys(key_len: usize, count: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    let base_key = "0".repeat(key_len - 10);
    
    for i in 0..count {
        let key = format!("{}{:010}", base_key, i);
        data.insert(key, format!("value_{}", i));
    }
    data
}

fn generate_repeated_pattern_keys(key_len: usize, count: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    let pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
    
    for i in 0..count {
        let mut key = String::new();
        while key.len() < key_len - 10 {
            key.push_str(pattern);
        }
        key.truncate(key_len - 10);
        key.push_str(&format!("{:010}", i));
        
        data.insert(key, format!("value_{}", i));
    }
    data
}

fn generate_mixed_content_keys(key_len: usize, count: usize) -> HashMap<String, String> {
    let mut data = HashMap::new();
    
    for i in 0..count {
        let mut key = String::new();
        
        // Mix of patterns: unicode, numbers, special chars
        let sections = key_len / 4;
        key.push_str(&"α".repeat(sections)); // Unicode
        key.push_str(&format!("{}", "1".repeat(sections))); // Numbers
        key.push_str(&"_-=+".repeat(sections / 4)); // Special chars
        key.push_str(&"Z".repeat(sections)); // ASCII
        
        // Ensure exact length
        while key.len() < key_len - 10 {
            key.push('X');
        }
        key.truncate(key_len - 10);
        key.push_str(&format!("{:010}", i));
        
        data.insert(key, format!("value_{}", i));
    }
    data
}

fn bench_hash_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_performance");
    
    // Test hash function performance directly for different key sizes
    for key_len in [64, 256, 1024, 4096].iter() {
        let test_key = "a".repeat(*key_len);
        
        group.bench_with_input(
            BenchmarkId::new("hash_computation", key_len),
            &test_key,
            |b, key| {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                
                b.iter(|| {
                    let mut hasher = DefaultHasher::new();
                    black_box(key).hash(&mut black_box(&mut hasher));
                    black_box(hasher.finish())
                })
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_lookup_performance,
    bench_construction_performance, 
    bench_key_length_impact,
    bench_key_patterns,
    bench_hash_performance
);
criterion_main!(benches);