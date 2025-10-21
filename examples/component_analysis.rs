use learned_kv::VerifiedKvStore;
use std::collections::HashMap;
use std::time::Instant;

fn main() {
    println!("=== Component Performance Analysis ===\n");
    println!("Strategy: Measure operations incrementally to isolate costs\n");

    for key_size in [64, 128, 256, 512, 1024, 2048] {
        analyze_key_size(key_size);
        println!();
    }
}

fn analyze_key_size(key_len: usize) {
    println!("Key size: {} bytes", key_len);

    // Create test data
    let mut data = HashMap::new();
    let base = "a".repeat(key_len.saturating_sub(10));

    for i in 0..1000 {
        let key = format!("{}{:010}", base, i);
        let value = format!("value_{}", i);
        data.insert(key, value);
    }

    let keys: Vec<String> = data.keys().cloned().collect();
    let store: VerifiedKvStore<String, String> = VerifiedKvStore::new(data.clone()).unwrap();

    let test_key = format!("{}{:010}", base, 500);
    let iterations = 10_000_000u128;

    // Warm up
    for _ in 0..100000 {
        let _ = store.get(&test_key);
        let _ = test_key == keys[500];
    }

    // 1. Full get() - includes hash, MPHF, bounds check, key comparison, array access
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = store.get(&test_key);
    }
    let full_get_ns = start.elapsed().as_nanos() / iterations;

    // 2. Just key comparison (string equality)
    let stored_key = &keys[500];
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = test_key == *stored_key;
    }
    let key_compare_ns = start.elapsed().as_nanos() / iterations;

    // 3. Hash computation (using std::hash)
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    let start = Instant::now();
    for _ in 0..iterations {
        let mut hasher = DefaultHasher::new();
        test_key.hash(&mut hasher);
        let _ = hasher.finish();
    }
    let hash_ns = start.elapsed().as_nanos() / iterations;

    // 4. Simple array bounds check + access
    let index = 500usize;
    let values = vec!["test"; 1000];
    let start = Instant::now();
    for _ in 0..iterations {
        if index < values.len() {
            let _ = &values[index];
        }
    }
    let array_access_ns = start.elapsed().as_nanos() / iterations;

    // Analysis
    let measured_overhead = key_compare_ns + array_access_ns;
    let mphf_and_hash = full_get_ns.saturating_sub(measured_overhead);

    println!("  Total get() time:        {:>6} ns (100.0%)", full_get_ns);
    println!("  ├─ Key comparison:       {:>6} ns ({:>5.1}%)",
             key_compare_ns, (key_compare_ns as f64 / full_get_ns as f64) * 100.0);
    println!("  ├─ Array access:         {:>6} ns ({:>5.1}%)",
             array_access_ns, (array_access_ns as f64 / full_get_ns as f64) * 100.0);
    println!("  ├─ Hash+MPHF (inferred): {:>6} ns ({:>5.1}%)",
             mphf_and_hash, (mphf_and_hash as f64 / full_get_ns as f64) * 100.0);
    println!("  └─ Ref: std::hash:       {:>6} ns (for comparison)", hash_ns);

    // Identify bottleneck
    if mphf_and_hash > full_get_ns / 2 {
        println!("  🔴 BOTTLENECK: Hashing + MPHF index ({:.1}%)",
                 (mphf_and_hash as f64 / full_get_ns as f64) * 100.0);
        println!("     → Optimize hash function or reduce key size");
    } else if key_compare_ns > full_get_ns / 2 {
        println!("  🔴 BOTTLENECK: Key comparison ({:.1}%)",
                 (key_compare_ns as f64 / full_get_ns as f64) * 100.0);
        println!("     → Consider storing hash prefixes for early rejection");
    } else {
        println!("  ✓  Well-distributed - no single dominant bottleneck");
    }

    // Performance metrics
    let ns_per_byte = full_get_ns as f64 / key_len as f64;
    println!("  📊 {:.3} ns/byte | {:.1}% overhead vs key comparison alone",
             ns_per_byte,
             ((full_get_ns as f64 / key_compare_ns.max(1) as f64 - 1.0) * 100.0));
}
