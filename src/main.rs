use learned_kv::{VerifiedKvStore, VerifiedKvStoreBuilder};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Learned Key-Value Store Demo");
    println!("==========================");
    println!("Using VerifiedKvStore (safe variant with key verification)");
    println!("Using UUID-STYLE STRING KEYS (best practice for strings)\n");

    // Using UUID-style string keys - best practice for reliable MPHF construction
    // Well-distributed hash pattern prevents collisions that sequential keys cause
    let mut data = HashMap::new();
    for i in 0..1000 {
        data.insert(format!("key-{:04x}-{:04x}", i / 256, i % 256), format!("value_{}", i));
    }
    println!("Created {} key-value pairs", data.len());

    let store: VerifiedKvStore<String, String> = VerifiedKvStore::new(data)?;
    println!("Built key-value store using PtrHash MPHF");
    println!("Store contains {} items", store.len());
    println!("Memory usage: ~{} bytes", store.memory_usage_bytes());

    println!("\nTesting lookups (safe - verifies keys):");
    for test_key in ["key-0000-0000", "key-0000-002a", "key-0003-00e7", "nonexistent_key"] {
        match store.get(&test_key.to_string()) {
            Ok(value) => println!("  {}: {}", test_key, value),
            Err(e) => println!("  {}: Error - {}", test_key, e),
        }
    }

    println!("\nTesting builder pattern:");
    let small_store: VerifiedKvStore<String, String> = VerifiedKvStoreBuilder::new()
        .insert("hello".to_string(), "world".to_string())
        .insert("foo".to_string(), "bar".to_string())
        .insert("rust".to_string(), "awesome".to_string())
        .build()?;

    println!(
        "Small store has {} items:",
        small_store.len()
    );
    for (key, value) in small_store.iter() {
        println!("  {}: {}", key, value);
    }

    println!("\nNote: VerifiedKvStore supports full API (iter, keys, serialization)");
    println!("For maximum performance without key verification, use LearnedKvStore");
    println!("(WARNING: LearnedKvStore may return wrong values for non-existent keys)");

    Ok(())
}
