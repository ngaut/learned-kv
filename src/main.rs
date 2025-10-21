use learned_kv::{VerifiedKvStore, VerifiedKvStoreBuilder};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Learned Key-Value Store Demo");
    println!("==========================");
    println!("Using new_string() for String keys - handles ALL patterns reliably!\n");

    // Demonstrate sequential pattern support with new_string()
    let mut data = HashMap::new();
    for i in 0..1000 {
        data.insert(format!("key_{:04}", i), format!("value_{}", i));
    }
    println!("Created {} key-value pairs with SEQUENTIAL pattern", data.len());
    println!("(This would PANIC with new(), but works with new_string()!)\n");

    let store = VerifiedKvStore::new_string(data)?;
    println!("Built key-value store using PtrHash MPHF with GxHash");
    println!("Store contains {} items", store.len());
    println!("Memory usage: ~{} bytes", store.memory_usage_bytes());

    println!("\nTesting lookups (safe - verifies keys):");
    for test_key in ["key_0000", "key_0042", "key_0999", "nonexistent_key"] {
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

    println!("\nVerifiedKvStore supports:");
    println!("  - Safe key verification (no wrong values)");
    println!("  - Full API (iter, keys, serialization)");
    println!("  - new_string() for String keys - handles all patterns reliably");

    Ok(())
}
