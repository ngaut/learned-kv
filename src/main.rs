use learned_kv::{KvStoreBuilder, LearnedKvStore};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Learned Key-Value Store Demo");
    println!("==========================");

    let mut data = HashMap::new();
    for i in 0..1000 {
        data.insert(format!("key_{}", i), format!("value_{}", i));
    }
    println!("Created {} key-value pairs", data.len());

    let store = LearnedKvStore::new(data)?;
    println!("Built key-value store using PtrHash MPHF");
    println!("Store contains {} items", store.len());
    println!("Memory usage: ~{} bytes", store.memory_usage_bytes());

    println!("\nTesting lookups:");
    for test_key in ["key_0", "key_42", "key_999", "nonexistent_key"] {
        match store.get(&test_key.to_string()) {
            Ok(value) => println!("  {}: {}", test_key, value),
            Err(e) => println!("  {}: Error - {}", test_key, e),
        }
    }

    println!("\nTesting builder pattern:");
    let small_store = KvStoreBuilder::new()
        .insert("hello".to_string(), "world".to_string())
        .insert("foo".to_string(), "bar".to_string())
        .insert("rust".to_string(), "awesome".to_string())
        .build()?;

    println!("Small store has {} items:", small_store.len());
    for (key, value) in small_store.iter() {
        println!("  {}: {}", key, value);
    }

    println!("\nTesting serialization:");
    small_store.save_to_file("test_store.bin")?;
    let loaded_store: LearnedKvStore<String, String> = LearnedKvStore::load_from_file("test_store.bin")?;
    println!("Successfully saved and loaded store from disk");
    println!("Loaded store has {} items", loaded_store.len());

    Ok(())
}