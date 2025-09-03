use learned_kv::{KvStoreBuilder, LearnedKvStore};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Learned Key-Value Store - Basic Usage Example");
    println!("================================================");

    // Method 1: Creating from HashMap
    println!("\n📦 Creating store from HashMap:");
    let mut data = HashMap::new();
    data.insert("apple".to_string(), 1.50);
    data.insert("banana".to_string(), 0.75);
    data.insert("orange".to_string(), 2.00);
    data.insert("grape".to_string(), 3.25);

    let store = LearnedKvStore::new(data)?;
    println!("✅ Store created with {} items", store.len());

    // Fast lookups (recommended for performance)
    println!("\n🔍 Fast lookups (recommended for performance):");
    for fruit in ["apple", "banana", "grape", "kiwi"] {
        match store.get(&fruit.to_string()) {
            Ok(price) => println!("  ✅ {}: ${:.2}", fruit, price),
            Err(_) => println!("  ❌ {}: not found (fast)", fruit),
        }
    }

    // Detailed lookups (for debugging) - using colors store since it has String values
    println!("\n🐛 Detailed lookups (for debugging):");
    // We'll demonstrate this after creating the colors store below

    // Method 2: Builder pattern
    println!("\n🏗️ Using the builder pattern:");
    let colors = KvStoreBuilder::new()
        .insert("red".to_string(), "#FF0000".to_string())
        .insert("green".to_string(), "#00FF00".to_string())
        .insert("blue".to_string(), "#0000FF".to_string())
        .insert("yellow".to_string(), "#FFFF00".to_string())
        .build()?;

    println!("✅ Color store created with {} items", colors.len());

    // Iteration
    println!("\n📋 Color codes:");
    for (color, code) in colors.iter() {
        println!("  • {}: {}", color, code);
    }

    // Now demonstrate detailed lookups with the colors store
    println!("\n🐛 Detailed error example:");
    match colors.get_detailed(&"purple".to_string()) {
        Ok(code) => println!("  Found: {}", code),
        Err(e) => println!("  Detailed error: {}", e),
    }

    // Memory usage and performance information
    println!("\n💾 Memory & Performance Information:");
    println!("  Fruits store: {} bytes", store.memory_usage_bytes());
    println!("  Colors store: {} bytes", colors.memory_usage_bytes());
    
    println!("\n⚡ Performance characteristics:");
    println!("  • Small keys (≤64 bytes): ~7ns lookups");
    println!("  • Medium keys (128-512 bytes): 10-55ns lookups");
    println!("  • Large keys (1KB+): Linear scaling with key length");
    println!("  • Use get() for hot paths, get_detailed() for debugging");

    // Serialization example
    println!("\n💾 Serialization example:");
    colors.save_to_file("colors.bin")?;
    let loaded_colors: LearnedKvStore<String, String> = LearnedKvStore::load_from_file("colors.bin")?;
    println!("  ✅ Successfully saved and loaded {} color codes", loaded_colors.len());
    
    // Clean up
    std::fs::remove_file("colors.bin")?;

    println!("\n🎉 Basic usage demonstration complete!");
    Ok(())
}