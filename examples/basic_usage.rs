use learned_kv::VerifiedKvStore;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Learned Key-Value Store - Basic Usage Example");
    println!("==============================================");

    // Method 1: Creating from HashMap
    println!("\nCreating store from HashMap:");
    let mut data = HashMap::new();
    data.insert("apple".to_string(), 1.50);
    data.insert("banana".to_string(), 0.75);
    data.insert("orange".to_string(), 2.00);
    data.insert("grape".to_string(), 3.25);

    let store = VerifiedKvStore::new(data)?;
    println!("Store created with {} items", store.len());

    // Fast lookups (recommended for performance)
    println!("\nFast lookups (recommended for performance):");
    for fruit in ["apple", "banana", "grape", "kiwi"] {
        match store.get(&fruit.to_string()) {
            Ok(price) => println!("  [OK] {}: ${:.2}", fruit, price),
            Err(_) => println!("  [NOT FOUND] {}: not found (fast)", fruit),
        }
    }

    // Detailed lookups (for debugging) - using colors store since it has String values
    println!("\nDetailed lookups (for debugging):");
    // We'll demonstrate this after creating the colors store below

    // Method 2: Creating from multiple entries
    println!("\nCreating store from multiple entries:");
    let mut color_data = HashMap::new();
    color_data.insert("red".to_string(), "#FF0000".to_string());
    color_data.insert("green".to_string(), "#00FF00".to_string());
    color_data.insert("blue".to_string(), "#0000FF".to_string());
    color_data.insert("yellow".to_string(), "#FFFF00".to_string());

    let colors = VerifiedKvStore::new(color_data)?;

    println!("Color store created with {} items", colors.len());

    // Iteration
    println!("\nColor codes:");
    for (color, code) in colors.iter() {
        println!("  - {}: {}", color, code);
    }

    // Now demonstrate detailed lookups with the colors store
    println!("\nDetailed error example:");
    match colors.get_detailed(&"purple".to_string()) {
        Ok(code) => println!("  Found: {}", code),
        Err(e) => println!("  Detailed error: {}", e),
    }

    // Memory usage and performance information
    println!("\nMemory & Performance Information:");
    println!("  Fruits store: {} bytes", store.memory_usage_bytes());
    println!("  Colors store: {} bytes", colors.memory_usage_bytes());

    println!("\nPerformance characteristics:");
    println!("  - Small keys (≤64 bytes): ~3-6ns lookups");
    println!("  - Medium keys (128-512 bytes): ~10-57ns lookups");
    println!("  - Large keys (1KB+): Linear scaling with key length");
    println!("  - Use get() for hot paths, get_detailed() for debugging");

    // Serialization example
    println!("\nSerialization example:");
    colors.save_to_file("colors.bin")?;
    let loaded_colors: VerifiedKvStore<String, String> =
        VerifiedKvStore::load_from_file("colors.bin")?;
    println!(
        "  Successfully saved and loaded {} color codes",
        loaded_colors.len()
    );

    // Clean up
    std::fs::remove_file("colors.bin")?;

    println!("\nBasic usage demonstration complete!");
    Ok(())
}
