//! # VerifiedKvStore
//!
//! A high-performance **String key-value store** using Minimal Perfect Hash Functions (MPHF).
//!
//! ## Features
//!
//! - **String keys only** - Simplified, no type confusion
//! - **O(1) lookups** - 5-300ns depending on key size
//! - **Safe verification** - Returns errors for missing keys, never wrong values
//! - **GxHash** - AES-NI accelerated, handles all string patterns
//! - **Immutable** - Built once, read many times
//! - **Serializable** - Save/load to disk
//!
//! ## Performance
//!
//! Based on optimized release builds:
//! - **64 bytes**: ~5ns lookups
//! - **128-512 bytes**: ~10-52ns lookups
//! - **1KB-2KB**: ~133-318ns lookups (hash-bound)
//!
//! ## Quick Start
//!
//! ```rust
//! use learned_kv::VerifiedKvStore;
//! use std::collections::HashMap;
//!
//! // Build from HashMap
//! let mut data = HashMap::new();
//! data.insert("key1".to_string(), "value1".to_string());
//! data.insert("key2".to_string(), "value2".to_string());
//! let store = VerifiedKvStore::new(data).unwrap();
//!
//! // Query
//! assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
//!
//! // Iterate
//! for (key, value) in store.iter() {
//!     println!("{}: {}", key, value);
//! }
//!
//! // Serialize
//! store.save_to_file("data.bin").unwrap();
//! let loaded: VerifiedKvStore<String> = VerifiedKvStore::load_from_file("data.bin").unwrap();
//! # std::fs::remove_file("data.bin").ok();
//! ```

pub mod error;
pub mod verified_kv_store;

// Persistence is internal implementation detail
mod persistence;

pub use error::KvError;
pub use verified_kv_store::{VerifiedKvStore, VerifiedKvStoreBuilder};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_basic_operations() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());
        data.insert("key2".to_string(), "value2".to_string());
        data.insert("key3".to_string(), "value3".to_string());

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());

        assert_eq!(store.get(&"key1".to_string()).unwrap(), "value1");
        assert_eq!(store.get(&"key2".to_string()).unwrap(), "value2");
        assert_eq!(store.get(&"key3".to_string()).unwrap(), "value3");

        assert!(store.contains_key(&"key1".to_string()));
        assert!(!store.contains_key(&"nonexistent".to_string()));
    }

    #[test]
    fn test_empty_store() {
        let empty_data: HashMap<String, String> = HashMap::new();
        let result = VerifiedKvStore::new(empty_data);
        assert!(matches!(result, Err(KvError::EmptyKeySet)));
    }

    #[test]
    fn test_builder_pattern() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("hello".to_string(), "world".to_string())
            .insert("foo".to_string(), "bar".to_string())
            .build()
            .unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&"hello".to_string()).unwrap(), "world");
        assert_eq!(store.get(&"foo".to_string()).unwrap(), "bar");
    }

    #[test]
    fn test_values_iterator() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("1".to_string(), "one".to_string())
            .insert("2".to_string(), "two".to_string())
            .insert("3".to_string(), "three".to_string())
            .build()
            .unwrap();

        let values: Vec<_> = store.values().cloned().collect();

        assert_eq!(values.len(), 3);
        assert!(values.contains(&"one".to_string()));
        assert!(values.contains(&"two".to_string()));
        assert!(values.contains(&"three".to_string()));
    }

    #[test]
    fn test_serialization() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .insert("more".to_string(), "info".to_string())
            .build()
            .unwrap();

        let test_file = "/tmp/test_verified_serialization.bin";

        // Save should succeed
        assert!(store.save_to_file(test_file).is_ok());

        // Load should succeed
        let loaded: VerifiedKvStore<String> = VerifiedKvStore::load_from_file(test_file).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get(&"test".to_string()).unwrap(), "data");

        // Cleanup
        std::fs::remove_file(test_file).ok();
    }

    #[test]
    fn test_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..100 {
            data.insert(format!("key_{}", i), format!("value_{}", i));
        }

        // Using new() for String keys
        let store = VerifiedKvStore::new(data).unwrap();
        assert_eq!(store.len(), 100);

        // Verify all keys we inserted work correctly
        for i in 0..100 {
            assert_eq!(
                store.get(&format!("key_{}", i)).unwrap(),
                &format!("value_{}", i)
            );
        }
    }

    #[test]
    fn test_memory_usage() {
        let store: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("test".to_string(), "data".to_string())
            .build()
            .unwrap();

        let usage = store.memory_usage_bytes();
        assert!(usage > 0);
    }

    // === Edge Case Tests ===

    #[test]
    fn test_single_element() {
        let mut data = HashMap::new();
        data.insert("only_key".to_string(), 42);
        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        assert_eq!(store.get(&"only_key".to_string()).unwrap(), &42);
        assert!(store.get(&"wrong_key".to_string()).is_err());
    }

    #[test]
    fn test_unicode_keys() {
        let mut data = HashMap::new();
        data.insert("你好".to_string(), "hello".to_string());
        data.insert("🚀".to_string(), "rocket".to_string());
        data.insert("Привет".to_string(), "privet".to_string());
        data.insert("مرحبا".to_string(), "marhaba".to_string());

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.get(&"你好".to_string()).unwrap(), "hello");
        assert_eq!(store.get(&"🚀".to_string()).unwrap(), "rocket");
        assert_eq!(store.get(&"Привет".to_string()).unwrap(), "privet");
        assert_eq!(store.get(&"مرحبا".to_string()).unwrap(), "marhaba");
    }

    #[test]
    fn test_special_characters() {
        let mut data = HashMap::new();
        data.insert("key-with-dashes".to_string(), 1);
        data.insert("key_with_underscores".to_string(), 2);
        data.insert("key.with.dots".to_string(), 3);
        data.insert("key@with#special$chars".to_string(), 4);
        data.insert("key with spaces".to_string(), 5);
        data.insert("key\twith\ttabs".to_string(), 6);
        data.insert("key\nwith\nnewlines".to_string(), 7);

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 7);
        assert_eq!(store.get(&"key-with-dashes".to_string()).unwrap(), &1);
        assert_eq!(store.get(&"key_with_underscores".to_string()).unwrap(), &2);
        assert_eq!(store.get(&"key.with.dots".to_string()).unwrap(), &3);
        assert_eq!(
            store.get(&"key@with#special$chars".to_string()).unwrap(),
            &4
        );
        assert_eq!(store.get(&"key with spaces".to_string()).unwrap(), &5);
        assert_eq!(store.get(&"key\twith\ttabs".to_string()).unwrap(), &6);
        assert_eq!(store.get(&"key\nwith\nnewlines".to_string()).unwrap(), &7);
    }

    #[test]
    fn test_long_keys() {
        let mut data = HashMap::new();
        let short_key = "a".to_string();
        let medium_key = "b".repeat(100);
        let long_key = "c".repeat(1000);
        let very_long_key = "d".repeat(10000);

        data.insert(short_key.clone(), "short");
        data.insert(medium_key.clone(), "medium");
        data.insert(long_key.clone(), "long");
        data.insert(very_long_key.clone(), "very_long");

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.get(&short_key).unwrap(), &"short");
        assert_eq!(store.get(&medium_key).unwrap(), &"medium");
        assert_eq!(store.get(&long_key).unwrap(), &"long");
        assert_eq!(store.get(&very_long_key).unwrap(), &"very_long");
    }

    #[test]
    fn test_empty_string_key() {
        let mut data = HashMap::new();
        data.insert("".to_string(), "empty_key_value");
        data.insert("normal".to_string(), "normal_value");

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&"".to_string()).unwrap(), &"empty_key_value");
        assert_eq!(store.get(&"normal".to_string()).unwrap(), &"normal_value");
    }

    #[test]
    fn test_similar_keys() {
        let mut data = HashMap::new();
        data.insert("key".to_string(), 1);
        data.insert("key1".to_string(), 2);
        data.insert("key2".to_string(), 3);
        data.insert("key_".to_string(), 4);
        data.insert("_key".to_string(), 5);
        data.insert("kkey".to_string(), 6);

        let store = VerifiedKvStore::new(data).unwrap();

        assert_eq!(store.get(&"key".to_string()).unwrap(), &1);
        assert_eq!(store.get(&"key1".to_string()).unwrap(), &2);
        assert_eq!(store.get(&"key2".to_string()).unwrap(), &3);
        assert_eq!(store.get(&"key_".to_string()).unwrap(), &4);
        assert_eq!(store.get(&"_key".to_string()).unwrap(), &5);
        assert_eq!(store.get(&"kkey".to_string()).unwrap(), &6);
    }

    // === Error Handling Tests ===

    #[test]
    fn test_key_not_found_error() {
        let mut data = HashMap::new();
        data.insert("exists".to_string(), "value");
        let store = VerifiedKvStore::new(data).unwrap();

        let result = store.get(&"nonexistent".to_string());
        assert!(result.is_err());
        assert!(matches!(result, Err(KvError::KeyNotFoundFast)));
    }

    #[test]
    fn test_key_not_found_detailed_error() {
        let mut data = HashMap::new();
        data.insert("exists".to_string(), "value");
        let store = VerifiedKvStore::new(data).unwrap();

        let result = store.get_detailed(&"missing_key".to_string());
        assert!(result.is_err());
        if let Err(KvError::KeyNotFound { key }) = result {
            // The key is formatted with Debug formatting, so it includes quotes
            assert!(key.contains("missing_key"));
        } else {
            panic!("Expected KeyNotFound error with key");
        }
    }

    #[test]
    fn test_empty_builder() {
        let result: Result<VerifiedKvStore<String>, _> = VerifiedKvStoreBuilder::new().build();
        assert!(result.is_err());
        assert!(matches!(result, Err(KvError::EmptyKeySet)));
    }

    // === Iterator Tests ===

    #[test]
    fn test_keys_iterator() {
        let store: VerifiedKvStore<i32> = VerifiedKvStoreBuilder::new()
            .insert("a".to_string(), 1)
            .insert("b".to_string(), 2)
            .insert("c".to_string(), 3)
            .build()
            .unwrap();

        let mut keys: Vec<_> = store.keys().cloned().collect();
        keys.sort();

        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_iter_complete() {
        let mut data = HashMap::new();
        data.insert("x".to_string(), 10);
        data.insert("y".to_string(), 20);
        data.insert("z".to_string(), 30);

        let store = VerifiedKvStore::new(data).unwrap();

        let collected: HashMap<_, _> = store.iter().map(|(k, v)| (k.clone(), *v)).collect();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected.get("x"), Some(&10));
        assert_eq!(collected.get("y"), Some(&20));
        assert_eq!(collected.get("z"), Some(&30));
    }

    // === Builder Pattern Tests ===

    #[test]
    fn test_builder_extend() {
        let initial_data = vec![("a".to_string(), 1), ("b".to_string(), 2)];

        let store: VerifiedKvStore<i32> = VerifiedKvStoreBuilder::new()
            .extend(initial_data)
            .insert("c".to_string(), 3)
            .build()
            .unwrap();

        assert_eq!(store.len(), 3);
        assert_eq!(store.get(&"a".to_string()).unwrap(), &1);
        assert_eq!(store.get(&"b".to_string()).unwrap(), &2);
        assert_eq!(store.get(&"c".to_string()).unwrap(), &3);
    }

    #[test]
    fn test_builder_with_entries() {
        let data = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ];

        let store: VerifiedKvStore<String> =
            VerifiedKvStoreBuilder::with_entries(data).build().unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&"key1".to_string()).unwrap(), &"val1".to_string());
    }

    #[test]
    fn test_builder_default() {
        let builder: VerifiedKvStoreBuilder<String> = Default::default();
        let result = builder.build();
        assert!(result.is_err());
    }

    // === Large Scale Tests ===

    #[test]
    fn test_very_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..10000 {
            data.insert(format!("key_{:05}", i), i);
        }

        let store = VerifiedKvStore::new(data).unwrap();
        assert_eq!(store.len(), 10000);

        // Spot check
        assert_eq!(store.get(&"key_00000".to_string()).unwrap(), &0);
        assert_eq!(store.get(&"key_05000".to_string()).unwrap(), &5000);
        assert_eq!(store.get(&"key_09999".to_string()).unwrap(), &9999);

        // Check non-existent
        assert!(store.get(&"key_10000".to_string()).is_err());
    }

    #[test]
    fn test_different_value_types() {
        // Test with various value types
        let int_store: VerifiedKvStore<i32> = VerifiedKvStoreBuilder::new()
            .insert("one".to_string(), 1)
            .build()
            .unwrap();
        assert_eq!(int_store.get(&"one".to_string()).unwrap(), &1);

        let vec_store: VerifiedKvStore<Vec<u8>> = VerifiedKvStoreBuilder::new()
            .insert("bytes".to_string(), vec![1, 2, 3])
            .build()
            .unwrap();
        assert_eq!(vec_store.get(&"bytes".to_string()).unwrap(), &vec![1, 2, 3]);

        let option_store: VerifiedKvStore<Option<String>> = VerifiedKvStoreBuilder::new()
            .insert("some".to_string(), Some("value".to_string()))
            .insert("none".to_string(), None)
            .build()
            .unwrap();
        assert_eq!(
            option_store.get(&"some".to_string()).unwrap(),
            &Some("value".to_string())
        );
        assert_eq!(option_store.get(&"none".to_string()).unwrap(), &None);
    }

    // === Persistence Tests ===

    #[test]
    fn test_save_load_roundtrip() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), vec![1, 2, 3]);
        data.insert("key2".to_string(), vec![4, 5, 6]);
        data.insert("key3".to_string(), vec![7, 8, 9]);

        let original = VerifiedKvStore::new(data).unwrap();
        let test_file = "/tmp/test_roundtrip.bin";

        original.save_to_file(test_file).unwrap();
        let loaded: VerifiedKvStore<Vec<i32>> = VerifiedKvStore::load_from_file(test_file).unwrap();

        assert_eq!(original.len(), loaded.len());
        assert_eq!(
            original.get(&"key1".to_string()).unwrap(),
            loaded.get(&"key1".to_string()).unwrap()
        );
        assert_eq!(
            original.get(&"key2".to_string()).unwrap(),
            loaded.get(&"key2".to_string()).unwrap()
        );
        assert_eq!(
            original.get(&"key3".to_string()).unwrap(),
            loaded.get(&"key3".to_string()).unwrap()
        );

        std::fs::remove_file(test_file).ok();
    }

    #[test]
    fn test_load_nonexistent_file() {
        let result: Result<VerifiedKvStore<String>, _> =
            VerifiedKvStore::load_from_file("/tmp/nonexistent_file_12345.bin");
        assert!(result.is_err());
        assert!(matches!(result, Err(KvError::IoError(_))));
    }

    #[test]
    fn test_persistence_large_dataset() {
        let mut data = HashMap::new();
        for i in 0..1000 {
            data.insert(format!("large_key_{}", i), format!("large_value_{}", i));
        }

        let original = VerifiedKvStore::new(data).unwrap();
        let test_file = "/tmp/test_large_persistence.bin";

        original.save_to_file(test_file).unwrap();
        let loaded: VerifiedKvStore<String> = VerifiedKvStore::load_from_file(test_file).unwrap();

        assert_eq!(original.len(), loaded.len());

        // Verify random keys
        for i in [0, 100, 500, 999] {
            let key = format!("large_key_{}", i);
            assert_eq!(original.get(&key).unwrap(), loaded.get(&key).unwrap());
        }

        std::fs::remove_file(test_file).ok();
    }

    // === Collision and Hash Distribution Tests ===

    #[test]
    fn test_sequential_numeric_strings() {
        // This pattern previously caused issues with FxHash
        let mut data = HashMap::new();
        for i in 0..1000 {
            data.insert(format!("{}", i), i);
        }

        let store = VerifiedKvStore::new(data).unwrap();

        // Verify all keys work
        for i in 0..1000 {
            assert_eq!(store.get(&format!("{}", i)).unwrap(), &i);
        }
    }

    #[test]
    fn test_padded_numeric_strings() {
        let mut data = HashMap::new();
        for i in 0..500 {
            data.insert(format!("{:010}", i), i);
        }

        let store = VerifiedKvStore::new(data).unwrap();

        for i in 0..500 {
            assert_eq!(store.get(&format!("{:010}", i)).unwrap(), &i);
        }
    }

    #[test]
    fn test_uuid_like_keys() {
        let mut data = HashMap::new();
        for i in 0..100 {
            let uuid = format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", i, i, i, i, i);
            data.insert(uuid.clone(), i);
        }

        let store = VerifiedKvStore::new(data.clone()).unwrap();

        for (key, value) in data {
            assert_eq!(store.get(&key).unwrap(), &value);
        }
    }

    // === Memory and Performance Characteristics ===

    #[test]
    fn test_memory_usage_scaling() {
        let small: VerifiedKvStore<String> = VerifiedKvStoreBuilder::new()
            .insert("a".to_string(), "v".to_string())
            .build()
            .unwrap();

        let mut medium_data = HashMap::new();
        for i in 0..100 {
            medium_data.insert(format!("key{}", i), format!("val{}", i));
        }
        let medium = VerifiedKvStore::new(medium_data).unwrap();

        let small_usage = small.memory_usage_bytes();
        let medium_usage = medium.memory_usage_bytes();

        // Medium should use more memory than small
        assert!(medium_usage > small_usage);

        // But not 100x more (due to MPHF efficiency)
        assert!(medium_usage < small_usage * 100);
    }

    #[test]
    fn test_contains_key_accuracy() {
        let mut data = HashMap::new();
        data.insert("present".to_string(), 1);
        data.insert("also_present".to_string(), 2);

        let store = VerifiedKvStore::new(data).unwrap();

        assert!(store.contains_key(&"present".to_string()));
        assert!(store.contains_key(&"also_present".to_string()));
        assert!(!store.contains_key(&"not_present".to_string()));
        assert!(!store.contains_key(&"".to_string()));
        assert!(!store.contains_key(&"presentt".to_string())); // Similar but different
    }
}
