use crate::error::KvError;
use ptr_hash::bucket_fn::Linear;
use ptr_hash::hash::{FastIntHash, KeyHasher};
use ptr_hash::{PtrHash, PtrHashParams};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;

/// High-performance immutable key-value store using Minimal Perfect Hash Functions.
///
/// # ⚠️ **CRITICAL DATA SAFETY WARNING** ⚠️
///
/// **This variant can return WRONG VALUES for non-existent keys!**
///
/// - Keys not in the original dataset may return arbitrary values from the store
/// - No verification is performed - silent data corruption possible
/// - Only safe if you GUARANTEE all lookups are for keys that exist
///
/// **Use `VerifiedKvStore` instead unless:**
/// - You have strict memory constraints (saves key storage, ~30-50% depending on key/value ratio)
/// - You can GUARANTEE all queries are for existing keys
/// - You understand and accept the data corruption risk
///
/// Generic Parameters:
/// - `K`: Key type (must be hashable)
/// - `V`: Value type (must be cloneable)
/// - `H`: Hash function (defaults to FastIntHash for integers)
///
/// This is the optimized variant that trades correctness for memory efficiency.
/// For a safe variant with key verification, see `VerifiedKvStore`.
#[derive(Clone)]
pub struct LearnedKvStore<K, V, H = FastIntHash>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
    H: KeyHasher<K>,
{
    mphf: PtrHash<K, Linear, Vec<u32>, H, Vec<u8>>,
    values: Vec<V>, // Direct storage without Option wrapper
    // Note: keys removed - MPHF is minimal perfect by mathematical guarantee
    // This saves significant memory (no key duplication, no Option overhead)
    len: usize, // Cached length for O(1) access
    _phantom: PhantomData<H>,
}

// Implementation for default hasher (easier type inference)
impl<K, V> LearnedKvStore<K, V, FastIntHash>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
{
    /// Create a new LearnedKvStore from a HashMap with the default hasher.
    ///
    /// For custom hash functions, use `new_with_hasher()` instead.
    pub fn new(data: HashMap<K, V>) -> Result<Self, KvError> {
        Self::new_with_hasher(data)
    }
}

// Implementation for all hashers
impl<K, V, H> LearnedKvStore<K, V, H>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
    H: KeyHasher<K>,
{
    /// Create a new LearnedKvStore with explicit hasher type.
    ///
    /// Available hashers from ptr_hash:
    /// - `FastIntHash` (default for integers)
    /// - `StringHash` (GxHash for strings)
    /// - `StrongerIntHash` (slower but better distribution)
    pub fn new_with_hasher(data: HashMap<K, V>) -> Result<Self, KvError> {
        if data.is_empty() {
            return Err(KvError::EmptyKeySet);
        }

        let keys: Vec<K> = data.keys().cloned().collect();
        let n = keys.len();

        let mphf = PtrHash::new(&keys, PtrHashParams::default());

        // OPTIMIZATION: Pre-allocate with uninitialized memory, then fill directly
        // This avoids Option wrapper overhead (saves 1-8 bytes per entry)
        let mut values: Vec<V> = Vec::with_capacity(n);

        // SAFETY: We're about to initialize all n elements via ptr::write
        // Clippy false positive: we DO initialize all elements via ptr::write below
        #[allow(clippy::uninit_vec)]
        unsafe {
            values.set_len(n);
        }

        // Track which indices are written (for debug verification)
        #[cfg(debug_assertions)]
        let mut written = vec![false; n];

        // Fill values at their MPHF-computed indices
        for (key, value) in data {
            let index = mphf.index(&key);

            // Verify MPHF guarantees
            debug_assert!(index < n, "MPHF returned index {} >= n ({})", index, n);

            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    !written[index],
                    "MPHF collision: index {} written twice",
                    index
                );
                written[index] = true;
            }

            // SAFETY:
            // 1. index < n (verified by debug_assert, guaranteed by MPHF for release)
            // 2. We allocated exactly n slots via set_len
            // 3. MPHF guarantees each index is used exactly once (minimal perfect hash)
            unsafe {
                std::ptr::write(values.as_mut_ptr().add(index), value);
            }
        }

        // Verify all slots were initialized
        #[cfg(debug_assertions)]
        debug_assert!(
            written.iter().all(|&w| w),
            "MPHF bug: not all indices were written. Missing: {:?}",
            written
                .iter()
                .enumerate()
                .filter(|(_, &w)| !w)
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        );

        Ok(Self {
            mphf,
            values,
            len: n, // Cache the length
            _phantom: PhantomData,
        })
    }

    /// Fast lookup with zero-allocation error handling.
    /// Use this method for high-performance scenarios where error details aren't needed.
    ///
    /// ⚠️ **CRITICAL WARNING** ⚠️
    /// This method may return **WRONG VALUES** for keys not in the original dataset!
    /// - For non-existent keys, may return an arbitrary value from the store
    /// - This is a performance/safety trade-off: no verification = faster but unsafe
    /// - Only use if you are CERTAIN all queries are for keys that exist
    ///
    /// **Use `VerifiedKvStore` instead if:**
    /// - You query arbitrary keys (not guaranteed to exist)
    /// - You need guaranteed error detection for non-existent keys
    /// - Safety is more important than maximum performance
    ///
    /// OPTIMIZATION: Trusts MPHF guarantee - minimal perfect hash means every valid
    /// key maps to exactly one unique index in [0, n). No key verification needed.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Result<&V, KvError> {
        let index = self.mphf.index(key);

        // For keys in the original set, MPHF guarantees index < n
        // For non-existent keys, may return arbitrary index (possibly out of bounds)
        // Using safe indexing: bounds check is optimized away by compiler for valid keys
        self.values.get(index).ok_or(KvError::KeyNotFoundFast)
    }

    /// Lookup with detailed error messages (slower due to string formatting).
    /// Use this method when you need detailed error information for debugging.
    ///
    /// WARNING: Without key verification, this may return an incorrect value
    /// for keys not in the original dataset instead of an error.
    /// Only use for keys you know are in the store.
    #[inline(always)]
    pub fn get_detailed(&self, key: &K) -> Result<&V, KvError> {
        let index = self.mphf.index(key);

        // Safe indexing - compiler optimizes bounds check for valid keys
        self.values.get(index).ok_or_else(|| KvError::KeyNotFound {
            key: format!("{:?}", key),
        })
    }

    /// Check if a key is in the store.
    ///
    /// WARNING: Without key storage, we cannot verify membership.
    /// This method approximates by checking if the MPHF index is in bounds.
    /// For keys not in the original set, this may return false positives.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.mphf.index(key) < self.len
    }

    /// Returns the number of key-value pairs in the store.
    /// O(1) operation - length is cached at construction time.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over all values in the store.
    ///
    /// **Note**: `keys()` and `iter()` are not available in LearnedKvStore
    /// because keys are not stored (memory optimization). If you need to iterate
    /// over keys, use `VerifiedKvStore` or keep a separate `Vec<K>`.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.values.iter()
    }

    /// Returns the approximate **stack-allocated** memory usage in bytes.
    /// Includes the values vector and struct overhead.
    ///
    /// ⚠️ **IMPORTANT - INCOMPLETE MEASUREMENT:**
    /// - Only measures stack-allocated memory (Vec capacity × `size_of::<V>`)
    /// - For `String`, `Vec`, etc: Reports struct size (24 bytes), NOT heap data
    /// - Does NOT include MPHF internal structures (~2-4 bits per key)
    ///
    /// **Example with String values:**
    /// ```text
    /// 10K entries with 128-byte strings:
    ///   Reported: ~240 KB  (just Vec<String> structs)
    ///   Actual:   ~1.5 MB  (including heap string data + MPHF)
    ///   Error:    6-8x underestimation
    /// ```
    ///
    /// **For production capacity planning:**
    /// - Use this for relative comparisons between store variants
    /// - Add MPHF overhead: ~`self.len() * 3 / 8` bytes
    /// - For heap-allocated types, measure actual heap separately
    /// - Use external profiler for accurate total memory usage
    pub fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.values.capacity() * std::mem::size_of::<V>()
        // Note: MPHF memory not included - requires mem_dbg feature
        // Approximate MPHF size: self.len * 3 / 8 bytes (for 3 bits/key)
    }
}

impl<K, V, H> LearnedKvStore<K, V, H>
where
    K: Clone
        + std::hash::Hash
        + Eq
        + std::fmt::Debug
        + Send
        + Sync
        + Serialize
        + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
    H: KeyHasher<K>,
{
    /// Save the store to a file.
    ///
    /// WARNING: Without key storage in the new optimized format, we cannot save/load reliably.
    /// Consider using this method only for testing, or add a custom serialization format
    /// that includes keys.
    ///
    /// This is disabled in the current optimized version. To enable save/load functionality,
    /// you would need to either:
    /// 1. Keep keys in memory (sacrificing memory optimization)
    /// 2. Require users to provide keys at load time
    /// 3. Use a custom serialization format
    pub fn save_to_file<P: AsRef<Path>>(&self, _path: P) -> Result<(), KvError> {
        // Use IoError variant to signal unsupported operation
        Err(KvError::IoError(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Serialization not supported in optimized mode without key storage",
        )))
    }

    /// Load the store from a file.
    ///
    /// See `save_to_file` documentation for limitations.
    pub fn load_from_file<P: AsRef<Path>>(_path: P) -> Result<Self, KvError> {
        Err(KvError::IoError(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Deserialization not supported in optimized mode without key storage",
        )))
    }
}

/// Builder for constructing LearnedKvStore instances.
///
/// Generic parameter `H` allows specifying a custom hasher.
pub struct KvStoreBuilder<K, V, H = FastIntHash> {
    data: HashMap<K, V>,
    _phantom: PhantomData<H>,
}

impl<K, V, H> KvStoreBuilder<K, V, H>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
    H: KeyHasher<K>,
{
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    pub fn insert(mut self, key: K, value: V) -> Self {
        self.data.insert(key, value);
        self
    }

    pub fn extend<I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        self.data.extend(iter);
        self
    }

    pub fn with_entries<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
    {
        Self {
            data: HashMap::from_iter(iter),
            _phantom: PhantomData,
        }
    }

    pub fn build(self) -> Result<LearnedKvStore<K, V, H>, KvError> {
        LearnedKvStore::new_with_hasher(self.data)
    }
}

impl<K, V, H> Default for KvStoreBuilder<K, V, H>
where
    K: Clone + std::hash::Hash + Eq + std::fmt::Debug + Send + Sync,
    V: Clone,
    H: KeyHasher<K>,
{
    fn default() -> Self {
        Self::new()
    }
}
