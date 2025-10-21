//! Robust persistence layer for LearnedKvStore
//!
//! Features:
//! - Format versioning for safe evolution
//! - Checksum validation for data integrity
//! - Atomic writes to prevent corruption
//!
//! ⚠️ **LIMITATION: MPHF is always rebuilt on load**
//! - MPHF serialization is not currently implemented
//! - Load times scale with dataset size (see VerifiedKvStore docs)

use crate::error::KvError;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Current format version - increment when format changes
const FORMAT_VERSION: u32 = 1;

/// Magic number to identify our file format
const MAGIC: &[u8; 8] = b"LEARNKV1";

/// Persistence strategy - currently only RebuildOnLoad is supported
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStrategy {
    /// Rebuild MPHF on load (only supported strategy)
    /// MPHF is not saved - it's reconstructed from keys on every load
    RebuildOnLoad,
}

/// File format header
#[derive(Debug, Serialize, Deserialize)]
struct FileHeader {
    /// Magic number for format identification
    magic: [u8; 8],
    /// Format version for compatibility checking
    version: u32,
    /// Total file size in bytes (for validation)
    file_size: u64,
    /// CRC32 checksum of data section
    checksum: u32,
    /// Number of keys in the store
    key_count: usize,
    /// Strategy used for this file (always 1 = RebuildOnLoad)
    strategy: u8,
}

impl FileHeader {
    fn new(file_size: u64, checksum: u32, key_count: usize, strategy: PersistenceStrategy) -> Self {
        Self {
            magic: *MAGIC,
            version: FORMAT_VERSION,
            file_size,
            checksum,
            key_count,
            strategy: match strategy {
                PersistenceStrategy::RebuildOnLoad => 1,
            },
        }
    }

    fn validate(&self) -> Result<PersistenceStrategy, KvError> {
        // Check magic number
        if &self.magic != MAGIC {
            return Err(KvError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid file format: expected magic {:?}, got {:?}",
                    MAGIC, self.magic
                ),
            )));
        }

        // Check version compatibility
        if self.version != FORMAT_VERSION {
            return Err(KvError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Incompatible format version: expected {}, got {}",
                    FORMAT_VERSION, self.version
                ),
            )));
        }

        // Decode strategy (only RebuildOnLoad supported, but accept legacy value 0 for compatibility)
        let strategy = match self.strategy {
            0 | 1 => PersistenceStrategy::RebuildOnLoad,
            _ => {
                return Err(KvError::IoError(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unknown persistence strategy: {}", self.strategy),
                )))
            }
        };

        Ok(strategy)
    }
}

/// Persisted data container
#[derive(Serialize, Deserialize)]
pub struct PersistedData<K, V> {
    /// Keys in the store
    pub keys: Vec<K>,
    /// Values in the store
    pub values: Vec<V>,
    /// Serialized MPHF (not currently used - MPHF is always rebuilt on load)
    pub mphf_data: Option<Vec<u8>>,
}

/// Writer for atomic file operations
pub struct AtomicWriter {
    temp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
    writer: BufWriter<File>,
}

impl AtomicWriter {
    /// Create a new atomic writer
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, KvError> {
        let final_path = path.as_ref().to_path_buf();
        let temp_path = final_path.with_extension("tmp");

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        Ok(Self {
            temp_path,
            final_path,
            writer: BufWriter::new(file),
        })
    }

    /// Write data to temp file
    pub fn write_all(&mut self, data: &[u8]) -> Result<(), KvError> {
        self.writer.write_all(data)?;
        Ok(())
    }

    /// Commit the write atomically
    pub fn commit(mut self) -> Result<(), KvError> {
        // Flush buffer
        self.writer.flush()?;

        // Sync to disk
        self.writer.get_ref().sync_all()?;

        // Atomic rename
        std::fs::rename(&self.temp_path, &self.final_path)?;

        Ok(())
    }
}

impl Drop for AtomicWriter {
    fn drop(&mut self) {
        // Clean up temp file if commit wasn't called
        let _ = std::fs::remove_file(&self.temp_path);
    }
}

/// Calculate CRC32 checksum
pub fn calculate_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Write data with full integrity protection
pub fn write_with_integrity<K, V, P>(
    path: P,
    data: &PersistedData<K, V>,
    strategy: PersistenceStrategy,
) -> Result<(), KvError>
where
    K: Serialize,
    V: Serialize,
    P: AsRef<Path>,
{
    // Serialize the data section
    let data_bytes = bincode::serialize(data)?;

    // Calculate checksum
    let checksum = calculate_checksum(&data_bytes);

    // Create header
    let header = FileHeader::new(
        (std::mem::size_of::<FileHeader>() + data_bytes.len()) as u64,
        checksum,
        data.keys.len(),
        strategy,
    );
    let header_bytes = bincode::serialize(&header)?;

    // Atomic write
    let mut writer = AtomicWriter::new(path)?;

    // Write header
    writer.write_all(&header_bytes)?;

    // Write data
    writer.write_all(&data_bytes)?;

    // Commit atomically
    writer.commit()?;

    Ok(())
}

/// Read data with full integrity validation
pub fn read_with_validation<K, V, P>(
    path: P,
) -> Result<(PersistedData<K, V>, PersistenceStrategy), KvError>
where
    K: for<'de> Deserialize<'de>,
    V: for<'de> Deserialize<'de>,
    P: AsRef<Path>,
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read and deserialize header
    let header: FileHeader = bincode::deserialize_from(&mut reader)?;

    // Validate header
    let strategy = header.validate()?;

    // Read remaining data
    let mut data_bytes = Vec::new();
    reader.read_to_end(&mut data_bytes)?;

    // Validate checksum
    let actual_checksum = calculate_checksum(&data_bytes);
    if actual_checksum != header.checksum {
        return Err(KvError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Checksum mismatch: expected {}, got {}",
                header.checksum, actual_checksum
            ),
        )));
    }

    // Deserialize data
    let data: PersistedData<K, V> = bincode::deserialize(&data_bytes)?;

    // Validate key count
    if data.keys.len() != header.key_count {
        return Err(KvError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Key count mismatch: header says {}, got {}",
                header.key_count,
                data.keys.len()
            ),
        )));
    }

    Ok((data, strategy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_checksum_calculation() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"hello world!";

        assert_eq!(calculate_checksum(data1), calculate_checksum(data2));
        assert_ne!(calculate_checksum(data1), calculate_checksum(data3));
    }

    #[test]
    fn test_atomic_write_commit() {
        let path = "/tmp/test_atomic_commit.bin";
        let _ = fs::remove_file(path);

        {
            let mut writer = AtomicWriter::new(path).unwrap();
            writer.write_all(b"test data").unwrap();
            writer.commit().unwrap();
        }

        assert!(Path::new(path).exists());
        let content = fs::read(path).unwrap();
        assert_eq!(content, b"test data");

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_atomic_write_rollback() {
        let path = "/tmp/test_atomic_rollback.bin";
        let _ = fs::remove_file(path);

        {
            let mut writer = AtomicWriter::new(path).unwrap();
            writer.write_all(b"test data").unwrap();
            // Don't commit - should rollback
        }

        // File should not exist (rolled back)
        assert!(!Path::new(path).exists());
    }

    #[test]
    fn test_write_read_roundtrip() {
        let path = "/tmp/test_persistence_roundtrip.bin";
        let _ = fs::remove_file(path);

        let original_data = PersistedData {
            keys: vec!["key1".to_string(), "key2".to_string()],
            values: vec![100, 200],
            mphf_data: None,
        };

        write_with_integrity(path, &original_data, PersistenceStrategy::RebuildOnLoad).unwrap();

        let (loaded_data, strategy): (PersistedData<String, i32>, _) =
            read_with_validation(path).unwrap();

        assert_eq!(strategy, PersistenceStrategy::RebuildOnLoad);
        assert_eq!(loaded_data.keys, original_data.keys);
        assert_eq!(loaded_data.values, original_data.values);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_corruption_detection() {
        let path = "/tmp/test_corruption.bin";
        let _ = fs::remove_file(path);

        let data = PersistedData {
            keys: vec!["key1".to_string()],
            values: vec![100],
            mphf_data: None,
        };

        write_with_integrity(path, &data, PersistenceStrategy::RebuildOnLoad).unwrap();

        // Corrupt the file
        let mut file_content = fs::read(path).unwrap();
        if !file_content.is_empty() {
            let last_idx = file_content.len() - 1;
            file_content[last_idx] ^= 0xFF;
        }
        fs::write(path, file_content).unwrap();

        // Should detect corruption
        let result: Result<(PersistedData<String, i32>, _), _> = read_with_validation(path);
        assert!(result.is_err());

        fs::remove_file(path).unwrap();
    }
}
