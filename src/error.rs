use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Key not found: {key}")]
    KeyNotFound { key: String },
    
    #[error("Key not found")]
    KeyNotFoundFast,  // Performance-optimized variant without string allocation
    
    #[error("Store is immutable after construction")]
    ImmutableStore,
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Empty key set provided")]
    EmptyKeySet,
}