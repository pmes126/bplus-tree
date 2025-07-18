mod node;
mod tree;
mod iterator;

pub use node::Node;
pub use node::NodeId;
pub use iterator::BPlusTreeRangeIter;
pub use crate::storage::CodecError;
pub use thiserror::Error;

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unknown backend: {0}")]
    UnknownBackend(String),

    #[error("Failed to initialize backend: {0}")]
    Backend(#[from] CodecError),
    
    #[error("Bad input: {0}")]
    BadInput(String),
    
    #[error("Failed to initialize backend: {0}")]
    BackendAny(String),
    
    #[error("Node Not Found: {0}")]
    NodeNotFound(String),
}
