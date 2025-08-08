mod node;
mod tree;
mod iterator;
mod epoch;
mod transaction;

pub use node::Node;
pub use node::NodeId;
pub use iterator::BPlusTreeIter;
pub use epoch::EpochManager;
pub use crate::storage::codec::CodecError;
pub use thiserror::Error;
use anyhow::Result;

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

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("Commit failed after {0} retries")]
    MaxRetries(usize),

    #[error("Commit aborted due to node not found: {0}")]
    NodeNotFound(String),

    #[error("Commit aborted due to IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Commit aborted due to codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("Commit aborted due to root mismatch")]
    RebaseRequired,
}

pub trait TxnTracker {
    fn reclaim(&mut self, node_id: NodeId) -> Result<()>;
    fn add_new(&mut self, node_id: NodeId) -> Result<()>;
    fn record_staged_height(&mut self, height: usize);
    fn record_staged_size(&mut self, size: usize);
}
