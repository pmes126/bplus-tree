//! Range-scan iterator for the B+ tree.
//!
//! NOTE: The iterator body is pending a rewrite after the `NodeStorage` API changed to return
//! raw [`NodeView`] bytes instead of typed `Node<K, V>` values.

#![allow(dead_code)]
use crate::bplustree::node::{Node, NodeId};
use crate::bplustree::tree::TreeError;
use crate::storage::NodeStorage;
use crate::storage::epoch::{EpochManager, ReaderGuard};
use std::sync::Arc;

/// An internal stack frame used during tree traversal.
struct TraversalFrame {
    node_id: NodeId,
    index: usize,
}

/// A range iterator over key-value pairs in a B+ tree.
pub struct BPlusTreeIter<'a, K, V, S>
where
    S: NodeStorage,
{
    storage: &'a S,
    current_leaf: Option<Node<K, V>>,
    index: usize,
    start: K,
    end: K,
    stack: Vec<TraversalFrame>,
    reader_guard: ReaderGuard,
}

/// A cursor into a decoded leaf node for sequential reads.
struct LeafCursor<'a, K, V> {
    node_id: NodeId,
    keys: &'a [K],
    values: &'a [V],
    pos: usize,
}

impl<'a, K, V, S> BPlusTreeIter<'a, K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage,
{
    /// Creates a new iterator starting at `start` and ending at `end`.
    pub fn new(
        storage: &'a S,
        root_id: NodeId,
        epoch_mgr: Arc<EpochManager>,
        start: &K,
        end: &K,
    ) -> Self {
        let mut iter = Self {
            storage,
            stack: Vec::new(),
            current_leaf: None,
            start: start.clone(),
            end: end.clone(),
            index: 0,
            reader_guard: epoch_mgr.pin(),
        };
        let _ = iter.descend_to_leaf(root_id, Some(start));
        iter
    }

    // TODO: rewrite after storage API change. NodeStorage::read_node_view now returns NodeView
    // (raw page bytes) rather than Node<K, V>. descend_to_leaf and Iterator::next need a
    // codec (KeyCodec<K> + ValueCodec<V>) to decode NodeView → Node<K, V> before matching.
    fn descend_to_leaf(&mut self, _node_id: NodeId, _key: Option<&K>) -> Result<(), anyhow::Error> {
        todo!("rewrite: decode NodeView → Node<K,V> via codec")
    }
}

// TODO: Iterator impl needs rewriting alongside descend_to_leaf above.
// NodeStorage no longer carries K/V generics; values come out as raw NodeView bytes
// and must be decoded with the appropriate codec before yielding (K, V) pairs.
impl<'a, K, V, S> Iterator for BPlusTreeIter<'a, K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage,
{
    type Item = Result<(K, V), TreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!("rewrite: decode NodeView → Node<K,V> via codec")
    }
}
