#![allow(dead_code)]

use crate::bplustree::node::{Node, NodeId};
use crate::bplustree::{EpochManager, epoch::ReaderGuard};
use crate::storage::{KeyCodec, NodeStorage, ValueCodec};
use std::fmt::Debug;
use std::sync::Arc;

struct TraversalFrame {
    node_id: NodeId,
    index: usize,
}

pub struct BPlusTreeIter<'a, K, V, S>
where
    K: KeyCodec + Ord,
    V: ValueCodec,
    S: NodeStorage<K, V>,
{
    storage: &'a S,
    current_leaf: Option<Node<K, V>>,
    index: usize,
    start: K,
    end: K,
    stack: Vec<TraversalFrame>,
    reader_guard: ReaderGuard,
}

struct LeafCursor<'a, K, V> {
    node_id: NodeId,
    keys: &'a [K],
    values: &'a [V],
    pos: usize,
}

impl<'a, K: Debug, V: Debug, S> BPlusTreeIter<'a, K, V, S>
where
    S: NodeStorage<K, V>,
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
{
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

    fn descend_to_leaf(
        &mut self,
        mut node_id: NodeId,
        key: Option<&K>,
    ) -> Result<(), anyhow::Error> {
        loop {
            let node = self.storage.read_node(node_id)?;
            match node {
                Some(Node::Internal { keys, children }) => {
                    let index = match key {
                        Some(k) => match keys.binary_search(k) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        },
                        None => 0,
                    };
                    self.stack.push(TraversalFrame { node_id, index });
                    node_id = children[index];
                }
                Some(Node::Leaf { ref keys, .. }) => {
                    let pos = match key {
                        Some(k) => match keys.binary_search(k) {
                            Ok(i) => i,
                            Err(i) => i,
                        },
                        None => 0,
                    };
                    self.index = pos;
                    self.current_leaf = node.clone();
                    return Ok(());
                }
                None => {
                    // If we reach here, it means the node does not exist
                    return Err(anyhow::anyhow!("Node with ID {} does not exist", node_id));
                }
            }
        }
    }
}

impl<'a, K: Debug, V: Debug, S> Iterator for BPlusTreeIter<'a, K, V, S>
where
    S: NodeStorage<K, V>,
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
{
    type Item = Result<(K, V), anyhow::Error>;

    // Returns the next item in the iteration, it returns a deep copy value of the Key and Value pair if it is within the range
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(Node::Leaf { keys, values, .. }) = &mut self.current_leaf
            {
                if self.index < keys.len() {
                    let (k, v) = (&keys[self.index], &values[self.index]);
                    if k > &self.end {
                        // If the key is beyond the end, stop iteration
                        return None;
                    }
                    self.index += 1;
                    return Some(Ok((k.clone(), v.clone())));
                }
            }

            // Need to move to next subtree
            while let Some(frame) = self.stack.pop() {
                let node = self.storage.read_node(frame.node_id).map_err(Some).ok()?;
                let i = frame.index;
                match node {
                    Some(Node::Internal { keys: _, children }) => {
                        let next_idx = i + 1;
                        if next_idx < children.len() {
                            let next_node = children[next_idx];
                            self.stack.push(TraversalFrame {
                                node_id: frame.node_id,
                                index: next_idx,
                            });
                            let _ = self.descend_to_leaf(next_node, None);
                            break;
                        }
                    }
                    Some(Node::Leaf { .. }) => {
                        panic!("Invalid frame")
                    }
                    None => {
                        // If we reach here, it means we have traversed all nodes
                        return None;
                    }
                }
            }
            if self.stack.is_empty() {
                // No more nodes to traverse
                return None;
            }
        }
    }
}
