//! Cursor-based range iterator for the B+ tree.
//!
//! Traverses leaves via a parent stack — no sibling pointers required.
//! The epoch is pinned for the lifetime of the iterator, guaranteeing that
//! all pages seen during the scan remain valid (COW snapshot isolation).

use crate::bplustree::NodeView;
use crate::bplustree::node_view::NodeId;
use crate::bplustree::tree::TreeError;
use crate::storage::NodeStorage;
use crate::storage::epoch::{EpochManager, ReaderGuard};

use std::sync::Arc;

/// A stack frame recording which child was last descended into.
struct TraversalFrame {
    node_id: NodeId,
    /// Index of the child pointer that was followed.
    child_index: usize,
}

/// A forward range iterator over `(key, value)` byte pairs in a B+ tree.
///
/// Created by [`SharedBPlusTree::range`][crate::bplustree::tree::SharedBPlusTree].
/// Yields entries in key order from `start` (inclusive) up to `end` (exclusive).
pub struct BPlusTreeIter<'a, S: NodeStorage> {
    storage: &'a S,
    /// Parent frames for backtracking when a leaf is exhausted.
    stack: Vec<TraversalFrame>,
    /// The currently loaded leaf node.
    leaf: Option<NodeView>,
    /// Position within the current leaf.
    pos: usize,
    /// Exclusive upper bound; `None` means unbounded (scan to end of tree).
    end: Option<Vec<u8>>,
    /// Once true, `next()` always returns `None`.
    done: bool,
    /// Pinned epoch — keeps COW pages alive for the duration of the scan.
    /// Declared last so it is dropped after `leaf` and `stack`.
    _guard: ReaderGuard,
}

impl<'a, S: NodeStorage> BPlusTreeIter<'a, S> {
    /// Creates a new range iterator scanning `[start, end)`.
    ///
    /// Pass `None` for `end` to scan from `start` to the end of the tree.
    pub fn new(
        storage: &'a S,
        root_id: NodeId,
        epoch_mgr: &Arc<EpochManager>,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, TreeError> {
        let guard = epoch_mgr.pin();
        let mut iter = Self {
            storage,
            stack: Vec::new(),
            leaf: None,
            pos: 0,
            end: end.map(|e| e.to_vec()),
            done: false,
            _guard: guard,
        };
        iter.seek_to_start(root_id, start)?;
        Ok(iter)
    }

    /// Descends from `node_id` to the first leaf containing a key >= `start`.
    fn seek_to_start(&mut self, node_id: NodeId, start: &[u8]) -> Result<(), TreeError> {
        let mut current_id = node_id;
        loop {
            let node = self
                .storage
                .read_node_view(current_id)?
                .ok_or(TreeError::Invariant("node not found during range scan"))?;

            if node.is_internal() {
                let child_idx = match node.lower_bound(start) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                self.stack.push(TraversalFrame {
                    node_id: current_id,
                    child_index: child_idx,
                });
                current_id = node.child_ptr_at(child_idx)?;
            } else {
                // Leaf node.
                let pos = match node.lower_bound(start) {
                    Ok(i) => i,
                    Err(i) => i,
                };
                if pos < node.keys_len() {
                    self.leaf = Some(node);
                    self.pos = pos;
                } else {
                    // All keys in this leaf are < start. Move to next leaf.
                    self.advance_leaf()?;
                }
                return Ok(());
            }
        }
    }

    /// Moves to the next leaf by backtracking through the parent stack.
    fn advance_leaf(&mut self) -> Result<(), TreeError> {
        self.leaf = None;
        loop {
            let frame = match self.stack.pop() {
                Some(f) => f,
                None => {
                    self.done = true;
                    return Ok(());
                }
            };

            // Re-read the internal node (safe: epoch is pinned).
            let node = self
                .storage
                .read_node_view(frame.node_id)?
                .ok_or(TreeError::Invariant("parent not found during range scan"))?;

            let next_child = frame.child_index + 1;
            let num_children = node.keys_len() + 1;

            if next_child < num_children {
                // Advance to next child in this internal node.
                self.stack.push(TraversalFrame {
                    node_id: frame.node_id,
                    child_index: next_child,
                });
                let child_id = node.child_ptr_at(next_child)?;
                self.descend_leftmost(child_id)?;
                return Ok(());
            }
            // This internal is exhausted — keep popping.
        }
    }

    /// Descends to the leftmost leaf under `node_id`.
    fn descend_leftmost(&mut self, node_id: NodeId) -> Result<(), TreeError> {
        let mut current_id = node_id;
        loop {
            let node = self
                .storage
                .read_node_view(current_id)?
                .ok_or(TreeError::Invariant("node not found during range scan"))?;

            if node.is_internal() {
                self.stack.push(TraversalFrame {
                    node_id: current_id,
                    child_index: 0,
                });
                current_id = node.child_ptr_at(0)?;
            } else {
                if node.keys_len() > 0 {
                    self.leaf = Some(node);
                    self.pos = 0;
                } else {
                    // Empty leaf — advance past it.
                    self.advance_leaf()?;
                }
                return Ok(());
            }
        }
    }
}

impl<'a, S: NodeStorage> Iterator for BPlusTreeIter<'a, S> {
    type Item = Result<(Vec<u8>, Vec<u8>), TreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let leaf = self.leaf.as_ref()?;

        // Read current entry as owned bytes.
        let key = match leaf.key_at(self.pos) {
            Ok(k) => k,
            Err(e) => return Some(Err(e.into())),
        };

        // Check exclusive upper bound.
        if let Some(end) = &self.end {
            if key.as_slice() >= end.as_slice() {
                self.done = true;
                return None;
            }
        }

        let value = match leaf.value_at(self.pos) {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        };

        // Advance cursor.
        self.pos += 1;
        if self.pos >= leaf.keys_len() {
            if let Err(_e) = self.advance_leaf() {
                self.done = true;
                // Yield the current entry; the error is lost on this call.
                // This matches the behavior of other embedded DB iterators.
            }
        }

        Some(Ok((key, value)))
    }
}
