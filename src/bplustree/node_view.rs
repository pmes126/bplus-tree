use crate::codec::KeyCodec;
use crate::keyfmt::{KeyFormat, ScratchBuf};
use crate::page::{InternalPage, LeafPage, PageError};

use std::fmt;
use thiserror::Error;

pub type NodeId = u64;

/// Errors returned by [`NodeView`] operations.
#[derive(Error, Debug)]
pub enum NodeViewError {
    /// The operation is not valid for this node kind (e.g. reading a value from an internal node).
    #[error("wrong node kind for this operation")]
    WrongKind,
    /// An error from the underlying page layout.
    #[error(transparent)]
    Page(#[from] PageError),
    /// The key-format id stored in the page header has no known decoder.
    #[error("unknown key format id: {0}")]
    UnknownKeyFormat(u8),
}

/// A view of a B+ tree node stored in a page.
///
/// `page_id` tracks the on-disk page ID this node was read from (or written
/// to). It is `None` for freshly constructed nodes that have not yet been
/// persisted.
pub enum NodeView {
    Internal {
        page: InternalPage,
        page_id: Option<NodeId>,
    },
    Leaf {
        page: LeafPage,
        page_id: Option<NodeId>,
    },
}

impl NodeView {
    // --- Initialization ---
    #[inline]
    pub fn new_internal(format_id: KeyFormat) -> Self {
        NodeView::Internal {
            page: InternalPage::new(format_id),
            page_id: None,
        }
    }

    #[inline]
    pub fn new_leaf(format_id: KeyFormat) -> Self {
        NodeView::Leaf {
            page: LeafPage::new(format_id),
            page_id: None,
        }
    }

    /// Returns the on-disk page ID, if this node has been read from or written to storage.
    #[inline]
    pub fn page_id(&self) -> Option<NodeId> {
        match self {
            NodeView::Internal { page_id, .. } | NodeView::Leaf { page_id, .. } => *page_id,
        }
    }

    /// Sets the on-disk page ID for this node.
    #[inline]
    pub fn set_page_id(&mut self, id: NodeId) {
        match self {
            NodeView::Internal { page_id, .. } | NodeView::Leaf { page_id, .. } => {
                *page_id = Some(id);
            }
        }
    }

    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, NodeView::Internal { .. })
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeView::Leaf { .. })
    }

    // --- Safe downcasting ---
    pub fn as_leaf(&self) -> Option<&LeafPage> {
        match self {
            NodeView::Leaf { page, .. } => Some(page),
            _ => None,
        }
    }

    pub fn as_leaf_mut(&mut self) -> Option<&mut LeafPage> {
        match self {
            NodeView::Leaf { page, .. } => Some(page),
            _ => None,
        }
    }

    pub fn as_internal(&self) -> Option<&InternalPage> {
        match self {
            NodeView::Internal { page, .. } => Some(page),
            _ => None,
        }
    }

    pub fn as_internal_mut(&mut self) -> Option<&mut InternalPage> {
        match self {
            NodeView::Internal { page, .. } => Some(page),
            _ => None,
        }
    }

    // --- Operations ---
    // Get the number of keys in the node
    #[inline]
    pub fn keys_len(&self) -> usize {
        match self {
            NodeView::Internal { page, .. } => page.key_count() as usize,
            NodeView::Leaf { page, .. } => page.key_count() as usize,
        }
    }

    /// Returns the value bytes at index `i` (leaf nodes only).
    pub fn value_bytes_at(&self, i: usize) -> Result<&[u8], NodeViewError> {
        match self {
            NodeView::Internal { .. } => Err(NodeViewError::WrongKind),
            NodeView::Leaf { page, .. } => Ok(page.read_value_at(i)?),
        }
    }

    /// Returns the child pointer at index `idx` (internal nodes only).
    #[inline]
    pub fn child_ptr_at(&self, idx: usize) -> Result<u64, NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.read_child_at(idx)?),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Returns the value at index `i` as an owned vector (leaf nodes only).
    #[inline]
    pub fn value_at(&self, i: usize) -> Result<Vec<u8>, NodeViewError> {
        match self {
            NodeView::Internal { .. } => Err(NodeViewError::WrongKind),
            NodeView::Leaf { page, .. } => Ok(page.read_value_at(i)?.to_vec()),
        }
    }

    /// Returns the key at index `i` as an owned vector.
    #[inline]
    pub fn key_at(&self, i: usize) -> Result<Vec<u8>, NodeViewError> {
        let mut scratch = ScratchBuf::new();
        match self {
            NodeView::Internal { page, .. } => Ok(page.get_key_at(i, &mut scratch)?.to_vec()),
            NodeView::Leaf { page, .. } => Ok(page.get_key_at(i, &mut scratch)?.to_vec()),
        }
    }

    /// Returns the key bytes at index `i` without copying.
    #[inline]
    pub fn key_bytes_at(&self, i: usize) -> Result<&[u8], NodeViewError> {
        let mut scratch = ScratchBuf::new();
        match self {
            NodeView::Internal { page, .. } => Ok(page.get_key_at(i, &mut scratch)?),
            NodeView::Leaf { page, .. } => Ok(page.get_key_at(i, &mut scratch)?),
        }
    }

    /// Returns the first key in the node.
    #[inline]
    pub fn first_key(&self) -> Result<Vec<u8>, NodeViewError> {
        self.key_at(0)
    }

    /// Find the insertion index for a given key. This is using a comparator from the key format
    pub fn lower_bound(&self, probe: &[u8]) -> Result<usize, usize> {
        match self {
            NodeView::Internal { page, .. } => {
                let mut scratch = ScratchBuf::new();
                page.lower_bound(probe, &mut scratch)
            }
            NodeView::Leaf { page, .. } => {
                let mut scratch = ScratchBuf::new();
                page.lower_bound(probe, &mut scratch)
            }
        }
    }

    /// Find the insertion index for a given key. Taking a comparator as a parameter
    pub fn lower_bound_cmp(
        &self,
        probe: &[u8],
        cmp: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    ) -> Result<usize, usize> {
        match self {
            NodeView::Internal { page, .. } => {
                let mut scratch = ScratchBuf::new();
                page.lower_bound_cmp(probe, &mut scratch, cmp)
            }
            NodeView::Leaf { page, .. } => {
                let mut scratch = ScratchBuf::new();
                page.lower_bound_cmp(probe, &mut scratch, cmp)
            }
        }
    }

    /// Inserts a key-value pair (leaf) or key-child pointer (internal) into the node.
    pub fn insert(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        child_ptr: Option<u64>,
    ) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => match child_ptr {
                Some(ptr) => Ok(page.insert_encoded(key, ptr)?),
                None => Err(NodeViewError::WrongKind),
            },
            NodeView::Leaf { page, .. } => match value {
                Some(val) => Ok(page.insert_encoded(key, val)?),
                None => Err(NodeViewError::WrongKind),
            },
        }
    }

    /// Inserts a key-value pair at `idx` into a leaf node.
    #[inline]
    pub fn insert_at(&mut self, idx: usize, key: &[u8], value: &[u8]) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { .. } => Err(NodeViewError::WrongKind),
            NodeView::Leaf { page, .. } => Ok(page.insert_at(idx, key, value)?),
        }
    }

    /// Inserts a separator key and right-child pointer at `idx` into an internal node.
    #[inline]
    pub fn insert_separator_at(
        &mut self,
        idx: usize,
        key: &[u8],
        child_ptr: u64,
    ) -> Result<(), NodeViewError> {
        match self {
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
            NodeView::Internal { page, .. } => Ok(page.insert_separator(idx, key, child_ptr)?),
        }
    }

    /// Overwrites the value at `idx` in a leaf node.
    #[inline]
    pub fn replace_at(&mut self, idx: usize, value: &[u8]) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { .. } => Err(NodeViewError::WrongKind),
            NodeView::Leaf { page, .. } => Ok(page.overwrite_value_at(idx, value)?),
        }
    }

    /// Removes the entry at `idx` from the node.
    #[inline]
    pub fn delete_at(&mut self, idx: usize) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.delete_separator(idx)?),
            NodeView::Leaf { page, .. } => Ok(page.delete_at(idx)?),
        }
    }

    /// Get the entry count of the node
    #[inline]
    pub fn entry_count(&self) -> usize {
        match self {
            NodeView::Internal { page, .. } => page.key_count() as usize,
            NodeView::Leaf { page, .. } => page.key_count() as usize,
        }
    }

    /// Returns the number of data bytes used (keys + slots/children + values), excluding header.
    #[inline]
    pub fn used_bytes(&self) -> usize {
        match self {
            NodeView::Internal { page, .. } => page.used_bytes(),
            NodeView::Leaf { page, .. } => page.used_bytes(),
        }
    }

    /// Returns `true` if both nodes' data can fit within a single page buffer.
    #[inline]
    pub fn can_merge_physically(&self, other: &NodeView) -> bool {
        let buffer_cap = match self {
            NodeView::Internal { .. } => crate::page::internal::BUFFER_SIZE,
            NodeView::Leaf { .. } => crate::page::leaf::BUFFER_SIZE,
        };
        self.used_bytes() + other.used_bytes() <= buffer_cap
    }

    /// Splits the node at `idx`, returning the new right half.
    pub fn split_off(&mut self, idx: usize) -> Result<NodeView, NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => {
                let keyfmt_id = KeyFormat::from_id(page.keyfmt_id())
                    .ok_or(NodeViewError::UnknownKeyFormat(page.keyfmt_id()))?;
                let mut new_page = InternalPage::new(keyfmt_id);
                page.split_off_into(idx, &mut new_page)?;
                Ok(NodeView::Internal {
                    page: new_page,
                    page_id: None,
                })
            }
            NodeView::Leaf { page, .. } => {
                let keyfmt_id = KeyFormat::from_id(page.keyfmt_id())
                    .ok_or(NodeViewError::UnknownKeyFormat(page.keyfmt_id()))?;
                let mut new_page = LeafPage::new(keyfmt_id);
                page.split_off_into(idx, &mut new_page)?;
                Ok(NodeView::Leaf {
                    page: new_page,
                    page_id: None,
                })
            }
        }
    }

    /// Replaces the child pointer at `idx` in an internal node.
    pub fn replace_child_at(&mut self, idx: usize, child_ptr: u64) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.replace_child_at(idx, child_ptr)?),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Pops the last key from an internal node (used during splits).
    pub fn pop_key(&mut self) -> Result<Option<Vec<u8>>, NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => {
                if page.key_count() == 0 {
                    return Ok(None);
                }
                let mut scratch = ScratchBuf::new();
                Ok(Some(page.pop_last_key(&mut scratch)?))
            }
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Replaces the key at `idx`.
    pub fn replace_key_at(&mut self, idx: usize, new_key: &[u8]) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.replace_key_at(idx, new_key)?),
            NodeView::Leaf { page, .. } => Ok(page.replace_key_at(idx, new_key)?),
        }
    }

    /// Writes the leftmost child pointer of an internal node.
    pub fn write_leftmost_child(&mut self, ptr: u64) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.write_leftmost_child(ptr)?),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Returns the number of child pointers in an internal node.
    pub fn children_len(&self) -> Result<usize, NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.key_count() as usize + 1),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Prepends a key and child pointer to an internal node.
    pub fn push_front(&mut self, key: &[u8], child_ptr: u64) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.push_front(key, child_ptr)?),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Removes and returns the key at `idx`.
    pub fn delete_key_at(&mut self, idx: usize) -> Result<Vec<u8>, NodeViewError> {
        let mut scratch = ScratchBuf::new();
        match self {
            NodeView::Internal { page, .. } => Ok(page.delete_key_at(idx, &mut scratch)?),
            NodeView::Leaf { page, .. } => Ok(page.delete_key_at(idx, &mut scratch)?),
        }
    }

    /// Inserts a key at `idx` without touching child pointers or values.
    pub fn insert_key_at(&mut self, idx: usize, key: &[u8]) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.insert_key_at(idx, key)?),
            NodeView::Leaf { page, .. } => Ok(page.insert_key_at(idx, key)?),
        }
    }

    /// Removes the child pointer at `idx` from an internal node.
    pub fn delete_child_at(&mut self, idx: usize) -> Result<(), NodeViewError> {
        match self {
            NodeView::Internal { page, .. } => Ok(page.delete_child_at(idx)?),
            NodeView::Leaf { .. } => Err(NodeViewError::WrongKind),
        }
    }

    /// Merges `other` into `self`. Both nodes must be the same kind.
    pub fn merge_into(&mut self, other: &mut NodeView) -> Result<(), NodeViewError> {
        match (self, other) {
            (
                NodeView::Internal {
                    page: self_page, ..
                },
                NodeView::Internal {
                    page: other_page, ..
                },
            ) => {
                let other_key_count = other_page.key_count();
                let mut scratch = ScratchBuf::new();
                for i in 0..other_key_count {
                    let key = other_page.get_key_at(i as usize, &mut scratch)?;
                    let child_ptr = other_page.read_child_at(i as usize + 1)?;
                    self_page.append(key, child_ptr)?;
                }
                Ok(())
            }
            (
                NodeView::Leaf {
                    page: self_page, ..
                },
                NodeView::Leaf {
                    page: other_page, ..
                },
            ) => {
                let other_key_count = other_page.key_count();
                let mut scratch = ScratchBuf::new();
                for i in 0..other_key_count {
                    let (k, v) = other_page.get_kv_at(i as usize, &mut scratch)?;
                    self_page.append(k, v)?;
                }
                Ok(())
            }
            _ => Err(NodeViewError::WrongKind),
        }
    }

    /// Prints the contents of the node for debugging.
    pub fn view_content<KC, K>(&self) -> Result<(), NodeViewError>
    where
        K: std::fmt::Debug,
        KC: KeyCodec<K>,
    {
        match self {
            NodeView::Internal { page, .. } => {
                let key_count = page.key_count();
                let mut scratch = ScratchBuf::new();
                for i in 0..key_count as usize {
                    let key = page.get_key_at(i, &mut scratch)?;
                    let key = KC::decode_key(key);
                    let child_ptr = page.read_child_at(i + 1)?;
                    println!("Key {}: {:?}, Child Ptr: {}", i, key, child_ptr);
                }
                Ok(())
            }
            NodeView::Leaf { page, .. } => {
                let key_count = page.key_count();
                let mut scratch = ScratchBuf::new();
                for i in 0..key_count as usize {
                    let (k, v) = page.get_kv_at(i, &mut scratch)?;
                    println!("Key {}: {:?}, Value: {:?}", i, k, v);
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for NodeView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NodeView::Leaf { page, .. } => page.fmt(f),
            NodeView::Internal { page, .. } => page.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyfmt::raw::RawFormat;

    #[test]
    fn test_node_view_merge() -> Result<(), NodeViewError> {
        // Create first leaf node and insert some key-value pairs
        let mut leaf1 = NodeView::new_leaf(KeyFormat::Raw(RawFormat));
        leaf1.insert(b"key1", Some(b"value1"), None)?;
        leaf1.insert(b"key2", Some(b"value2"), None)?;

        // Create second leaf node and insert some key-value pairs
        let mut leaf2 = NodeView::new_leaf(KeyFormat::Raw(RawFormat));
        leaf2.insert(b"key3", Some(b"value3"), None)?;
        leaf2.insert(b"key4", Some(b"value4"), None)?;

        // Merge leaf2 into leaf1
        leaf1.merge_into(&mut leaf2)?;

        // Verify that leaf1 now contains all key-value pairs
        assert_eq!(leaf1.entry_count(), 4);
        assert_eq!(leaf1.key_at(0)?, b"key1");
        assert_eq!(leaf1.value_at(0)?, b"value1".to_vec());
        assert_eq!(leaf1.key_at(1)?, b"key2");
        assert_eq!(leaf1.value_at(1)?, b"value2".to_vec());
        assert_eq!(leaf1.key_at(2)?, b"key3");
        assert_eq!(leaf1.value_at(2)?, b"value3".to_vec());
        assert_eq!(leaf1.key_at(3)?, b"key4");
        assert_eq!(leaf1.value_at(3)?, b"value4".to_vec());

        let mut internal1 = NodeView::new_internal(KeyFormat::Raw(RawFormat));
        internal1.write_leftmost_child(0)?; // Leftmost child
        internal1.insert(b"key2", None, Some(1))?;
        internal1.insert(b"key4", None, Some(2))?;
        let mut internal2 = NodeView::new_internal(KeyFormat::Raw(RawFormat));
        internal2.write_leftmost_child(3)?; // Leftmost child
        internal2.insert(b"key6", None, Some(3))?;
        internal2.insert(b"key8", None, Some(4))?;
        internal1.merge_into(&mut internal2)?;
        assert_eq!(internal1.entry_count(), 4);
        assert_eq!(internal1.key_at(0)?, b"key2");
        assert_eq!(internal1.child_ptr_at(0)?, 0); // Leftmost child
        assert_eq!(internal1.key_at(1)?, b"key4");
        assert_eq!(internal1.child_ptr_at(1)?, 1);
        assert_eq!(internal1.key_at(2)?, b"key6");
        assert_eq!(internal1.child_ptr_at(2)?, 2);
        assert_eq!(internal1.key_at(3)?, b"key8");
        assert_eq!(internal1.child_ptr_at(3)?, 3);
        assert_eq!(internal1.child_ptr_at(4)?, 4); // Rightmost child

        Ok(())
    }
}
