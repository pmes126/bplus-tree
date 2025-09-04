use crate::storage::page::{InternalPage, LeafPage};
use anyhow::Result;
use std::cmp::Ordering;

pub type NodeId = u64;

/// A view of a B+ tree node stored in a page
#[derive(Clone, Debug)]
pub enum NodeView {
    Internal { page: InternalPage },
    Leaf { page: LeafPage },
}

impl NodeView {
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, NodeView::Internal { .. })
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeView::Leaf { .. })
    }

    #[inline]
    pub fn key_bytes_at(&self, idx: usize) -> &[u8] {
        match self {
            NodeView::Internal { page } => page.key_bytes_at(idx).unwrap(),
            NodeView::Leaf { page } => page.key_bytes_at(idx).unwrap(),
        }
    }

    #[inline]
    pub fn keys_len(&self) -> usize {
        match self {
            NodeView::Internal { page } => page.header.entry_count as usize,
            NodeView::Leaf { page } => page.header.entry_count as usize,
        }
    }

    /// Lower bound using bytewise compare
    pub fn lower_bound(&self, probe: &[u8]) -> Result<usize, usize> {
        let mut lo = 0usize;
        let mut hi = self.keys_len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key_bytes_at(mid).cmp(probe) {
                Ordering::Less => lo = mid + 1,    // move to the right
                Ordering::Equal => return Ok(mid), // found exact match
                Ordering::Greater => hi = mid,
            }
        }
        Err(lo) // return the insertion point   
    }

    /// Get the child pointer at index i
    #[inline]
    pub fn child_ptr_at(&self, i: usize) -> Result<Option<u64>> {
        match self {
            NodeView::Internal { page } => {
                if i == 0 {
                    return Ok(Some(page.header.leftmost_child)); // No child pointer for index 0
                }
                let idx = i - 1; // Internal nodes have child pointers at i-1
                page.child_at(idx).map(Some).map_err(|e| anyhow::anyhow!(e))
            }
            NodeView::Leaf { .. } => Ok(None), // Leaf pages don't have children, but we return 0
        }
    }

    /// Get the value at index i
    #[inline]
    pub fn value_at(&self, i: usize) -> Result<Option<Vec<u8>>> {
        match self {
            NodeView::Internal { .. } => Ok(None), // Internal nodes do not store values
            NodeView::Leaf { page } => {
                let value = page.value_bytes_at(i)?;
                Ok(Some(value.to_vec()))
            }
        }
    }

    /// Insert a key-value pair or key-child pointer into the node at a given index
    #[inline]
    pub fn insert_at(
        &mut self,
        idx: usize,
        key: &[u8],
        value: Option<&[u8]>,
        child_ptr: Option<u64>,
    ) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                if let Some(ptr) = child_ptr {
                    page.insert_entry_at(idx, key, ptr).map_err(|e| anyhow::anyhow!(e))
                } else {
                    Err(anyhow::anyhow!(
                        "Internal nodes require a child pointer for insertion"
                    ))
                }
            }
            NodeView::Leaf { page } => {
                if let Some(val) = value {
                    page.insert_entry_at(idx, key, val).map_err(|e| anyhow::anyhow!(e))
                } else {
                    Err(anyhow::anyhow!(
                        "Leaf nodes require a value for insertion"
                    ))
                }
            }
        }
    }

    /// Get the entry count of the node
    #[inline]
    pub fn entry_count(&self) -> usize {
        match self {
            NodeView::Internal { page } => page.header.entry_count as usize,
            NodeView::Leaf { page } => page.header.entry_count as usize,
        }
    }

    /// Split the node into two, returning the new node and the split key
    pub fn split_off(&mut self, idx: usize) -> Result<(Vec<u8>, NodeView), anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                let new_page = page.split_off(idx).map_err(|e| anyhow::anyhow!(e))?;
                let split_key = new_page.key_bytes_at(0)?; // First key of the new page
                Ok((split_key.to_vec(), NodeView::Internal { page: new_page }))
            }
            NodeView::Leaf { page } => {
                let new_page = page.split_off(idx).map_err(|e| anyhow::anyhow!(e))?;
                let split_key = new_page.key_bytes_at(0)?; // First key of the new page
                Ok((split_key.to_vec(), NodeView::Leaf { page: new_page }))
            }
        }
    }
}
