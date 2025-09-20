use crate::page::{InternalPage, LeafPage};
use anyhow::Result;

pub type NodeId = u64;

/// A view of a B+ tree node stored in a page
#[derive(Clone, Debug)]
pub enum NodeView {
    Internal { page: InternalPage },
    Leaf { page: LeafPage },
}

impl NodeView {
    #[inline]
    pub fn new_internal(format_id: u8) -> Self {
        NodeView::Internal {
            page: InternalPage::new(format_id),
        }
    }

    #[inline]
    pub fn new_leaf(format_id: u8) -> Self {
        NodeView::Leaf {
            page: LeafPage::new(format_id),
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

    #[inline]
    pub fn keys_len(&self) -> usize {
        match self {
            NodeView::Internal { page } => page.key_count() as usize,
            NodeView::Leaf { page } => page.key_count() as usize,
        }
    }

    pub fn value_bytes_at(&self, i: usize) -> Result<Option<&[u8]>> {
        match self {
            NodeView::Internal { .. } => Ok(None), // Internal nodes do not store values
            NodeView::Leaf { page } => {
                let value = page.read_value_at(i)?;
                Ok(Some(value))
            }
        }
    }

    pub fn get_child_for_key(&self, _probe: &[u8]) -> Result<Option<(NodeId, usize)>> {
        match self {
            NodeView::Leaf { .. } => Ok(None), // Leaf nodes do not have children
            // Internal nodes: find the child pointer for the given key
            NodeView::Internal { .. } => {
                Ok(None) // Placeholder: Implement child pointer search
            }
        }
    }

    /// Get the child pointer at index i
    #[inline]
    pub fn child_ptr_at(&self, idx: usize) -> Result<Option<u64>> {
        match self {
            NodeView::Internal { page } => page
                .read_child_at(idx)
                .map(Some)
                .map_err(|e| anyhow::anyhow!(e)),
            NodeView::Leaf { .. } => Ok(None), // Leaf pages don't have children, but we return 0
        }
    }

    /// Get the value at index i
    #[inline]
    pub fn value_at(&self, i: usize) -> Result<Option<Vec<u8>>> {
        match self {
            NodeView::Internal { .. } => Ok(None), // Internal nodes do not store values
            NodeView::Leaf { page } => {
                let value = page.read_value_at(i)?;
                Ok(Some(value.to_vec()))
            }
        }
    }

    /// Get the key at index i
    #[inline]
    pub fn key_at(&self, i: usize) -> Result<Vec<u8>> {
        let mut scratch = Vec::new();
        match self {
            NodeView::Internal { page } => {
                let key = page.get_key_at(i, &mut scratch)?;
                Ok(key.to_vec())
            }
            NodeView::Leaf { page } => {
                let key = page.get_key_at(i, &mut scratch)?;
                Ok(key.to_vec())
            }
        }
    }

    /// Get the first key in the node
    #[inline]
    pub fn first_key(&self) -> Result<Vec<u8>> {
        self.key_at(0)
    }

    /// Find the insertion index for a given key. This is using a comparator from the key format
    pub fn lower_bound(&self, probe: &[u8]) -> Result<usize, usize> {
        match self {
            NodeView::Internal { page } => {
                let mut scratch = Vec::new();
                page.lower_bound(probe, &mut scratch)
            }
            NodeView::Leaf { page } => {
                let mut scratch = Vec::new();
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
            NodeView::Internal { page } => {
                let mut scratch = Vec::new();
                page.lower_bound_cmp(probe, &mut scratch, cmp)
            }
            NodeView::Leaf { page } => {
                let mut scratch = Vec::new();
                page.lower_bound_cmp(probe, &mut scratch, cmp)
            }
        }
    }

    /// Insert a key-value pair or key-child pointer into the node
    pub fn insert(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        child_ptr: Option<u64>,
    ) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                if let Some(ptr) = child_ptr {
                    page.insert_encoded(key, ptr)
                        .map_err(|e| anyhow::anyhow!(e))
                } else {
                    Err(anyhow::anyhow!(
                        "Internal nodes require a child pointer for insertion"
                    ))
                }
            }
            NodeView::Leaf { page } => {
                if let Some(val) = value {
                    page.insert_encoded(key, val)
                        .map_err(|e| anyhow::anyhow!(e))
                } else {
                    Err(anyhow::anyhow!("Leaf nodes require a value for insertion"))
                }
            }
        }
    }

    /// Insert a key-value pair into a leaf node for a given index
    #[inline]
    pub fn insert_at(&mut self, idx: usize, key: &[u8], value: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { .. } => Err(anyhow::anyhow!(
                "Internal nodes do not store values, cannot insert"
            )),
            NodeView::Leaf { page } => page
                .insert_at(idx, key, value)
                .map_err(|e| anyhow::anyhow!(e)),
        }
    }

    #[inline]
    pub  fn insert_separator_at(&mut self, idx: usize, key: &[u8], child_ptr: u64) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Leaf { .. } => Err(anyhow::anyhow!(
                "Leaf nodes do not have children, cannot insert separator"
            )),
            NodeView::Internal { page } => page
                .insert_separator(idx, key, child_ptr)
                .map_err(|e| anyhow::anyhow!(e)),
        }
    }

    /// Replace the value at a given index in a leaf node
    #[inline]
    pub fn replace_at(&mut self, idx: usize, value: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { .. } => Err(anyhow::anyhow!(
                "Internal nodes do not store values, cannot replace"
            )),
            NodeView::Leaf { page } => page
                .overwrite_value_at(idx, value)
                .map_err(|e| anyhow::anyhow!(e)),
        }
    }

    /// Remove the entry at a given index from the node
    #[inline]
    pub fn delete_at(&mut self, idx: usize) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { .. } => Err(anyhow::anyhow!(
                "Internal nodes do not store values, cannot replace"
            )),
            NodeView::Leaf { page } => page.delete_at(idx).map_err(|e| anyhow::anyhow!(e)),
        }
    }

    /// Get the entry count of the node
    #[inline]
    pub fn entry_count(&self) -> usize {
        match self {
            NodeView::Internal { page } => page.key_count() as usize,
            NodeView::Leaf { page } => page.key_count() as usize,
        }
    }

    /// Split the node into two, returning the new node and the split key
    pub fn split_off(&mut self, idx: usize) -> Result<NodeView, anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                //println!("Splitting internal node at index {} with format id {}", idx, page.fmt().format_id() );
                let mut new_page = InternalPage::new(page.fmt().format_id());
                page.split_off_into(idx, &mut new_page).map_err(|e| anyhow::anyhow!(e))?;
                Ok(NodeView::Internal { page: new_page })
            }
            NodeView::Leaf { page } => {
                let mut new_page = LeafPage::new(page.fmt().format_id());
                page.split_off_into(idx, &mut new_page)
                    .map_err(|e| anyhow::anyhow!(e))?;
                Ok(NodeView::Leaf { page: new_page })
            }
        }
    }

    /// Replace the child pointer at a given index in an internal node
    pub fn replace_child_at(&mut self, idx: usize, child_ptr: u64) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                let child_idx = idx;
                page.replace_child_at(child_idx, child_ptr)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            NodeView::Leaf { .. } => Err(anyhow::anyhow!(
                "Leaf nodes do not have children to replace"
            )),
        }
    }

    /// Pop the last key from an internal node. This is  used during splits.
    pub fn pop_key(&mut self) -> Result<Option<Vec<u8>>, anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                let mut scratch = Vec::new();
                if page.key_count() == 0 {
                    return Ok(None);
                }
                let key = page.pop_last_key(&mut scratch)?;
                Ok(Some(key))
            }
            NodeView::Leaf { .. } => Err(anyhow::anyhow!(
                "Leaf nodes do not have children to replace"
            )),
        }
    }

    /// Write the leftmost child pointer of an internal node
    pub fn write_leftmost_child(&mut self, ptr: u64) -> Result<(), anyhow::Error> {
        match self {
            NodeView::Internal { page } => {
                page.write_leftmost_child(ptr)
                    .map_err(|e| anyhow::anyhow!(e))
            }
            NodeView::Leaf { .. } => Err(anyhow::anyhow!(
                "Leaf nodes do not have children to write"
            )),
        }
    }
}
