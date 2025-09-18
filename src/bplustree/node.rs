use crate::bplustree::node_view::NodeView;
use crate::codec::{NodeCodec, bincode::DefaultNodeCodec};

use std::fmt::Debug;

pub type NodeId = u64;

/// In-memory representation of a node
#[derive(Debug, Clone)]
pub enum Node<K, V> {
    Internal {
        keys: Vec<K>,          // Sorted n keys
        children: Vec<NodeId>, // n+1 children
    },
    Leaf {
        keys: Vec<K>,
        values: Vec<V>,
    },
}

impl<K, V> Node<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Internal { keys, children } => keys.is_empty() && children.is_empty(),
            Node::Leaf { keys, values } => keys.is_empty() && values.is_empty(),
        }
    }

    pub fn is_underflowed(&self, min_keys: usize) -> bool {
        match self {
            Node::Internal { keys, .. } => keys.len() < min_keys,
            Node::Leaf { keys, .. } => keys.len() < min_keys,
        }
    }

    pub fn get_keys(&self) -> &[K] {
        match self {
            Node::Internal { keys, .. } => keys,
            Node::Leaf { keys, .. } => keys,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf { .. })
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, Node::Internal { .. })
    }

    pub fn from_node_view<KC, VC>(node_view: NodeView) -> Result<Self, crate::codec::CodecError>
    where
        KC: crate::codec::KeyCodec<K>,
        VC: crate::codec::ValueCodec<V>,
    {
        match node_view {
            NodeView::Internal { page } => {
                let page_raw = page
                    .to_bytes()
                    .map_err(|e| crate::codec::CodecError::EncodeFailure { msg: e.to_string() })?;
                <DefaultNodeCodec<KC, VC> as NodeCodec<K, V>>::decode(page_raw)
            }
            NodeView::Leaf { page } => {
                let page_raw = page
                    .to_bytes()
                    .map_err(|e| crate::codec::CodecError::EncodeFailure { msg: e.to_string() })?;
                <DefaultNodeCodec<KC, VC> as NodeCodec<K, V>>::decode(page_raw)
            }
        }
    }
}
