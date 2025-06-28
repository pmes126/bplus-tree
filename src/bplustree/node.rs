use std::cell::RefCell;
use std::rc::Rc;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type NodeRef<K, V> = Rc<RefCell<Node<K, V, u64>>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Node<K, V, NodeId> {
    Internal {
        keys: Vec<K>,
        children: Vec<NodeId>,
    },
    Leaf {
        keys: Vec<K>,
        values: Vec<V>,
        next: Option<NodeId>,
    },
}

impl<K, V, NodeId> Node<K, V, NodeId> {
    pub fn as_leaf_mut(&mut self) -> Option<&mut Node<K, V, NodeId>> {
        match self {
            Node::Leaf { .. } => Some(self),
            _ => None,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Internal { keys, children } => keys.is_empty() && children.is_empty(),
            Node::Leaf { keys, values, next: _ } => keys.is_empty() && values.is_empty(),
        }
    }
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf { .. })
    }
    pub fn is_internal(&self) -> bool {
        matches!(self, Node::Internal { .. })
    }
}
