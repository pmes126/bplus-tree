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
        next: Option<NodeId>, // for in order traversal
    },
}


impl<K, V> Node<K, V>
where 
    K: Ord,
    V: Clone,
{
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Internal { keys, children } => keys.is_empty() && children.is_empty(),
            Node::Leaf { keys, values, next: _ } => keys.is_empty() && values.is_empty(),
        }
    }
    
    pub fn is_underflowed(&self, min_keys: usize) -> bool {
        match self {
            Node::Internal { keys, children } => keys.len() < min_keys,
            Node::Leaf { keys, values, next: _ } => keys.len() < min_keys,
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
}
