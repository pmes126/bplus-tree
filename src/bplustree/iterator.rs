use crate::bplustree::node::{Node, NodeId};
use crate::storage::{KeyCodec, ValueCodec, NodeStorage};
use std::fmt::Debug;

struct TraversalFrame {
    node_id: NodeId,
    index: usize,
}

pub struct BPlusTreeIter<'a, K, V, S>
    where K: KeyCodec + Ord,
          V: ValueCodec,
          S: NodeStorage<K, V>,
{
    pub(super) storage: &'a mut S,
    pub(super) current_id: Option<NodeId>,
    pub current_leaf: Option<Node<K, V>>,
    pub(super) index: usize,
    pub(super) start: K,
    pub(super) end: K,
    pub(super) stack: Vec<TraversalFrame>,
    pub phantom: std::marker::PhantomData<(K, V)>,
}

struct LeafCursor<'a, K, V> {
    node_id: NodeId,
    keys: &'a [K],
    values: &'a [V],
    pos: usize,
}

impl<'a, K: Debug, V: Debug, S> BPlusTreeIter<'a, K, V, S> 
    where S: NodeStorage<K, V>,
            K: KeyCodec + Clone + Ord,
            V: ValueCodec + Clone,
{
    pub fn new(
        storage: &'a mut S,
        root_id: NodeId,
        start: &K,
        end: &K,
    ) -> Self {
        let mut iter = Self {
            storage,
            stack: Vec::new(),
            current_leaf: None,
            start: start.clone(),
            end: end.clone(),
            current_id: None,
            index: 0,
            phantom: std::marker::PhantomData,
        };
        iter.descend_to_leaf(root_id, Some(start));
        iter
    }

    fn descend_to_leaf(&mut self, mut node_id: NodeId, key: Option<&K>) -> Result<(), anyhow::Error> {
        loop {
            let node = self.storage.read_node(node_id)?;

            match node {
                Some(Node::Internal { keys, children }) => {
                    let i = key.map_or(0, |k| match keys.binary_search(k) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    });
                    self.stack.push(TraversalFrame{node_id, index: i});
                    node_id = children[i];
                }
                Some(Node::Leaf { ref keys, .. }) => {
                    let pos = key.map_or(None, |k| {
                        match keys.binary_search(k) {
                            Ok(i) => Some(i),
                            Err(_i) => None, // Key not found
                        }
                    });

                    self.index = pos.unwrap_or(0);
                    self.current_leaf = node.clone();
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
    where S: NodeStorage<K, V>,
          K: KeyCodec + Clone + Ord,
          V: ValueCodec + Clone,
{
    type Item = Result<(K, V), anyhow::Error>;

    // Returns the next item in the iteration, it returns a deep copy value of the Key and Value pair if it is within the range
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(Node::Leaf { keys, values, .. }) = &mut self.current_leaf {
                if self.index < keys.len() {
                    let (k, v) = (&keys[self.index], &values[self.index]);
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
                        if i + 1 < children.len() {
                            let next_node = children[i + 1];
                            self.stack.push(TraversalFrame { node_id: frame.node_id, index: i + 1 });
                            let _ = self.descend_to_leaf(next_node, None);
                            break;
                        } 
                    }
                    Some(Node::Leaf {..}) => { panic!("Invalid frame") }
                    None => {
                        // If we reach here, it means we have traversed all nodes
                        return None;
                    }
                }
            }
        }
    }
}
