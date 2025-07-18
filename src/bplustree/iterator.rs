use crate::bplustree::node::{Node};
use crate::storage::{KeyCodec, ValueCodec, NodeStorage};

pub struct BPlusTreeRangeIter<'a, K, V, S>
    where K: KeyCodec + Ord,
          V: ValueCodec,
          S: NodeStorage<K, V>,
{
    pub(super) storage: &'a mut S,
    pub(super) current_id: Option<u64>,
    pub(super) index: usize,
    pub(super) start: K,
    pub(super) end: K,
    pub phantom: std::marker::PhantomData<(K, V)>,
}

// Implementation of the BPlusTreeRangeIter
impl<'a, K: Ord + KeyCodec, V: ValueCodec, S> Iterator for BPlusTreeRangeIter<'a, K, V, S> 
    where S: NodeStorage<K, V>,
            K: Clone + Ord,
            V: Clone,
{
    type Item = Result<(K, V), anyhow::Error>;

    // Returns the next item in the iteration, it returns a deep copy value of the Key and Value pair if it is within the range
    fn next(&mut self) ->Option<Self::Item> {
        while let Some(node_id) = &self.current_id {
            let mut node = match self.storage.read_node(*node_id) {
                Ok(node) => node,
                Err(e) => return Some(Err(e)), // If we can't read the node, we stop iterating
            };

            match node.take() {
                Some(Node::Leaf { keys, values, next }) => {
                    // If the node is a leaf, we can iterate over its keys and values
                    while self.index < keys.len() {
                        let key = &keys[self.index];
                        if *key >= self.end { // Stop if the key is beyond the end of the range
                            return None;
                        }
                        self.index += 1;
                        if key >= &self.start { // Return the key-value pair if it is within the
                            // range
                            let val = values[self.index].clone();
                            let res = Ok(((*key).clone() , val));
                            return Some(res);
                        }
                    }
                    self.current_id = next;
                    self.index = 0;
                },
                _ => {
                    // If the node is not a leaf, we should not be here
                    return None;
                }
            }
        }
        None
    }
}
