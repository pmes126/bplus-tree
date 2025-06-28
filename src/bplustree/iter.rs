use crate::node::{Node};
use crate::storage::NodeStorage;

pub struct BPlusTreeRangeIter<'a, K, V> {
    pub(super) storage: &'a InMemoryStorage<K, V>,
    pub(super) current_id: Option<u64>,
    pub(super) index: usize,
    pub(super) start: K,
    pub(super) end: K,
}

// Implementation of the BPlusTreeRangeIter
impl<'a, K: Ord + Clone, V: Clone> Iterator for BPlusTreeRangeIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = &self.current_id {
            let node = self.storage.load(*node);
            let node_borrow = node.borrow();
            match &*node_borrow {
                Node::Leaf { keys, values, next } => {
                    while self.index < keys.len() {
                        let key = &keys[self.index];
                        if *key >= self.end { // Stop if the key is beyond the end of the range
                            return None;
                        }
                        let val = &values[self.index];
                        self.index += 1;
                        if key >= &self.start { // Return the key-value pair if it is within the
                            // range
                            return Some((key.clone(), val.clone()));
                        }
                    }
                    self.current_id = *next;
                    self.index = 0;
                }
                _ => return None,
            }
        }
        None
    }
}
