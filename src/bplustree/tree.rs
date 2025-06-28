pub use crate::bplustree::Node;
use crate::storage::CachedStorage;
pub use crate::storage::NodeStorage;
pub use iter::BPlusTreeRangeIter;

pub type NodeId = u64; // Type for node IDs

#[derive(Debug)]
pub struct BPlusTree<K, V, S: NodeStorage<K, V>> {
    root: NodeId,
    next_id: NodeId,
    order: usize,
    max_keys: usize,
    min_keys: usize,
    storage: S,
}

// BPlusTree implementation
impl<K, V, S> BPlusTree<K, V, S>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned + Clone,
    S: NodeStorage<K, V>,
{
    pub fn new(mut storage: S, order: usize) -> Self {
        let root_node = Node::Leaf(LeafNode {
            entries: vec![],
            next: None,
        });
        storage.write_node(0, &root_node);
        Self {
            root: 0,
            next_id: 1,
            storage,
            order,
            max_keys: order - 1,
            min_keys: (order + 1) / 2,
        }
    }

    // Reads a node from the B+ tree storage, using the cache if available.
    fn read_node(&mut self, id: NodeId) -> io::Result<Node<K, V>> {
        if let Some(n) = self.storage.read_node(&id) {
            return Ok(n.clone());
        }
        let node = self.storage.read_node(id)?;
        self.cache.put(id, node.clone());
        Ok(node)
    }

    // Writes a node to the B+ tree storage and updates the cache.
    fn write_node(&mut self, id: NodeId, node: Node<K, V>) -> io::Result<()> {
        self.cache.put(id, node.clone());
        self.storage.write_node(id, &node)
    }

    // Gets the value associated with a key in the B+ tree.
    fn get(&mut self, key: &K) -> io::Result<Option<V>> {
        let mut id = self.root;
        loop {
            let node = self.read_node(id)?;
            match node {
                Node::Internal { keys, children } => {
                    let idx = keys.binary_search(key).unwrap_or_else(|x| x);
                    id = children[idx];
                }
                Node::Leaf { keys, values, .. } => {
                    if let Some(i) = keys.iter().position(|k| k == key) {
                        return Ok(Some(values[i].clone()));
                    }
                    return Ok(None);
                }
            }
        }
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) {
        let mut path = vec![];
        let mut current_id = self.root_id;

        // Find insertion point
        loop {
            let node = self.storage.read_node(current_id);
            let node_borrow = node.borrow();
            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    path.push((current_id, i));
                    current_id = children[i];
                }
                Node::Leaf { .. } => break,
            }
        }
        // We have found the leaf node
        let leaf_node = self.storage.read_node(current_id);
        let mut leaf = leaf_node.borrow_mut();
        if let Node::Leaf { keys, values, .. } = &mut *leaf {
            match keys.binary_search(&key) {
                Ok(i) => {
                    values[i] = value; // Replace existing value
                    return;
                }
                Err(i) => {
                    keys.insert(i, key.clone());
                    values.insert(i, value);
                    self.size += 1;
                }
            }
        }
        // Handle overflow
        if let Node::Leaf { keys, values, next } = &mut *leaf {
            if keys.len() > self.max_keys {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let new_leaf = Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    next: next.take(),
                };
                // Write the new leaf node to storage
                //
                let new_leaf_id = self.next_id;
                self.storage.write_node(new_leaf_id, new_leaf);
                *next = Some(new_leaf_id);
                self.next_id += 1;
                // Update the current leaf node to storage
                self.storage.write_node(current_id, leaf);
                // Propagate the split upwards.
                self.insert_into_parent(path, key, new_leaf_id);
            }
        }
    }
    // insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn insert_into_parent(
        &mut self,
        mut path: Vec<(u64, usize)>,
        mut key: K,
        mut new_child_id: u64,
    ) {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let node = self.storage.read_node(parent_id);
            let mut node_borrow = node.borrow_mut();
            if let Node::Internal { keys, children } = &mut *node_borrow {
                keys.insert(insert_pos, key.clone());
                children.insert(insert_pos + 1, new_child_id);
                self.storage.write_node(parent_id, node_borrow);

                if keys.len() <= self.max_keys {
                    return;
                }

                // Node is overflowed, we need to split it
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                let split_key_for_parent = keys.pop().unwrap();

                let new_internal = Node::Internal {
                    keys: right_keys,
                    children: right_children,
                };
                let new_internal_id = self.next_id;
                // Write the new internal node to storage
                let new_internal_id = self.storage.write_node(new_internal_id, new_internal);
                self.next_id += 1;
                // Update the current node to storage
                self.storage.write_node(parent_id, node_borrow);

                key = split_key_for_parent;
                new_child_id = new_internal_id;
                continue;
            }
        }

        let old_root = self.root_id;
        let new_root = Node::Internal {
            keys: vec![key],
            children: vec![old_root, new_child_id],
        };
        // Write the new root node to storage
        self.storage.write_node(self.next_id, new_root);
        self.next_id += 1;
        self.height += 1;
    }

    // Returns true if the B+ tree is empty.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    // Returns the number of keys in the B+ tree.
    pub fn len(&self) -> usize {
        self.size
    }

    // Returns the height of the B+ tree.
    pub fn height(&self) -> usize {
        self.height
    }

    // Search for a key and return the value if exists
    pub fn search(&self, key: &K) -> Option<V> {
        let mut current_id = self.root_id;
        loop {
            let node = self.storage.load(current_id);
            let node = node.borrow();
            match &*node {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current_id = children[i];
                }
                Node::Leaf { keys, values, .. } => {
                    match keys.binary_search(&key) {
                        Ok(i) => return Some(values[i].clone()),
                        Err(_i) => return None, // Key not found
                    };
                }
            }
        }
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range(&self, start: &K, end: &K) -> Option<impl Iterator<Item = (K, V)>> {
        if start > end {
            return None; // Invalid range
        }
        let mut current_id = self.root_id.clone();

        loop {
            let node = self.storage.load(current_id);
            let node_borrow = node.borrow();

            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&start) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current_id = children[i];
                }
                Node::Leaf { keys, .. } => {
                    // Find the index in the leaf node
                    let start_index = keys.binary_search(&start).unwrap_or(
                        keys.len(), // If not found the iterator will skip to the next leaf node
                    );

                    return Some(BPlusTreeRangeIter {
                        storage: &self.storage,
                        current_id: Some(current_id),
                        index: start_index,
                        start: start.clone(),
                        end: end.clone(),
                    });
                }
            }
        }
    }

    // Delete and handle underflow of leaf nodes
    pub fn delete(&mut self, key: &K) -> Option<V> {
        let mut current_id = self.root_id;
        let mut parent_stack: Vec<(u64, usize)> = vec![];

        loop {
            let node = self.storage.load(current_id);
            let node_borrow = node.borrow();
            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    parent_stack.push((current_id, i));
                    current_id = children[i];
                }
                Node::Leaf { .. } => {
                    break; // Found the leaf node
                }
            }
        }
        // We have found the leaf node
        // Need to borrow the node mutably and re-match to leaf to remove the key
        let node = self.storage.load(current_id);
        let mut node_mut = node.borrow_mut();
        let mut ret_val: Option<V> = None;
        if let Node::Leaf { keys, values, .. } = &mut *node_mut {
            match keys.binary_search(&key) {
                Ok(i) => {
                    keys.remove(i);
                    ret_val = Some(values.remove(i));
                    self.storage.delete_node(current_id);
                    self.size -= 1;
                }
                Err(_i) => {}
            }
            // Check if the leaf node is underflowed
            if keys.len() < MIN_KEYS && !parent_stack.is_empty() {
                self.handle_leaf_underflow(&mut parent_stack, current_id);
            }
        }
        return ret_val;
    }

    // Handle underflow of leaf nodes
    fn handle_leaf_underflow(&mut self, parent_stack: &mut Vec<(u64, usize)>, leaf_id: u64) {
        // If the leaf node is underflowed, we need to either merge or borrow from a sibling
        while let Some((parent_id, index_in_parent)) = parent_stack.pop() {
            let parent = self.storage.load(parent_id);
            let mut parent_mut = parent.borrow_mut();

            if let Node::Internal { keys, children } = &mut *parent_mut {
                let child = self.storage.load(leaf_id);
                let underflowed = match &*child.borrow() {
                    Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys.len() < DEGREE,
                };

                if !underflowed {
                    return;
                }

                let sibling_index = if index_in_parent > 0 {
                    index_in_parent - 1
                } else {
                    index_in_parent + 1
                };

                if let Some(&sibling_id) = children.get(sibling_index) {
                    let sibling = self.storage.read_node(sibling_id);
                    let mut sibling_mut = sibling.borrow_mut();
                    let mut child_mut = child.borrow_mut();

                    match (&mut *child_mut, &mut *sibling_mut) {
                        (
                            Node::Leaf {
                                keys: ck,
                                values: cv,
                                next: cn,
                            },
                            Node::Leaf {
                                keys: sk,
                                values: sv,
                                ..
                            },
                        ) => {
                            if sk.len() > self.min_keys {
                                if index_in_parent < sibling_index {
                                    ck.push(sk.remove(0));
                                    cv.push(sv.remove(0));
                                    keys[index_in_parent] = ck[0].clone();
                                } else {
                                    ck.insert(0, sk.pop().unwrap());
                                    cv.insert(0, sv.pop().unwrap());
                                    keys[sibling_index] = sk.last().unwrap().clone();
                                }
                                return;
                            } else {
                                if index_in_parent < sibling_index {
                                    ck.extend(sk.drain(..));
                                    cv.extend(sv.drain(..));
                                    *cn = sibling_id;
                                    children.remove(sibling_index);
                                    keys.remove(index_in_parent);
                                } else {
                                    sk.extend(ck.drain(..));
                                    sv.extend(cv.drain(..));
                                    children.remove(index_in_parent);
                                    keys.remove(sibling_index);
                                }
                            }
                        }
                        (
                            Node::Internal {
                                keys: ck,
                                children: cc,
                            },
                            Node::Internal {
                                keys: sk,
                                children: sc,
                            },
                        ) => {
                            if sk.len() > self.min_keys {
                                if index_in_parent < sibling_index {
                                    ck.insert(0, keys[index_in_parent].clone());
                                    cc.insert(0, sc.pop().unwrap());
                                    keys[index_in_parent] = sk.pop().unwrap();
                                } else {
                                    ck.push(keys[sibling_index].clone());
                                    cc.push(sc.remove(0));
                                    keys[sibling_index] = sk.remove(0);
                                }
                                return;
                            } else {
                                if index_in_parent < sibling_index {
                                    ck.push(keys.remove(index_in_parent));
                                    ck.extend(sk.drain(..));
                                    cc.extend(sc.drain(..));
                                    children.remove(sibling_index);
                                } else {
                                    sk.insert(0, keys.remove(sibling_index));
                                    sk.splice(0..0, ck.drain(..));
                                    sc.splice(0..0, cc.drain(..));
                                    children.remove(index_in_parent);
                                }
                            }
                        }
                        _ => {}
                    }
                }

                if children.len() == 1 {
                    self.root_id = children[0];
                    return;
                }

                child_id = parent_id;
            }
        }
    }

    /// Clears the B+ tree, removing all keys and values.
    pub fn clear(&mut self) {
        self.root_id = 0;
        self.leaf_count = 0;
        self.height = 0;
        self.size = 0;
        self.storage.clear(); // Clear the storage
    }
}
