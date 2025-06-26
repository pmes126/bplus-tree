use crate::node::Node;

pub mod iter;
pub use crate::storage::in_memory::InMemoryStorage;
pub use iter::BPlusTreeRangeIter;

static DEGREE: usize = 4; // B+ tree degree

#[derive(Debug)]
pub struct BPlusTree<K: Ord + Clone + Default, V: Clone> {
    root_id: u64, // ID of the root node
    leaf_count: usize, // number of leaf nodes
    height: usize, // height of the tree
    size: usize, // number of keys in the tree
    storage: InMemoryStorage<K, V>, // storage for nodes
}

// BPlusTree implementation
impl<K: Ord + Clone + Default, V: Clone> BPlusTree<K, V> {
    pub fn new(root_node: Node<K, V, u64>) -> Self {
        let mut storage = InMemoryStorage::new();
        let root_id = storage.allocate(root_node);
        Self {
            root_id,
            leaf_count: 1,
            height: 1,
            size: 1,
            storage,
        }
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) {
        let mut path = vec![];
        let mut current_id = self.root_id;

        // Find insertion point
        loop {
            let node = self.storage.load(current_id);
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
        let leaf_node = self.storage.load(current_id);
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
                }
            } 
        }
        // Handle overflow
        if let Node::Leaf { keys, values, next } = &mut *leaf {
            if keys.len() > DEGREE {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let new_leaf = Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    next: next.take(),
                };
                let new_leaf_id = self.storage.allocate(new_leaf);
                *next = Some(new_leaf_id);
                drop(leaf);

                self.insert_into_parent(path, key, new_leaf_id);
            }
        }
    }
    // insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn insert_into_parent(&mut self, mut path: Vec<(u64, usize)>, mut key: K, mut new_child_id: u64) {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let node = self.storage.load(parent_id);
            let mut node_borrow = node.borrow_mut();
            if let Node::Internal { keys, children } = &mut *node_borrow {
                keys.insert(insert_pos, key.clone());
                children.insert(insert_pos + 1, new_child_id);

                if keys.len() <= DEGREE {
                    return;
                }

                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                let split_key_for_parent = keys.pop().unwrap();

                let new_internal = Node::Internal {
                    keys: right_keys,
                    children: right_children,
                };
                let new_internal_id = self.storage.allocate(new_internal);

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
        self.root_id = self.storage.allocate(new_root);
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
                Node::Leaf { keys, values, .. } => {
                    drop(node_borrow);
                    let mut node_mut = node.borrow_mut();
                    if let Node::Leaf { keys, values, .. } = &mut *node_mut {
                        match keys.binary_search(&key) {
                            Ok(i) => {
                                keys.remove(i);
                                return Some(values.remove(i));
                            }
                            Err(_i) => {
                                return None;
                            }
                        } 
                    }
                    break;
                }
            }
        }
        // Need to also handle underflow
        None
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
