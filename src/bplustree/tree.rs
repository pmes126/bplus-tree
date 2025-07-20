use crate::bplustree::{Node, TreeError};
use crate::storage::ValueCodec;
use crate::storage::KeyCodec;
use crate::storage::{NodeStorage, MetadataStorage, metadata, metadata::{METADATA_PAGE_1, METADATA_PAGE_2}};
use crate::bplustree::BPlusTreeRangeIter;
use anyhow::Result;

pub type NodeId = u64; // Type for node IDs

/// Result of inserting into a B+ tree node
pub enum InsertResult<K, N> {
    /// Node was updated in-place (no split)
    Updated(N),
    /// Node was split, promote this key to the parent
    Split {
        left: NodeId,        // Left half (including inserted key)
        right: NodeId,       // Right half
        split_key: K,        // First key of right node, to push into parent
    },
    /// Insert was skipped (e.g., duplicate key policy)
    Unchanged,
}

pub enum SplitResult<K, N> {
    SplitNodes {
        left_node: N,        // Left half (including inserted key)
        right_node: N,       // Right half
        split_key: K,        // First key of right node, to push into parent
    },
}

#[derive(Debug)]
pub struct BPlusTree<K, V, S: NodeStorage<K, V>> 
where
    K: KeyCodec + Ord,
    V: ValueCodec,
    {
    root_id: NodeId,
    order: usize,
    max_keys: usize,
    min_keys: usize,
    storage: S,
    height: usize, // Height of the B+ tree
    phantom: std::marker::PhantomData<(K, V)>,
}

// BPlusTree implementation
impl<K, V, S> BPlusTree<K, V, S>
where
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
    S: NodeStorage<K, V> + MetadataStorage,
{
    pub fn new(mut storage: S, order: usize) -> Result<BPlusTree<K, V, S>, TreeError> {
        let root_node = Node::Leaf {
            keys: vec![],
            values: vec![],
            next: None,
        };
        if order < 2 {
            return Err(TreeError::BadInput(
                "Order must be at least 2".to_string()
            ));
        } 
        // Initialize the root node ID
        let init_id = storage.write_node(&root_node).map_err(|e| TreeError::BackendAny(e.to_string()))?;
        println!("Initialized root node with ID: {}", init_id);
        let metadata_1 = metadata::new_metadata_page(
            init_id,
            1, // Initial transaction ID
            0, // Placeholder for checksum
            order as u8,
        );
        let metadata_2 = metadata::new_metadata_page(
            init_id,
            0, // Initial transaction ID
            0, // Placeholder for checksum
            order as u8,
        );
        storage.write_meta(METADATA_PAGE_1, &metadata_1)?;
        storage.write_meta(METADATA_PAGE_2, &metadata_2)?;

        Ok(Self {
            root_id: init_id,
            storage,
            order,
            max_keys: order - 1,
            min_keys: (order + 1).saturating_div(2), // Ensure min_keys is at least 1
            height: 1, // Start with height 1 for the root node
            phantom: std::marker::PhantomData,
        })
    }

    pub fn load(mut storage: S) -> Result<BPlusTree<K, V, S>, TreeError> {
        let md = storage.get_metadata()?;
        let root_id = md.root_node_id;
        let order = md.order as usize;
        
        let max_keys = order - 1;
        let min_keys = (order + 1).saturating_div(2); // Ensure min_keys is at least 1

        Ok(Self {
            root_id,
            storage,
            order,
            max_keys,
            min_keys,
            height: 1, //TODO: Load the height from metadata
            phantom: std::marker::PhantomData,
        })
    }

    // Reads a node from the B+ tree storage, using the cache if available.
    fn read_node(&mut self, id: NodeId) -> Result<Option<Node<K, V>>> {
        self.storage.read_node(id)
    }

    // Writes a node to the B+ tree storage and updates the cache.
    fn write_node(&mut self, node: &Node<K, V>) -> Result<u64> {
        self.storage.write_node(node)
    }

    // Gets the value associated with a key in the B+ tree.
    fn get(&mut self, key: &K) -> Result<Option<V>> {
        let mut id = self.root_id;
        loop {
            let node = self.read_node(id)?;
            match node {
                Some(Node::Leaf { keys, values, .. }) => {
                            match keys.binary_search(&key) {
                                Ok(i) =>  return Ok(Some(values[i].clone())),
                                Err(_) => return Ok(None), // Key not found
                            };
                }
                Some(Node::Internal { keys, children }) => {
                            let idx = match keys.binary_search(&key) {
                                Ok(i) => i,
                                Err(_) => return Ok(None), // Key not found
                            };
                    id = children[idx];
                }
                None => return Ok(None), // Node not found
            }
        }
    }

    // Gets the insertion path for a key, returning the path and the leaf node where the key
    // should be inserted.
    pub fn get_insertion_path(&mut self, key: &K) -> Result<(Vec<(NodeId, usize)>, Node<K, V>)> {
        let mut path = vec![];
        let mut current_id = self.root_id;

        // Find insertion point
        loop {
            let mut node = self.read_node(current_id)?;
            match node.take() {
                Some(node_res) => match &node_res {
                    Node::Leaf { .. } => {
                        return Ok((path, node_res)); // Found the leaf node
                    }
                    Node::Internal { keys, children } => {
                        let i = match keys.binary_search(key) {
                            Ok(i) => i,
                            Err(i) => i,
                        };
                        path.push((current_id, i));
                        current_id = children[i];
                    }
                },
                None => {
                    // Node not found, this should not happen as we are traversing the path
                   return Err(TreeError::BackendAny(
                       "Node not found while getting insertion path".to_string(),
                   ).into());
                }
            }
        }
   }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        // Get the insertion path and the leaf node ID where the key should be inserted
        let (path, mut leaf_node) = self.get_insertion_path(&key)?;
        // We have found the leaf node, update a copy of the leaf node and insert it back with a
        // new id retaining COW semantics.
        match &mut leaf_node {
             Node::Leaf { keys, values, .. } => {
                 // If the key already exists, we replace the value
                match keys.binary_search(&key) {
                    Ok(i) => {
                        values[i] = value; // Replace existing value
                    }
                    Err(i) => {
                        keys.insert(i, key.clone());
                        values.insert(i, value);
                    }
                }
                // Check if the leaf node is overflowed
                if keys.len() > self.max_keys {
                    let split_res = self.split_leaf_node(leaf_node)?;
                    match split_res {
                        SplitResult::SplitNodes { left_node, right_node, split_key } => {
                            // We have a split, we need to insert the new leaf node into the parent
                            let new_leaf_id = self.write_node(&right_node)?;
                            let updated_node_id = self.write_node(&left_node)?;
                            let node_split = InsertResult::Split {
                                left: updated_node_id,
                                right: new_leaf_id,
                                split_key: split_key.clone(),
                            };
                            // Propagate the split upwards.
                            self.propagate_split(path, node_split)?;
                        }
                    }
                } else { // Insert and update path.
                    let new_leaf_id = self.write_node(&leaf_node)?;
                    if path.is_empty() {
                        // If the path is empty, we are inserting into the root node
                        self.root_id = new_leaf_id;
                    } else {
                        // Otherwise, we need to propagate the update to the parent nodes
                        self.propagate_node_update(path, new_leaf_id)?;
                    }
                }
             },
             _ => {
                // If the node is not a leaf, this should not happen
                return Err(TreeError::BackendAny(
                    "Expected a leaf node for insertion".to_string(),
                ).into());
             }
         }
        Ok(())
    }

    // Splits a leaf node into two nodes and returns the new right node, the left node, and the
    // first key of the right node to be pushed up to the parent.
    fn split_leaf_node (
        &mut self,
        mut leaf_node: Node<K, V>,
    ) -> Result<SplitResult<K, Node<K, V>>> {
        if let Node::Leaf { keys, values, next } = &mut leaf_node {
            let mid = keys.len() / 2;
            let right_keys = keys.split_off(mid);
            let right_values = values.split_off(mid);
            let split_key = right_keys.first().ok_or_else(|| { TreeError::BackendAny("Leaf node has no keys to split".to_string()) })?;
            let right_leaf = Node::Leaf {
                keys: right_keys.to_vec(),
                values: right_values,
                next: next.take(), // Retain the next pointer
            };
            let left_leaf = Node::Leaf {
                keys: keys.to_vec(),
                values: values.to_vec(),
                next: Some(self.write_node(&right_leaf)?), // Link to the new right leaf
            };
            Ok( SplitResult::SplitNodes { right_node: right_leaf, left_node: left_leaf, split_key: split_key.clone()})
        } else {
            Err(TreeError::BackendAny(
                "Expected a leaf node for splitting".to_string(),
            ).into())
        }
    }

    fn split_internal_node(
        &mut self,
        mut internal_node: Node<K, V>,
    ) -> Result<SplitResult<K, Node<K, V>>> {
        if let Node::Internal { keys, children } = &mut internal_node {
            let mid = keys.len() / 2;
            let right_keys = keys.split_off(mid + 1);
            let right_children = children.split_off(mid + 1);
            let split_key = right_keys.first().ok_or_else(|| { TreeError::BackendAny("Internal node has no keys to split".to_string()) })?;
            let right_internal = Node::Internal {
                keys: right_keys.to_vec(),
                children: right_children,
            };
            let left_internal = Node::Internal {
                keys: keys.to_vec(),
                children: children.to_vec(),
            };
            Ok(SplitResult::SplitNodes { right_node: right_internal, left_node: left_internal, split_key: split_key.clone() })
        } else {
            Err(TreeError::BackendAny(
                "Expected an internal node for splitting".to_string(),
            ).into())
        }
    }

    // Propagate an update to the parent nodes in the path, this is used when we insert into a leaf
    // node and we need to update the parent nodes with the new leaf node ID.
    fn propagate_node_update (
        &mut self,
        mut path: Vec<(u64, usize)>,
        new_node_id: NodeId,
    ) -> Result<()> {
        let mut node_id = new_node_id;
        // We need to update the parent nodes with the new leaf node ID
        while let Some((parent_id, insert_pos)) = path.pop() {
            let mut node = self.read_node(parent_id)?;
            match node.take() { // with take the node belongs to the context below, so we can modify it
                Some(mut node) => match &mut node {
                    Node::Internal { children, .. } => {
                        // Replace the original child id with the new node id
                        children[insert_pos] = node_id;
                        // If there is no overflow we can just write the node and return
                        node_id = self.write_node(&node)?;
                        if parent_id == self.root_id {
                            // If we are at the root update the root ID
                            self.root_id = node_id;
                            return Ok(());
                        }
                    }
                    Node::Leaf { .. } => {
                        // We should never reach a leaf node here, as we are updating parent nodes
                        return Err(TreeError::BackendAny(
                            "Reached a leaf node while trying to insert into parent".to_string(),
                        ).into());
                    
                    }
                },
                None => {
                    // Node not found, this should not happen as we are traversing a path in the
                    // tree
                    return Err(TreeError::NodeNotFound(
                     "Node not found while inserting into parent".to_string(),
                    ).into());
                }
            }
        }
        Ok(())
    }

    // insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn propagate_split (
        &mut self,
        mut path: Vec<(u64, usize)>,
        node_split: InsertResult<K, Node<K, V>>,
    ) -> Result<()> {
        if let InsertResult::Split { left, right, split_key } = node_split {
            let mut left = left;
            let mut right = right;
            let mut key = split_key.clone();
            // For each parent node in the path, we need to insert the split key and the new nodes
            while let Some((parent_id, insert_pos)) = path.pop() {
                let mut node = self.read_node(parent_id)?;
                match node.take() { // with take the node belongs to the context below, so we can
                    // modify it
                    Some(mut node) => match &mut node {
                        Node::Leaf { .. } => {
                            // We should never reach a leaf node here, as we are inserting into the parent node.
                           return Err(TreeError::BackendAny(
                               "Reached a leaf node while trying to insert into parent".to_string(),
                           ).into());
                        }
                        Node::Internal { keys, children } => {
                            keys.insert(insert_pos, key);
                            // Replace the original child id with the new left child id,
                            children[insert_pos] = left;
                            // insert the split key right after
                            children.insert(insert_pos + 1, right);
                            // if there is no further overflow we can just propagate the update and return
                            if keys.len() <= self.max_keys {
                                let new_node_id = self.write_node(&node)?;
                                if parent_id == self.root_id {
                                    // If we are at the root update the root ID
                                    self.root_id = new_node_id;
                                } else {
                                    // Otherwise propagate the update to the parent
                                    self.propagate_node_update(path, new_node_id)?;
                                }
                                return Ok(())
                            } else {
                                // Node is overflowed, we need to split it
                                let split_res = self.split_internal_node(node)?;
                                match split_res {
                                    SplitResult::SplitNodes { left_node, right_node, split_key } => {
                                        // We have a split, we need to insert the new internal node into the parent
                                        let new_left_id = self.write_node(&left_node)?;
                                        let new_right_id = self.write_node(&right_node)?;
                                        right = new_right_id;
                                        left = new_left_id;
                                        key = split_key.clone();
                                    }
                                }
                                continue;
                            }
                        }
                    },
                    None => {
                        // Node not found, this should not happen as we are traversing the path
                        return Err(TreeError::NodeNotFound(
                         "Node not found while inserting into parent".to_string(),
                        ).into());
                    }
                }
            }
            // If we reach here there have been node splits up to the root need to create a new root node (internal) and increase the height.
            let new_root = Node::Internal {
                keys: vec![key.clone()],
                children: vec![left, right],
            };
            // Write the new root node to storage
            let new_root_id = self.write_node(&new_root)?;
            self.root_id = new_root_id;
            self.height += 1; // Increase the height of the tree
        }

        Ok(())
    }

    // Search for a key and return the value if exists
    pub fn search(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        loop {
            let node = self.read_node(current_id)?;
            match node {
                Some(Node::Internal { keys, children }) => {
                    let i = match keys.binary_search(key) {
                        Ok(i) => i,
                        Err(_i) => return Ok(None), // Key not found
                    };
                    current_id = children[i];
                }
                Some(Node::Leaf { keys, values, .. }) => {
                    match keys.binary_search(key) {
                        Ok(i) => return Ok(Some(values[i].clone())),
                        Err(_i) => return Ok(None), // Key not found
                    };
                }
                None => return Ok(None), // Node not found
            }
        }
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range(&mut self, start: &K, end: &K) -> Result<Option<BPlusTreeRangeIter<K, V, S>>> {
        if start > end {
            return Ok(None); // Invalid range
        }
        let mut current_id = self.root_id;

        loop {
            let node = self.read_node(current_id)?;

            match node {
                Some(Node::Internal { keys, children }) => {
                    let i = match keys.binary_search(start) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current_id = children[i];
                }
                Some(Node::Leaf { keys, .. }) => {
                    // Find the index in the leaf node
                    let start_index = keys.binary_search(start).unwrap_or(
                        keys.len(), // If not found the iterator will skip to the next leaf node
                    );

                    return Ok(Some(BPlusTreeRangeIter {
                        storage: &mut self.storage,
                        current_id: Some(current_id),
                        index: start_index,
                        start: start.clone(),
                        end: end.clone(),
                        phantom: std::marker::PhantomData,
                    }));
                }
                None => return Ok(None), // Node not found
            }
        }
    }

    // Delete and handle underflow of leaf nodes
    pub fn delete(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        // Stack to keep track of parent nodes and the index of the child in the parent
        let mut parent_stack: Vec<(u64, usize)> = vec![];

        loop {
            let node = self.read_node(current_id)?;
            match node {
                Some(Node::Internal { keys, children }) => {
                    let i = match keys.binary_search(key) {
                        Ok(i) => i,
                        Err(_) => return Ok(None), // Key not found
                    };
                    parent_stack.push((current_id, i));
                    current_id = children[i];
                }
                Some(Node::Leaf {mut keys, mut values, .. }) => {
                    match keys.binary_search(key) {
                        Ok(i) => {
                            let ret_val = Some(values[i].clone());
                            keys.remove(i);
                            values.remove(i);
                            // Check if the leaf node is underflowed
                            if keys.len() < self.min_keys && !parent_stack.is_empty() {
                                // Handle underflow by borrowing from the parent or merging
                                //self.handle_leaf_underflow(&mut parent_stack, current_id)?;
                            }
                            return Ok(ret_val)
                        }
                        Err(_i) => {
                            return Ok(None); // Key not found
                        }
                    }
                }
                None => return Ok(None), // Node not found
            }
        }
    }

    // Set the root of the B+ tree
    pub fn set_root(&mut self, root: NodeId) {
        self.root_id = root;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_store::FileStore;
    use crate::storage::page_store::PageStore;

    #[test]
    fn write_and_read_node() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile.bin";
        
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, 3)?;
        let key = 1u64;
        let value = "a".to_string();
        let res = tree_root.insert(key, value.clone());
        assert!(res.is_ok(), "Node should be inserted successfully");
        let res = tree_root.search(&key)?;
        assert!(res.is_some(), "Node should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        Ok(())
    }
    
    #[test]
    fn write_and_read_nodes() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile.bin";
        
        let order = 11; // B+ tree order
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        for i in 0..order - 1 {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
            let res = tree_root.search(&key)?;
            assert!(res.is_some(), "Node should be read successfully");
            assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        }
        Ok(())
    }

    #[test]
    fn write_and_read_nodes_with_overflow() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile.bin";
        
        let order = 2; // B+ tree order
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        for i in 0..order*100 {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
            let res = tree_root.search(&key)?;
            assert!(res.is_some(), "Node should be read successfully");
            assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        }
        Ok(())
    }
}

