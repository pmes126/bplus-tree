use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::bplustree::epoch::COMMIT_COUNT;
use crate::bplustree::{Node, TreeError};
use crate::bplustree::BPlusTreeRangeIter;
use crate::bplustree::EpochManager;
use crate::storage::ValueCodec;
use crate::storage::KeyCodec;
use crate::storage::{NodeStorage, MetadataStorage, metadata, metadata::{METADATA_PAGE_1, METADATA_PAGE_2, Metadata}};
use anyhow::Result;

pub type NodeId = u64; // Type for node IDs
pub type PathNode = (NodeId, usize); // Type for path nodes (node ID and index in parent)


fn print_vec<T: std::fmt::Debug>(vec: &Vec<T>, msg: &str) {
    println!("{}: {:?}", msg, vec);
}

/// Result of inserting into a B+ tree node
pub enum InsertResult<N> {
    /// Node was updated in-place (no split)
    Updated(N),
    /// Insert was skipped (e.g., duplicate key policy)
    Unchanged,
}

/// Result of deleting from a B+ tree node
pub enum DeleteResult {
    /// Node was updated in-place (no underflow)
    Updated,
    /// Node was merged with a sibling
    Merged {
        left: NodeId,        // Left half (including deleted key)
        right: NodeId,       // Right half
    },
    /// Node was underflowed and needs to be handled
    Underflowed,
    /// Key was not found in the node
    NotFound,
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
    size: usize,
    max_keys: usize,
    min_internal_keys: usize,
    min_leaf_keys: usize,
    storage: S,
    height: usize, // Height of the B+ tree
    staged_root_id: Option<NodeId>, // Staged root ID for transactions
    staged_height: Option<usize>, // Staged height for transactions
    epoch_mgr: Arc<EpochManager>, // Epoch manager for transaction management
    commit_count: AtomicUsize, // Number of commits made to the tree
    txn_id: u64, // Slot of metadata storage
    // Phantom data to hold the types of keys and values
    phantom: std::marker::PhantomData<(K, V)>,


}

// BPlusTree implementation
impl<K: Debug, V: Debug, S> BPlusTree<K, V, S>
where
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
    S: NodeStorage<K, V> + MetadataStorage,
{
    pub fn new(mut storage: S, order: usize) -> Result<BPlusTree<K, V, S>, TreeError> {
        let root_node = Node::Leaf {
            keys: Vec::with_capacity(order),
            values: Vec::with_capacity(order),
            next: None,
        };
        if order < 2 {
            return Err(TreeError::BadInput(
                "Order must be at least 2".to_string()
            ));
        } 
        // Initialize the root node ID
        let init_id = storage.write_node(&root_node).map_err(|e| TreeError::BackendAny(e.to_string()))?;
        let init_txn_id = 1; // Initial transaction ID
        let mut metadata_1 = metadata::new_metadata_page(
            init_id,
            init_txn_id, // Initial transaction ID
            0, // Placeholder for checksum
            1, // Initial height 
            order,
            0,
        );
        let mut metadata_2 = metadata::new_metadata_page(
            init_id,
            init_txn_id, // Initial transaction ID
            0, // Placeholder for checksum
            1, // Initial height 
            order,
            0,
        );
        storage.write_metadata(METADATA_PAGE_1, &mut metadata_1)?;
        storage.write_metadata(METADATA_PAGE_2, &mut metadata_2)?;

        Ok(Self {
            root_id: init_id,
            storage,
            order,
            size: 0, // Initial size is 0
            max_keys: order - 1,
            min_internal_keys: order.div_ceil(2) - 1, // Ensure min_internal_keys is at
            // least 2
            min_leaf_keys: (order-1).div_ceil(2), // Ensure min_keys is at least 1
            height: 1, // Start with height 1 for the root node
            staged_root_id: None, // No staged root initially
            staged_height: None, // No staged height initially
            epoch_mgr: EpochManager::new_shared(), // Initialize the epoch manager
            commit_count: AtomicUsize::new(0), // Initialize commit count
            txn_id: init_txn_id,     // Initialize transaction ID
            phantom: std::marker::PhantomData,
        })
    }

    pub fn load(mut storage: S) -> Result<BPlusTree<K, V, S>, TreeError> {
        println!("Loading B+ tree with root ID from storage");
        let md = storage.get_metadata()?;
        let root_id = md.root_node_id;
        let order = md.order;
        let size = md.size;
        
        let max_keys = order - 1;
        let min_internal_keys = (order - 1).saturating_div(2); // Ensure min_internal_keys is at 
        let min_leaf_keys = order.saturating_div(2); // Ensure min_keys is at least 1

        Ok(Self {
            root_id,
            storage,
            order,
            size,
            max_keys,
            min_internal_keys,
            min_leaf_keys,
            height: 1, //TODO: Load the height from metadata
            staged_root_id: None, // No staged root initially
            staged_height: None, // No staged height initially
            epoch_mgr: EpochManager::new_shared(), // Initialize the epoch manager
            commit_count: AtomicUsize::new(0), // Initialize commit count
            txn_id: md.txn_id, // Initialize transaction ID from metadata
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

    // Returns the path of where a key should be inserted.
    pub fn get_insertion_path(&mut self, key: &K) -> Result<(Vec<PathNode>, Node<K, V>)> {
        let mut path = vec![];
        let mut current_id = self.root_id;

        // Find insertion point
        loop {
            match self.read_node(current_id)? {
                Some(node_res) => match &node_res {
                    Node::Leaf { .. } => {
                        return Ok((path, node_res)); // Found the leaf node
                    }
                    Node::Internal { keys, children } => {
                        let i = match keys.binary_search(key) {
                            Ok(i) => i + 1,
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

    // Replaces a node in a list of children with a new node ID, reclaiming the old node.
    fn replace_node(
        &mut self,
        children: &mut [NodeId],
        idx: usize,
        new_node_id: NodeId,
    ) -> Result<()> {
        if idx >= children.len() {
            return Err(TreeError::BackendAny(
                "Index out of bounds while replacing node".to_string(),
            ).into());
        }
        self.reclaim_node(children[idx])?;
        children[idx] = new_node_id;
        Ok(())
    }

    // Removes a node from a list of children, reclaiming the node.
    fn remove_node(
        &mut self,
        children: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<()> {
        if idx >= children.len() {
            return Err(TreeError::BackendAny(
                "Index out of bounds while removing node".to_string(),
            ).into());
        }
        self.reclaim_node(children[idx])?;
        children.remove(idx);
        Ok(())
    }

    // Inserts a key-value pair into the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let _guard = self.epoch_mgr.pin();
        self.insert_inner(key, value)
    }
    
    // Inserts a key-value pair into the B+ tree, acquiring an epoch guard to ensure consistency,
    // finally the result is committed.
    pub fn insert_and_commit(&mut self, key: K, value: V) -> Result<()> {
        let _guard = self.epoch_mgr.pin();
        self.insert_inner(key, value)?;
        self.commit()
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert_inner(&mut self, key: K, value: V) -> Result<()> {
        let (path, mut leaf_node) = self.get_insertion_path(&key)?;
    
        let Node::Leaf { keys, values, .. } = &mut leaf_node else {
            return Err(TreeError::BackendAny(
                "Expected a leaf node for insertion".to_string(),
            ).into());
        };
    
        match keys.binary_search(&key) {
            Ok(i) => {
                values[i] = value; // Replace existing value
            }
            Err(i) => {
                keys.insert(i, key.clone());
                values.insert(i, value);
            }
        }
    
        if keys.len() > self.max_keys {
            self.handle_leaf_split(path, leaf_node)?
        } else {
            self.write_and_propagate(path, &leaf_node)?
        }
        self.size += 1; // Increment the size of the tree
        return Ok(());
    }

    // Handles the split of a leaf node when it exceeds the maximum number of keys.
    fn handle_leaf_split(
        &mut self,
        path: Vec<(NodeId, usize)>,
        leaf_node: Node<K, V>,
    ) -> Result<()> {
        let SplitResult::SplitNodes {
            mut left_node,
            right_node,
            split_key,
        } = self.split_leaf_node(leaf_node)?;    
        let right_id = self.write_node(&right_node)?;
        if let Node::Leaf { next, .. } = &mut left_node {
            *next = Some(right_id); // Link the left node to the right node
        }
        let left_id = self.write_node(&left_node)?;
    
        self.propagate_split(path, left_id, right_id, split_key)?;
        Ok(())
    }

    // Splits a leaf node into two nodes and returns the new right node, the left node, and the
    // first key of the right node to be pushed up to the parent.
    fn split_leaf_node (
        &mut self,
        mut leaf_node: Node<K, V>,
    ) -> Result<SplitResult<K, Node<K, V>>> {
        // Equally split the keys and values between the two nodes.
        if let Node::Leaf { keys, values, next } = &mut leaf_node {
            let mid = keys.len() / 2;
            let split_idx = mid; // Index to split the keys and values
            let right_keys = keys.split_off(split_idx);
            let right_values = values.split_off(split_idx);
            let split_key = right_keys.first().ok_or_else(|| { TreeError::BackendAny("Leaf node has no keys to split".to_string()) })?;
            let right_leaf = Node::Leaf {
                keys: right_keys.to_vec(),
                values: right_values,
                next: next.take(), // Retain the next pointer
            };
            let mut new_keys: Vec<K> = Vec::with_capacity(self.order);
            new_keys.extend_from_slice(keys);
            let mut new_values: Vec<V> = Vec::with_capacity(self.order);
            new_values.extend_from_slice(values);
            let left_leaf = Node::Leaf {
                keys: std::mem::take(&mut new_keys),
                values: std::mem::take(&mut new_values),
                next: None, // Link to the new right leaf
            };
            Ok( SplitResult::SplitNodes { left_node: left_leaf, right_node: right_leaf, split_key: split_key.clone()})
        } else {
            Err(TreeError::BackendAny(
                "Expected a leaf node for splitting".to_string(),
            ).into())
        }
    }

    // Splits an internal node into two nodes and returns the new right node, the left node, and
    // the first key of the right node to be pushed up to the parent.
    fn split_internal_node(
        &mut self,
        mut internal_node: Node<K, V>,
    ) -> Result<SplitResult<K, Node<K, V>>> {
        if let Node::Internal { keys, children } = &mut internal_node {
            // Index to split the keys and values, right node will have
            // values past mid + 1, split node will be at mid and removed and left node will have
            // the remaining values
            let mid = keys.len() / 2;
            let split_idx = mid + 1;
            let right_keys = keys.split_off(split_idx);
            let right_children = children.split_off(split_idx);
            let right_internal = Node::Internal {
                keys: right_keys,
                children: right_children,
            };
            // split key is the key at the split index, which will be  removed and pushed up to the parent
            let split_key = keys.pop().ok_or_else(|| { TreeError::BackendAny("Internal node has no mid keys for split".to_string()) })?;
            let left_internal = Node::Internal {
                keys: std::mem::take(keys),
                children: std::mem::take(children),
            };
            Ok(SplitResult::SplitNodes { right_node: right_internal, left_node: left_internal, split_key: split_key.clone() })
        } else {
            Err(TreeError::BackendAny(
                "Expected an internal node for splitting".to_string(),
            ).into())
        }
    }

    // Writes a node and propagates the update to the parent nodes.
    fn write_and_propagate(&mut self, path: Vec<(u64, usize)>, node: &Node<K, V>) -> Result<()> {
        let new_node_id = self.write_node(node)?;
        if path.is_empty() {
            self.replace_root(new_node_id)?;
        } else {
            self.propagate_node_update(path, new_node_id)?;
        }
        Ok(())
    }

    // Propagates an update to the parent nodes after a node has been updated or split.
    fn propagate_node_update(
        &mut self,
        mut path: Vec<(NodeId, usize)>,
        mut updated_child_id: NodeId,
    ) -> Result<()> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let mut parent_node = self
                .read_node(parent_id)?
                .ok_or_else(|| TreeError::NodeNotFound("Parent node not found".to_string()))?;

            let Node::Internal { ref mut children, .. } = parent_node else {
                return Err(TreeError::BackendAny(
                    "Expected internal node while updating parents".to_string(),
                )
                .into());
            };
            if insert_pos >= children.len() {
                return Err(TreeError::BackendAny(
                    format!(
                        "Insert position {} out of bounds for children in node {}",
                        insert_pos, parent_id
                    ),
                )
                .into());
            }
            // Reclaim the original child node and update the child pointer
            self.replace_node(children, insert_pos, updated_child_id)?;
            updated_child_id = self.write_node(&parent_node)?;

            if parent_id == self.root_id {
                self.replace_root(updated_child_id)?;
            }
        }
        Ok(())
    }

    // Insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn propagate_split(&mut self,
          mut path: Vec<(NodeId, usize)>,
          mut left: NodeId,
          mut right: NodeId,
          mut key: K,
        ) -> Result<()> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let Some(mut node) = self.read_node(parent_id)? else {
                return Err(TreeError::NodeNotFound(
                    "Node not found while inserting into parent".to_string(),
                ).into());
            };
            let Node::Internal { keys, children } = &mut node else {
                return Err(TreeError::BackendAny(
                    "Expected internal node in propagation path".to_string(),
                ).into());
            };
            // Insert the split key and adjust children
            keys.insert(insert_pos, key);
            // Reclaim the original child node
            self.replace_node(children, insert_pos, left)?;
            // Replace and insert the new children
            children.insert(insert_pos + 1, right);
            // if there is no further overflow we can just propagate the update and return
            if keys.len() <= self.max_keys {
                self.write_and_propagate(path, &node)?;
                return Ok(());
            }
            // Handle internal node split
            let SplitResult::SplitNodes {
                left_node,
                right_node,
                split_key,
            } = self.split_internal_node(node)?;

            left = self.write_node(&left_node)?;
            right = self.write_node(&right_node)?;
            key = split_key;
        }

        // We reached the root: create a new root node
        let new_root = Node::Internal {
            keys: vec![key],
            children: vec![left, right],
        };

        let new_root_id = self.write_node(&new_root)?;
        self.replace_root(new_root_id)?;
        self.height += 1;

        Ok(())
    }

    // Search for a key in the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn search(&mut self, key: &K) -> Result<Option<V>> {
        if self.root_id == 0 {
            println!("Root id {} empty", self.root_id);
            return Ok(None); // Empty tree
        }
        let _guard = self.epoch_mgr.pin();
        self.search_inner(key)
    }
    
    // Search for a key and return the value if exists
    pub fn search_inner(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        println!("Searching for key: {:?} at root id {}", key, current_id);
        loop {
            match self.read_node(current_id)? {
                Some(Node::Internal { keys, children }) => {
                    // target >= keys[i] means we should go to the (i+1)-th child
                    // target < keys[i]  (not found) means we should go to the i-th child - descent
                    // where it would be inserted
                    let i = match keys.binary_search(key) {
                        Ok(i) => i + 1, // Go to the next child
                        Err(i) => i, // Go to the child where it would be inserted
                    };
                    current_id = children[i];
                }
                Some(Node::Leaf { keys, values, .. }) => {
                    println!("Searching for key at leaf with keys {:?} values {:?}", keys, values);
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
        if self.root_id == 0 {
            return Ok(None); // Empty tree
        }
        if start > end {
            return Ok(None); // Invalid range
        }
        let _guard = self.epoch_mgr.pin();
        let mut current_id = self.root_id;
        loop {
            match self.read_node(current_id)? {
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

    // Deletes a key from the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn delete(&mut self, key: &K) -> Result<DeleteResult> {
        let _guard = self.epoch_mgr.pin();
        self.delete_inner(key)
    }

    // Deletes a key from the B+ tree, acquiring an epoch guard to ensure consistency.
    pub fn delete_and_commit(&mut self, key: &K) -> Result<DeleteResult> {
        let _guard = self.epoch_mgr.pin();
        let res = self.delete_inner(key)?;
        self.commit()?;
        Ok(res)
    }

    // Delete and handle underflow of leaf nodes
    // Every key in an internal node must match the first key in its right child
    pub fn delete_inner(&mut self, key: &K) -> Result<DeleteResult> {
        let (path, mut node) = self.get_insertion_path(key)?;
        let Node::Leaf { keys, values, .. } = &mut node else {
            return Err(TreeError::BackendAny("Expected leaf node".to_string()).into());
        };
        let Ok(index) = keys.binary_search(key) else {
            return Ok(DeleteResult::NotFound);
        };

        keys.remove(index);
        values.remove(index);

        // no underflow if the node has enough keys or it is the root node
        if keys.len() >= self.min_leaf_keys || path.is_empty() {
            self.write_and_propagate(path, &node)?;
            return Ok(DeleteResult::Updated);
        }

        let res = self.handle_underflow(path, node)?;
        self.size = self.size.saturating_sub(1); // Decrement the size of the tree
        return Ok(res);
    }

    // Handles underflow of a node after deletion, trying to borrow from siblings or merge with them.
    fn handle_underflow(
        &mut self,
        mut path: Vec<(NodeId, usize)>,
        mut node: Node<K, V>,
    ) -> Result<DeleteResult> {
        while let Some((parent_id, idx)) = path.pop() {
            let Some(mut parent_node) = self.read_node(parent_id)? else {
                return Err(TreeError::NodeNotFound("Parent node not found".to_string()).into());
            };
            {
                let Node::Internal { keys: ref mut parent_keys, ref mut children } = parent_node else {
                    return Err(TreeError::BackendAny("Expected internal node as parent".to_string()).into());
                };
                
                // COULD REMOVE
                if path.is_empty() && self.shrink_to_root(children)? {
                    return Ok(DeleteResult::Underflowed);
                }
                // Try borrowing from left or right sibling, on success just propagate the update,
                // no change in number of keys in the parent node
                if idx > 0 && self.try_borrow_from_left(&mut node, parent_keys, children, idx)? {
                    return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                }
                if (idx < children.len() - 1) && self.try_borrow_from_right(&mut node, parent_keys, children, idx)? {
                    return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                }
                // Try to merge with left or right sibling
                let mut merged = None;
                if let Some(id) = self.try_merge_with_left(&mut node, parent_keys, children, idx)? {
                    merged = Some(id);
                } else if let Some(id) = self.try_merge_with_right(&mut node, parent_keys, children, idx)? {
                    merged = Some(id);
                }
                // We should have merged with a sibling or borrowed from it otherwise invalid state
                if merged.is_some() {
                    // the parent node underflowed after merge
                    if parent_keys.len() < self.min_internal_keys {
                        // handle root node underflow
                        if path.is_empty() {
                           if self.shrink_to_root(children)? {
                               return Ok(DeleteResult::Underflowed);
                           } else {
                               return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                           }
                        }
                        // Continue handling underflow
                        node = parent_node;
                        continue;
                    } else {
                        // Parent node didn't overflow, just write the updated parent node
                        return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                    }
                }
            }
        }
        Err(TreeError::BackendAny("Node underflow couldn't be resolved".to_string()).into())
    }

    // Shrinks the tree to the root if it has only one child and the height is greater than 1.
    fn shrink_to_root(&mut self, children: &[NodeId]) -> Result<bool> {
        // shrink the tree if we have only one child at the root and the height is greater than 1
        if children.len() == 1 && self.height > 1 {
            self.replace_root(children[0])?;
            self.height = self.height.saturating_sub(1);
            return Ok(true);
        }
        Ok(false)
    }


    // Tries to borrow a key from the left sibling of the current node.
    fn try_borrow_from_left(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut [K],
        children: &mut [NodeId],
        idx: usize,
    ) -> Result<bool> {
        if idx == 0 {
            return Ok(false);
        }
        let left_child_idx = idx - 1; // The index of the left sibling in the children array
        let left_sibling_id = children[left_child_idx];
        let parent_key_idx = idx - 1; // The key in the parent node that separates the two children
        let Some(mut left_sibling) = self.read_node(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound("Left sibling not found".to_string()).into());
        };
        match (&mut left_sibling, &mut *node) {
            (
                Node::Leaf { keys: left_keys, values: left_values, .. },
                Node::Leaf { keys: right_keys, values: right_values, .. },
            ) => {
                if left_keys.len() > self.min_leaf_keys {
                    let borrowed_key = left_keys.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no keys to borrow".to_string())
                    })?;
                    let borrowed_value = left_values.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no values to borrow".to_string())
                    })?;
                    right_keys.insert(0, borrowed_key.clone());
                    right_values.insert(0, borrowed_value);
                    // Update the separator key with the borrowed key - separator should alwasy be the first key of the right child
                    parent_keys[parent_key_idx] = borrowed_key; 
                } else {
                    return Ok(false);
                }
            }
            (
                Node::Internal { keys: left_keys, children: left_children },
                Node::Internal { keys: right_keys, children: right_children },
            ) => {
                if left_keys.len() > self.min_internal_keys {
                    let borrowed_key = left_keys.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no keys to borrow".to_string())
                    })?;
                    let borrowed_child = left_children.pop().ok_or_else(|| {
                        TreeError::BackendAny("Left sibling has no children to borrow".to_string())
                    })?;
                    right_keys.insert(0, parent_keys[parent_key_idx].clone());
                    right_children.insert(0, borrowed_child);
                    // Update the parent key with the borrowed key
                    parent_keys[parent_key_idx] = borrowed_key;
                } else {
                    return Ok(false);
                }
            }
            _ => {
                return Err(TreeError::BackendAny(
                    "Expected matching node types for borrowing".to_string(),
                ).into());
            }
        };
        let new_node_id = self.write_node(node)?;
        let new_left_node_id = self.write_node(&left_sibling)?;

        self.replace_node(children, left_child_idx, new_left_node_id)?;
        self.replace_node(children, idx, new_node_id)?;

        Ok(true)
    }

    // Tries to borrow a key from the right sibling of the current node.
    fn try_borrow_from_right(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut [K],
        children: &mut [NodeId],
        idx: usize,
    ) -> Result<bool> {
        if idx >= children.len() {
            return Ok(false); // No right sibling to borrow from
        }
        let parent_key_idx = idx; // The key in the parent node that separates the two children
        let right_sibling_id = children[idx + 1];
        let Some(mut right_sibling) = self.read_node(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound("Right sibling not found".to_string()).into());
        };
        match (&mut *node, &mut right_sibling) {
            (
                Node::Leaf { keys: left_keys, values: left_values, .. },
                Node::Leaf { keys: right_keys, values: right_values, .. },
            ) => {
                if right_keys.len() > self.min_leaf_keys {
                    // Borrow from the right sibling
                    let borrowed_key = right_keys.remove(0);
                    let borrowed_value = right_values.remove(0);
                    let new_separator_key = right_keys[0].clone(); // The first key of the right
                    // sibling becomes the new separator key
                    left_keys.push(borrowed_key);
                    left_values.push(borrowed_value);
                    // Update the separator key with the first key  of the right sibling
                    parent_keys[parent_key_idx] = new_separator_key.clone(); // Update the parent key with the
                } else {
                    return Ok(false); // Not enough keys to borrow
                }
            }
            (
                Node::Internal { keys: left_keys, children: left_children },
                Node::Internal { keys: right_keys, children: right_children },
            ) => {
                if right_keys.len() > self.min_internal_keys { 
                    // Steps for Internal node are diffent we need to swap the first key of the
                    // right sibling with the separator from parent
                    // 1. Move separator key from parent to the left node
                    left_keys.push(parent_keys[parent_key_idx].clone());
                    // 2. Update the parent key with the first key of the right sibling
                    parent_keys[parent_key_idx] = right_keys.remove(0); 
                    // 3. Borrow a child from the right sibling
                    let borrowed_child = right_children.remove(0);
                    left_children.push(borrowed_child);
                } else {
                    return Ok(false); // Not enough keys to borrow
                }
            }
            _ => {
                return Err(TreeError::BackendAny(
                    "Expected matching node types for borrowing".to_string(),
                ).into());
            }
        }
        // Write the updated nodes back to storage
        let new_node_id = self.write_node(node)?;
        let new_right_node_id = self.write_node(&right_sibling)?;
        self.replace_node(children, idx, new_node_id)?;
        self.replace_node(children, idx + 1, new_right_node_id)?;

        Ok(true)
    }

    // Tries to merge the current node with its left sibling if possible.
    fn try_merge_with_left(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<Option<NodeId>> {
        if idx == 0 {
            return Ok(None);
        }
        let left_sibling_id = children[idx - 1];
        let parent_key_idx = idx - 1; // The key in the parent node that separates the two children
        let Some(mut left_sibling) = self.read_node(left_sibling_id)? else {
            return Err(TreeError::NodeNotFound("Left sibling not found".to_string()).into());
        };
        match (&mut left_sibling, &mut *node) {
            (
                Node::Leaf { keys: left_keys, .. },
                Node::Leaf { keys: right_keys, .. },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                // Merge the current node with the left sibling
                let merged_node = self.merge_nodes(&mut left_sibling, node)?;
                let merged_node_id = self.write_node(&merged_node)?;
                // Update the parent node
                self.remove_node(children, idx)?; // remove the right sibling
                self.replace_node(children, idx - 1, merged_node_id)?; // update the left sibling
                // Update the parent keys
                if !parent_keys.is_empty() {
                    parent_keys.remove(parent_key_idx); // Update the parent key with the first key of the merged node
                }
                Ok(Some(merged_node_id))
            },
            (
                Node::Internal { keys: left_keys, .. },
                Node::Internal { keys: right_keys, .. },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                let seperator_key = parent_keys.remove(parent_key_idx); // The key that separates
                // The two children has to be removed and added to the left sibling
                left_keys.push(seperator_key); // Add the separator key to the left sibling
                // Merge the left sibling with the current node
                let merged_node = self.merge_nodes(&mut left_sibling, node)?;
                let merged_node_id = self.write_node(&merged_node)?;
                // Update the parent node
                self.remove_node(children, idx)?; // remove the right sibling
                self.replace_node(children, idx - 1, merged_node_id)?; // update the left sibling
                Ok(Some(merged_node_id))
            },
            _ => {
                Err(TreeError::BackendAny(
                    "Expected matching node types for merging".to_string(),
                ).into())
            }
        }
    }

    // Tries to merge the current node with its right sibling if possible.
    fn try_merge_with_right(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<Option<NodeId>> {
        // Check if there is a right sibling to merge with
        let right_idx = idx + 1;
        if right_idx >= children.len() {
            return Ok(None);
        }
        
        let right_sibling_id = children[right_idx];
        let parent_key_idx = idx; // The key in the parent node that separates the two children
        let Some(mut right_sibling) = self.read_node(right_sibling_id)? else {
            return Err(TreeError::NodeNotFound("Left sibling not found".to_string()).into());
        };
        match (&mut *node, &mut right_sibling) {
            (
                Node::Leaf { keys: left_keys, .. },
                Node::Leaf { keys: right_keys, .. },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                // Merge the current node with the left sibling
                let merged_node = self.merge_nodes(node, &mut right_sibling)?;
                let merged_node_id = self.write_node(&merged_node)?;
                // Update the parent node
                self.remove_node(children, right_idx)?; // remove the right sibling
                self.replace_node(children, idx, merged_node_id)?; // update the left sibling
                // Update the parent keys
                if !parent_keys.is_empty() {
                    parent_keys.remove(parent_key_idx); // Update the parent key with the first key of the merged node
                }
                Ok(Some(merged_node_id))
            },
            (
                Node::Internal { keys: left_keys, .. },
                Node::Internal { keys: right_keys, .. },
            ) => {
                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Ok(None); // Cannot merge, total keys exceed max keys
                }
                let seperator_key = parent_keys.remove(parent_key_idx); // The key that separates
                // the two children has to be removed and added to the left sibling
                left_keys.push(seperator_key); // Add the separator key to the left sibling
                // Merge the current node with the right sibling
                let merged_node = self.merge_nodes(node, &mut right_sibling)?;
                let merged_node_id = self.write_node(&merged_node)?;
                // Update the parent node
                self.remove_node(children, right_idx)?; // remove the right sibling
                self.replace_node(children, idx, merged_node_id)?; // update the left sibling
                Ok(Some(merged_node_id))
            },
            _ => {
                Err(TreeError::BackendAny(
                    "Expected matching node types for merging".to_string(),
                ).into())
            }
        }
    }

    // Merges two nodes (left and right) into a single node, returning the new node ID.
    pub fn merge_nodes (
        &mut self,
        left_node: &mut Node<K, V>,
        right_node: &mut Node<K, V>
    ) -> Result<Node<K, V>> {
        match(&mut *left_node, right_node) { // Match on a new mutable reference to the left node 
                                             // way to pattern match on mutable references to enums or structs in Rust when you want to destructure their contents mutably.
        (
            Node::Leaf { keys: left_keys, values: left_values, next: left_next },
            Node::Leaf { keys: right_keys, values: right_values, next: right_next },
        ) => {                // Check if the total number of keys exceeds the maximum allowed
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Err(TreeError::BackendAny(
                        "Cannot merge leaf nodes, total keys exceed max keys".to_string(),
                    ).into());
                }
                // Merge the two leaf nodes
                left_keys.append(right_keys); // Move keys from right to left
                left_values.append(right_values); // Move values from right to left
                *left_next = *right_next; // Clear the next pointer of the left node
                Ok(Node::Leaf {
                    keys: std::mem::take(left_keys),
                    values: std::mem::take(left_values),
                    next: *left_next,
                })
            },
        (
            Node::Internal { keys: left_keys, children: left_children },
            Node::Internal { keys: right_keys, children: right_children },
        ) => {                
                if left_keys.len() + right_keys.len() > self.max_keys {
                    return Err(TreeError::BackendAny(
                        "Cannot merge internal nodes, total keys exceed max keys".to_string(),
                    ).into());
                }
                left_keys.append(right_keys);
                left_children.append(right_children);
                // Update the parent node with the new node ID
                Ok(Node::Internal {
                    keys: std::mem::take(left_keys),
                    children: std::mem::take(left_children),
                })
             },
        _ => Err(TreeError::BackendAny(
            "Expected leaf nodes for merging".to_string(),
        ).into()),
        }
    }

    fn replace_root(&mut self, new_root_id: NodeId) -> Result<()> {
        // Reclaim the old root node
        //self.staged_root_id = Some(self.root_id);
        //self.staged_height = Some(self.height);
        self.reclaim_node(self.root_id)?;
        self.root_id = new_root_id;
        Ok(())
    }

    // Set the root of the B+ tree
    pub fn set_root(&mut self, root: NodeId) {
        self.root_id = root;
    }

    pub fn reclaim_node(&mut self, node_id: NodeId) -> Result<()> {
        let epoch = self.epoch_mgr.get_current_thread_epoch().ok_or_else(|| {
            TreeError::BackendAny("Failed to get epoch for current thread".to_string())
        })?;
        self.epoch_mgr.add_reclaim_candidate(epoch, node_id);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        // Skip if there's no staged root
        //let new_root = match self.staged_root_id {
        //    Some(id) => id,
        //    None => return Ok(()), // No-op
        //};
        let new_root = self.root_id; // Use the current root as the new root
    
        //let new_height = match self.staged_height {
        //    Some(height) => height,
        //    None => self.height,
        //};
        let new_height = self.height; // Use the current root as the new root
    
        self.storage.flush()?;

        //self.reclaim_node(self.root_id)?;
        println!("Committing new root: {} at height: {}", new_root, new_height);
        // Now commit the new root
        self.root_id = new_root;
        self.height = new_height;
        self.staged_root_id = None;
        self.staged_height = None;
    
        let target_slot = self.txn_id % 2;
        self.txn_id += 1; // Increment transaction ID for the next commit

        self.storage.commit_metadata(
            target_slot as u8,
            self.txn_id,
            new_root,
            new_height,
            self.order,
            self.size,
        )?;

        self.commit_count.fetch_add(1, Ordering::Relaxed);

        let _new_epoch = self.epoch_mgr.advance(); // Bump the epoch for transaction management
        
        let safe_epoch = self.epoch_mgr.oldest_active();
        let reclaimed = self.epoch_mgr.reclaim(safe_epoch);
        for pid in reclaimed {
            self.storage.free_node(pid)?;
        }

        if (self.commit_count.load(Ordering::Relaxed) as u64) % COMMIT_COUNT == 0 {
            self.epoch_mgr.advance(); // Pin new epoch for reclamation
        }
        Ok(())
    }

    pub fn traverse(&mut self) -> Result<Vec<(K, V)>> {
        let mut result = Vec::new();
        if self.root_id == 0 {
            return Ok(result); // Empty tree
        }
        let _guard = self.epoch_mgr.pin();
        self.traverse_inner(self.root_id, &mut result)?;
        Ok(result)
    }

    pub fn traverse_inner(
        &mut self,
        node_id: NodeId,
        result: &mut Vec<(K, V)>,
    ) -> Result<()> {
        match self.read_node(node_id)? {
            Some(Node::Internal { keys, children }) => {
                println!("Traversing internal node with id: {} keys: {:?}, children {:?}",node_id, keys, children);
                for (i, child_id) in children.iter().enumerate() {
                    if i <= keys.len() {
                        self.traverse_inner(*child_id, result)?;
                    }
                }
            }
            Some(Node::Leaf { keys, values, .. }) => {
                println!("Traversing leaf node with id: {} keys: {:?}, values {:?}", node_id, keys, values);
                for (key, value) in keys.iter().zip(values.iter()) {
                    result.push((key.clone(), value.clone()));
                }
            }
            None => return Err(TreeError::NodeNotFound("Node not found".to_string()).into()),
        }
        Ok(())
    }

    fn create_leaf_node(&self) -> Node<K, V> {
        Node::Leaf {
            keys: Vec::with_capacity(self.max_keys),
            values: Vec::with_capacity(self.max_keys),
            next: None, // Next pointer for linked list of leaf nodes
        }
    }
    
    fn create_internal_node(&self) -> Node<K, V> {
        Node::Internal {
            keys: Vec::with_capacity(self.max_keys),
            children: Vec::with_capacity(self.max_keys + 1), // +1 for the extra child pointer
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_store::FileStore;
    use crate::storage::page_store::PageStore;

    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use rand::Rng;

    #[test]
    fn write_and_read_value() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile.bin";
        
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let order = 3; // B+ tree order
        let mut tree = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        let key = 1u64;
        let value = "a".to_string();
        let res = tree.insert(key, value.clone());
        assert!(res.is_ok(), "Node should be inserted successfully");
        let res = tree.search(&key)?;
        assert!(res.is_some(), "Node should be read successfully");
        assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        Ok(())
    }
    
    #[test]
    fn write_and_read_values_multiple() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile.bin";
        
        let order = 20; // B+ tree order
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        for i in 0..order - 1 {
            let key = i as u64;
            let value = format!("value_{}", i);
            //let res = tree_root.insert(key, value.clone());
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Value should be inserted successfully");
            let res = tree_root.search(&key)?;
            assert!(res.is_some(), "Value should be read successfully");
            assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        }
        Ok(())
    }

    #[test]
    fn write_and_read_values_with_overflow() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_2.bin";
        
        let order = 3; // B+ tree order
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        let multiplier = 1000; // Number of times to insert times the order - this will cause
        // overflows 
        for i in 0..order*multiplier {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Value should be inserted successfully");
        }
        for i in 0..order*multiplier {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.search(&key)?;
            assert!(res.is_some(), "Value should be read successfully");
            assert_eq!(res.unwrap(), value, "Value should match the inserted value");
        }
        Ok(())
    }

    #[test]
    fn write_and_delete_lockstep() -> Result<(), anyhow::Error> {
        let file_path = "test_lockstep.bin";
        let order = 3; // B+ tree order
        let multiplier = 2; // Number of times to insert and delete
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        let bound = order as u64*multiplier;
        for i in 0..bound {
            let key = i;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
            println!("Initial state");
            tree_root.traverse()?;
        for i in 0..bound {
            let key = i;
            tree_root.delete(&key)?;
            let res = tree_root.search(&(key))?;
            println!("Key {} deleted, search result: {:?}", key, res);
            assert!(res.is_none(), "Key {} should be deleted successfully res none {}", key, res.is_none());

            let mut rng = thread_rng();
            if bound == i + 1 {
                return Ok(()); // No more keys to search
            }
            let key_rand = rng.gen_range(i+1..bound);
            println!("Searching for random key {}", key_rand);
            let tree_vals = tree_root.traverse()?;
            println!("Tree values: {:?}", tree_vals);
            let res = tree_root.search(&(key_rand))?;
            assert!(res.is_some(), "Key {} should be present res some {}", key_rand, res.is_some());
        }
        Ok(())
    }

    #[test]
    fn write_and_delete_values() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_3.bin";
        
        let order = 10; // B+ tree order
        let multiplier = 200_u64; // Number of times to insert and delete
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        // Inserting values
        for i in 0..order as u64*multiplier {
            let key = i;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        // Deleting all values
        for i in 0..order as u64*multiplier {
            let key = i;
            tree_root.delete(&key)?;
            let res = tree_root.search(&key)?;
            assert!(res.is_none(), "Key {} should be deleted successfully res none {}", key, res.is_none());
        }
        // Check that the tree is empty after all deletions
        let res = tree_root.traverse()?;

        for i in 0..order as u64*multiplier {
            let key = i;
            let res = tree_root.search(&key)?;
            assert!(res.is_none(), "Key {} should be deleted successfully res none {}", key, res.is_none());
        }
        assert!(res.is_empty(), "Tree should be empty after all deletions");
        Ok(())
    }
    
    #[test]
    fn write_and_delete_values_random() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_4.bin";
        
        let order = 10; // B+ tree order
        let multiplier = 200_u64; // Number of times to insert and delete
        //let order = 3; // B+ tree order
        //let multiplier = 2; // Number of times to insert and delete
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        for i in 0..order as u64*multiplier {
            let key = i;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        let mut values_to_delete: Vec<u64> = (0..(order as u64)*multiplier).collect();
        let mut rng = thread_rng();
        values_to_delete.shuffle(&mut rng);

        for i in values_to_delete {
            let key = i;
            tree_root.delete(&key)?;
            let res = tree_root.search(&key)?;
            assert!(res.is_none(), "Node should be deleted successfully");
        }
        Ok(())
    }

    #[test]
    fn test_height_increase_decrease() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_5.bin";
        
        let order = 3; // B+ tree order
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        let iterations = order * 10; // Number of times to insert
        for i in 0..order - 1 {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        assert_eq!(tree_root.height, 1, "Height should be 1 after inserting {} nodes", order-1);
        for i in 0..order - 1 {
            let key = i as u64;
            tree_root.delete(&key)?;
        }
        assert_eq!(tree_root.height, 1, "Height should remain 1 after deleting all nodes");
        for i in 0..iterations {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        for i in 0..iterations {
            let key = i as u64;
            tree_root.delete(&key)?;
        }
        assert_eq!(tree_root.height, 1, "Height should remain 1 after deleting all nodes");
        Ok(())
    }

    #[test]
    fn insert_duplicate_keys_should_overwrite_value() -> Result<()> {
        let file_path = "test_duplicates.bin";
        let order = 4;
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree = BPlusTree::<String, String, FileStore<PageStore>>::new(store, order)?;

        for i in 0..order {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            let value_updated = format!("value_upd_{}", i);
            let res = tree.insert(key.clone(), value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
            assert_eq!(tree.search(&(key.clone()))?, Some(value));
            let res = tree.insert(key.clone(), value_updated.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
            assert_eq!(tree.search(&(key.clone()))?, Some(value_updated), "Value should be updated for duplicate key");
        }

        Ok(())
    }

    #[test]
    fn commit_and_load_tree() -> Result<()> {
        let file_path = "test_commit_load.bin";
        let order = 4;
        let multiplier = 10; // Number of times to insert
        let iterations = order * multiplier;
        {
            let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
            let mut tree = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;

            for i in 0..iterations {
                let key = i as u64;
                let value = format!("value_{}", i);
                let res = tree.insert(key, value.clone());
                assert!(res.is_ok(), "Node should be inserted successfully");
            }

            // Commit the changes
            tree.commit()?;
            for i in 0..iterations {
                let key = i as u64;
                let res = tree.search(&key)?;
                assert!(res.is_some(), "Loaded tree should have the key {}", key);
            }
        }
        {
            let store_load = FileStore::<PageStore>::new(file_path)?;
            // Load the tree from storage
            let mut loaded_tree = BPlusTree::<u64, String, FileStore<PageStore>>::load(store_load)?;
            // Verify the loaded tree
            for i in 0..iterations {
                let key = i as u64;
                let value = format!("value_{}", i);
                let res = loaded_tree.search(&key)?;
                assert!(res.is_some(), "Loaded tree should have the key {}", key);
                assert_eq!(loaded_tree.search(&key)?, Some(value), "Loaded tree should have the correct value for key {}", key);
            }
        }
        Ok(())
    }
}

