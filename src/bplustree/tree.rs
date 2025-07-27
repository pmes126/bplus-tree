use std::fmt::Debug;

use crate::bplustree::{Node, TreeError};
use crate::storage::ValueCodec;
use crate::storage::KeyCodec;
use crate::storage::{NodeStorage, MetadataStorage, metadata, metadata::{METADATA_PAGE_1, METADATA_PAGE_2}};
use crate::bplustree::BPlusTreeRangeIter;
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
    max_keys: usize,
    min_internal_keys: usize,
    min_leaf_keys: usize,
    storage: S,
    height: usize, // Height of the B+ tree
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
            min_internal_keys: (order - 1).saturating_div(2), // Ensure min_internal_keys is at
            // least 1
            min_leaf_keys: order.saturating_div(2), // Ensure min_keys is at least 1
            height: 1, // Start with height 1 for the root node
            phantom: std::marker::PhantomData,
        })
    }

    pub fn load(mut storage: S) -> Result<BPlusTree<K, V, S>, TreeError> {
        let md = storage.get_metadata()?;
        let root_id = md.root_node_id;
        let order = md.order as usize;
        
        let max_keys = order - 1;
        let min_internal_keys = (order - 1).saturating_div(2); // Ensure min_internal_keys is at
        let min_leaf_keys = order.saturating_div(2); // Ensure min_keys is at least 1

        Ok(Self {
            root_id,
            storage,
            order,
            max_keys,
            min_internal_keys,
            min_leaf_keys,
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

    // should be inserted.
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

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
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
            self.handle_leaf_split(path, leaf_node)
        } else {
            self.write_and_propagate(path, &leaf_node)
        }
    }

    // Handles the split of a leaf node when it exceeds the maximum number of keys.
    fn handle_leaf_split(
        &mut self,
        path: Vec<(NodeId, usize)>,
        leaf_node: Node<K, V>,
    ) -> Result<()> {
        let SplitResult::SplitNodes {
            left_node,
            right_node,
            split_key,
        } = self.split_leaf_node(leaf_node)?;    
        let left_id = self.write_node(&left_node)?;
        let right_id = self.write_node(&right_node)?;
    
        self.propagate_split(path, left_id, right_id, split_key)?;
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
            let mut new_keys: Vec<K> = Vec::with_capacity(self.order);
            new_keys.extend_from_slice(keys);
            let mut new_values: Vec<V> = Vec::with_capacity(self.order);
            new_values.extend_from_slice(values);
            let left_leaf = Node::Leaf {
                keys: new_keys,
                values: new_values,
                next: Some(self.write_node(&right_leaf)?), // Link to the new right leaf
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
            let mid = keys.len() / 2;
            let right_keys = keys.split_off(mid + 1);
            let right_children = children.split_off(mid + 1);
            let split_key = right_keys.first().ok_or_else(|| { TreeError::BackendAny("Internal node has no keys to split".to_string()) })?;
            let right_internal = Node::Internal {
                keys: right_keys.to_vec(),
                children: right_children,
            };
            let mut new_keys: Vec<K> = Vec::with_capacity(self.order);
            new_keys.extend_from_slice(keys);
            let mut new_children: Vec<NodeId> = Vec::with_capacity(self.order + 1);
            new_children.extend_from_slice(children);
            let left_internal = Node::Internal {
                keys: new_keys,
                children: new_children,
            };
            Ok(SplitResult::SplitNodes { right_node: right_internal, left_node: left_internal, split_key: split_key.clone() })
        } else {
            Err(TreeError::BackendAny(
                "Expected an internal node for splitting".to_string(),
            ).into())
        }
    }

    // Propagates an update to the parent nodes after a node has been updated or split.
    fn propagate_node_update(
        &mut self,
        mut path: Vec<(NodeId, usize)>,
        mut updated_child_id: NodeId,
    ) -> Result<()> {
        if path.is_empty() {
            self.root_id = updated_child_id;
            return Ok(());
        }
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

            children[insert_pos] = updated_child_id;
            updated_child_id = self.write_node(&parent_node)?;

            if parent_id == self.root_id {
                self.root_id = updated_child_id;
                return Ok(());
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
            children[insert_pos] = left;
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

        self.root_id = self.write_node(&new_root)?;
        self.height += 1;

        Ok(())
    }

    // Search for a key and return the value if exists
    pub fn search(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        loop {
            match self.read_node(current_id)? {
                Some(Node::Internal { keys, children }) => {
                    // target >= keys[i] means we should go to the (i+1)-th child
                    // target < keys[i]  (not found) means we should go to the i-th child - descent
                    // where it would be inserted
                    let i = match keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i, 
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

    // Delete and handle underflow of leaf nodes
    pub fn delete(&mut self, key: &K) -> Result<DeleteResult> {
        let (path, mut node) = self.get_insertion_path(key)?;
        let Node::Leaf { keys, values, .. } = &mut node else {
            return Err(TreeError::BackendAny("Expected leaf node".to_string()).into());
        };
        let Ok(index) = keys.binary_search(key) else {
            return Ok(DeleteResult::NotFound);
        };

        keys.remove(index);
        values.remove(index);

        // This is the root node
        if path.is_empty() {
            self.root_id = self.write_node(&node)?;
            return Ok(DeleteResult::Updated);
        };
        // no underflow if the node has enough keys
        if keys.len() >= self.min_leaf_keys {
            self.write_and_propagate(path, &node)?;
            return Ok(DeleteResult::Updated);
        }
        // handle underflow
        self.handle_underflow(path, node)
    }

    // Writes a node and propagates the update to the parent nodes.
    fn write_and_propagate(&mut self, path: Vec<(u64, usize)>, node: &Node<K, V>) -> Result<()> {
        let new_node_id = self.write_node(node)?;
        if path.is_empty() {
            self.root_id = new_node_id;
        } else {
            self.propagate_node_update(path, new_node_id)?;
        }
        Ok(())
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
                if idx > 0 && self.try_borrow_from_left(&mut node, children, idx)? {
                        return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                }
                if (idx < children.len() - 1) && self.try_borrow_from_right(&mut node, children, idx)? {
                    return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                }
                // Try merging with the left sibling
                if let Some(merged_id) = self.try_merge_with_left(&mut node, parent_keys, children, idx)? {
                    // If the merge resulted in an underflow and we are not at the root, we need to continue handling it
                    if parent_keys.len() < self.min_internal_keys {
                         // we are at the root node
                         if path.is_empty() {
                            if self.shrink_to_root(children)? {
                                return Ok(DeleteResult::Underflowed);
                            } else {
                                return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                            }
                        } else {
                            // we are not at the root node, we need to continue handling the
                            // underflow
                            node = parent_node; // Revisit the parent node
                            continue;
                        }
                    } else {
                        return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Merged { left: parent_id, right: merged_id });
                    }
                }
                // Try merging with right sibling
                if let Some(merged_id) = self.try_merge_with_right(&mut node, parent_keys, children, idx)? {
                    // If the merge resulted in an underflow and we are not at the root, we need to continue handling it
                    if parent_keys.len() < self.min_internal_keys {
                        if path.is_empty() {
                           if self.shrink_to_root(children)? {
                               return Ok(DeleteResult::Underflowed);
                           } else {
                               return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Updated);
                           }
                        } else {
                            node = parent_node; // Revisit the parent node
                            continue;
                        }
                    } else {
                        return self.write_and_propagate(path, &parent_node).map(|_| DeleteResult::Merged { left: parent_id, right: merged_id });
                    }
                }
            }
        }
        Err(TreeError::BackendAny("Leaf underflow couldn't be resolved".to_string()).into())
    }

    // Shrinks the tree to the root if it has only one child and the height is greater than 1.
    fn shrink_to_root(&mut self, children: &Vec<NodeId>) -> Result<bool> {
        // shrink the tree if we have only one child at the root and the height is greater than 1
        if children.len() == 1 && self.height > 1 {
            self.root_id = children[0];
            self.height = self.height.saturating_sub(1);
            return Ok(true);
        }
        Ok(false)
    }


    // Tries to borrow a key from the left sibling of the current node.
    fn try_borrow_from_left(
        &mut self,
        node: &mut Node<K, V>,
        children: &mut [NodeId],
        idx: usize,
    ) -> Result<bool> {
        if idx == 0 {
            return Ok(false);
        }
        let left_sibling_id = children[idx - 1];
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
                    right_keys.insert(0, borrowed_key);
                    right_values.insert(0, borrowed_value);
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
                    right_keys.insert(0, borrowed_key);
                    right_children.insert(0, borrowed_child);
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

        children[idx - 1] = new_left_node_id;
        children[idx] = new_node_id;

        Ok(true)
    }

    // Tries to borrow a key from the right sibling of the current node.
    fn try_borrow_from_right(
        &mut self,
        node: &mut Node<K, V>,
        children: &mut [NodeId],
        idx: usize,
    ) -> Result<bool> {
        if idx >= children.len() {
            return Ok(false); // No right sibling to borrow from
        }   
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
                    left_keys.push(borrowed_key);
                    left_values.push(borrowed_value);
                    // Write the updated leaf nodes back to storage
                    let new_node_id = self.write_node(node)?;
                    let new_right_node_id = self.write_node(&right_sibling)?;
                    children[idx + 1] = new_right_node_id;
                    children[idx] = new_node_id;
                    Ok(true)
                } else {
                    Ok(false) // Not enough keys to borrow
                }
            }
            (
                Node::Internal { keys: left_keys, children: left_children },
                Node::Internal { keys: right_keys, children: right_children },
            ) => {
                if right_keys.len() > self.min_internal_keys {
                    // Borrow from the right sibling
                    let borrowed_key = right_keys.remove(0);
                    let borrowed_child = right_children.remove(0);
                    left_keys.push(borrowed_key);
                    left_children.push(borrowed_child);
                    // Write the updated leaf nodes back to storage
                    let new_node_id = self.write_node(node)?;
                    let new_right_node_id = self.write_node(&right_sibling)?;
                    children[idx + 1] = new_right_node_id;
                    children[idx] = new_node_id;
                    Ok(true)
                } else {
                    Ok(false) // Not enough keys to borrow
                }
            }
        _ => {
            Err(TreeError::BackendAny(
                "Expected matching node types for borrowing".to_string(),
            ).into())
            }
        }
    }

    // Tries to merge the current node with its left sibling if possible.
    fn try_merge_with_left(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<Option<NodeId>> {
        if idx > 0 {
            let separator_key_idx = idx - 1;
            let merged_child_idx = idx - 1;
            let left_sibling_id = children[idx - 1];
            let left_sibling = self.read_node(left_sibling_id)?;
            if let Some(mut left) = left_sibling {
                // Merge the current node with the left sibling
                let merged_node_id = self.merge_nodes(&mut left, node)?;
                // Update the parent node
                children.remove(idx);
                children[merged_child_idx] = merged_node_id;
                // Update the parent keys
                if parent_keys.len() > 1 {
                    parent_keys.remove(separator_key_idx); // Remove the separator key at idx - 1
                }
                return Ok(Some(merged_node_id));
            }
        }
        Ok(None)
    }

    // Tries to merge the current node with its right sibling if possible.
    fn try_merge_with_right(
        &mut self,
        node: &mut Node<K, V>,
        parent_keys: &mut Vec<K>,
        children: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<Option<NodeId>> {
        let right_idx = idx + 1;
        if right_idx >= children.len() {
            return Ok(None);
        }
        let right_sibling_id = children[idx + 1];
        let right_sibling = self.read_node(right_sibling_id)?;
        if let Some(mut right) = right_sibling {
            // Merge the current node with the right sibling
            let merged_node_id = self.merge_nodes(node, &mut right)?;
            children.remove(idx + 1); // Remove the right sibling
            children[idx] = merged_node_id;
            // Update the parent keys
            if parent_keys.len() > 1 {
                parent_keys.remove(idx); // Remove the key at idx
            }
            return Ok(Some(merged_node_id));
        }
        Ok(None)
    }

    // Merges two nodes (left and right) into a single node, returning the new node ID.
    pub fn merge_nodes (
        &mut self,
        left_node: &mut Node<K, V>,
        right_node: &mut Node<K, V>
    ) -> Result<NodeId> {
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
                left_keys.extend(right_keys.clone());
                left_values.extend(right_values.clone());
                *left_next = *right_next; // Clear the next pointer of the left node
                // Write the merged node back to storage
                let new_node_id = self.write_node(left_node)?;
                // Update the parent node with the new node ID
                Ok(new_node_id)
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
                left_keys.extend(right_keys.clone());
                left_children.extend(right_children.clone());
                let new_node_id = self.write_node(left_node)?;
                // Update the parent node with the new node ID
                Ok(new_node_id)
             },
        _ => Err(TreeError::BackendAny(
            "Expected leaf nodes for merging".to_string(),
        ).into()),
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

    use rand::seq::SliceRandom;
    use rand::thread_rng;

    #[test]
    fn write_and_read_values() -> Result<(), anyhow::Error> {
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
    fn write_and_read_values_multiple() -> Result<(), anyhow::Error> {
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
    fn write_and_read_values_with_overflow() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_2.bin";
        
        let order = 4; // B+ tree order
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

    #[test]
    fn write_and_delete_values() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_3.bin";
        
        let order = 10; // B+ tree order
        let multiplier = 200_u64; // Number of times to insert and delete
        let store: FileStore<PageStore> = FileStore::<PageStore>::new(file_path)?;
        let mut tree_root = BPlusTree::<u64, String, FileStore<PageStore>>::new(store, order)?;
        for i in 0..order as u64*multiplier {
            let key = i;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        for i in 0..order as u64*multiplier {
            let key = i;
            tree_root.delete(&key)?;
            let res = tree_root.search(&key)?;
            assert!(res.is_none(), "Key {} should be deleted successfully res none {}", key, res.is_none());
        }
        Ok(())
    }
    
    #[test]
    fn write_and_delete_values_random() -> Result<(), anyhow::Error> {
        let file_path = "test_flatfile_4.bin";
        
        let order = 10; // B+ tree order
        let multiplier = 200_u64; // Number of times to insert and delete
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
        for i in 0..order-1 {
            let key = i as u64;
            let value = format!("value_{}", i);
            let res = tree_root.insert(key, value.clone());
            assert!(res.is_ok(), "Node should be inserted successfully");
        }
        assert_eq!(tree_root.height, 1, "Height should be 1 after inserting {} nodes", order-1);
        for i in 0..order {
            let key = i as u64;
            tree_root.delete(&key)?;
        }
        assert_eq!(tree_root.height, 1, "Height should remain 1 after deleting all nodes");
        Ok(())
    }
}

