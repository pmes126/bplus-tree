use std::fmt::Debug;
use crate::bplustree::tree::{SharedBPlusTree};
use crate::storage::ValueCodec;
use crate::storage::KeyCodec;
use crate::storage::{NodeStorage, MetadataStorage};
use anyhow::Result;

enum WriteOp<K, V> {
    Insert(K, V),
    Delete(K),
}

enum TxnStatus {
    Committed,
    Aborted,
}

const MAX_COMMIT_RETRIES: usize = 10;

pub struct WriteTransaction<K, V, S>
where
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
    S: NodeStorage<K, V> + MetadataStorage,
{
    tree: SharedBPlusTree<K, V, S>,
    staged_root_id: u64,
    changes: Vec<WriteOp<K, V>>,
    reclaimed_nodes: Vec<u64>, // Pages to be reclaimed
    staged_nodes: Vec<u64>, // Pages to be reclaimed
}

impl<K: Debug, V: Debug, S> WriteTransaction<K, V, S>
where
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
    S: NodeStorage<K, V> + MetadataStorage,
{
    pub fn new(tree: SharedBPlusTree<K, V, S>) -> Self {
        let staged_root_id = tree.get_root_id(); // Read current root
        let txn_id = tree.get_txn_id();

        Self {
            tree,
            staged_root_id,
            changes: Vec::new(),
            staged_nodes: Vec::new(),
            reclaimed_nodes: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.changes.push(WriteOp::Insert(key.clone(), value.clone()));
        let write_res = self.tree.insert_with_root(key, value, self.staged_root_id)?;
        self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
        self.staged_nodes.extend(write_res.staged_nodes);
        self.staged_root_id = write_res.new_root_id;
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Result<()> {
        self.changes.push(WriteOp::Delete(key.clone()));
        let write_res = self.tree.delete_with_root(key, self.staged_root_id)?;
        self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
        self.staged_nodes.extend(write_res.staged_nodes);
        self.staged_root_id = write_res.new_root_id;
        Ok(())
    }

    pub fn commit(mut self) -> Result<TxnStatus> {
        for _ in 0..MAX_COMMIT_RETRIES {
            let current_root = self.tree.get_root_id();
            if current_root == self.staged_root_id {
                // Still valid — commit this transaction
                self.tree.commit(
                    self.staged_root_id,
                )?;
                if let Some(epoch) = self.tree.get_epoch_mgr().get_current_thread_epoch() {
                    // Add all staged nodes to the epoch manager for reclamation
                    for node_id in self.reclaimed_nodes.drain(..) {
                        self.tree.get_epoch_mgr().add_reclaim_candidate(epoch, node_id);
                    }
                }
                // Flush all dirty nodes (metadata + node writes)
                self.tree.flush()?;

                return Ok(TxnStatus::Committed);
            } else {
                // Root changed — retry entire transaction
                self.staged_root_id = current_root;
                self.reclaimed_nodes.clear(); // reset collected reclaim info
                if let Some(epoch) = self.tree.get_epoch_mgr().get_current_thread_epoch() {
                    for page_id in self.staged_nodes.drain(..) {
                        self.tree.get_epoch_mgr().add_reclaim_candidate(page_id, epoch);
                    }
                }
                self.rebase()?;
            }
        }
        Ok(TxnStatus::Aborted) // Too many retries, abort transaction
    }

    fn rebase(&mut self) -> Result<()> {
        for op in &self.changes {
            match op {
                WriteOp::Insert(k, v) => {
                    let write_res = self.tree.insert_with_root(
                        k.clone(),
                        v.clone(),
                        self.staged_root_id,
                    )?;
                    self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
                    self.staged_nodes.extend(write_res.staged_nodes);
                    self.staged_root_id = write_res.new_root_id;
                }
                WriteOp::Delete(k) => {
                    let write_res = self.tree.delete_with_root(
                        k,
                        self.staged_root_id,
                    )?;
                    self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
                    self.staged_nodes.extend(write_res.staged_nodes);
                    self.staged_root_id = write_res.new_root_id;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_transaction() {
        // This is a placeholder for actual tests.
        // You would typically create a mock or a real BPlusTree instance,
        // then perform insertions and deletions, and finally commit the transaction.
        // Assertions would be made to ensure the tree's state is as expected.
    }
}
