use std::fmt::Debug;
use crate::bplustree::tree::{SharedBPlusTree, BaseVersion, StagedMetadata, WriteResult};
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
    staged_update: Option<StagedMetadata>, // Staged metadata root ID
    tree_base_version: BaseVersion, // Base version of the tree at transaction start
    changes: Vec<WriteOp<K, V>>,
    reclaimed_nodes: Vec<u64>, // Pages to be reclaimed
    staged_nodes: Vec<u64>, // Pages to be reclaimed
    initial_root_id: u64, // Current root ID of the tree
}

impl<K: Debug, V: Debug, S> WriteTransaction<K, V, S>
where
    K: KeyCodec + Clone + Ord,
    V: ValueCodec + Clone,
    S: NodeStorage<K, V> + MetadataStorage,
{
    pub fn new(tree: SharedBPlusTree<K, V, S>) -> Self {

        Self {
            tree: tree.clone(),
            tree_base_version: BaseVersion { committed_ptr: tree.get_metadata_ptr() }, // Store initial root ID for reference
            staged_update: { // No staged update initially
            let res = tree.get_snapshot();
                Some(StagedMetadata {
                    root_id: res.root_id,
                    height: res.height,
                    size: res.size,
                })
            },
            changes: Vec::new(),
            staged_nodes: Vec::new(),
            reclaimed_nodes: Vec::new(),
            initial_root_id: tree.get_root_id(), // Get the initial root ID
        }
    }
    
    // Get the root ID of the intermediate staged tree, if there is one, otherwise return the
    // current root ID
    pub fn get_root_id(&self) -> u64 {
        self.staged_update.as_ref()
            .map_or(self.initial_root_id, |res| res.root_id)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.changes.push(WriteOp::Insert(key.clone(), value.clone()));
        let root_id = self.tree.get_root_id();
        let res = self.tree.insert_with_root(key, value, root_id)?;
        self.reclaimed_nodes.extend(res.reclaimed_nodes);
        self.staged_nodes.extend(res.staged_nodes);
        self.staged_update = 
                Some(StagedMetadata {
                    root_id: res.new_root_id,
                    height: res.new_height,
                    size: res.new_size,
                });
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Result<()> {
        self.changes.push(WriteOp::Delete(key.clone()));
        let res = self.tree.delete_with_root(key, self.get_root_id())?;
        self.reclaimed_nodes.extend(res.reclaimed_nodes);
        self.staged_nodes.extend(res.staged_nodes);
        self.staged_update = 
                Some(StagedMetadata {
                    root_id: res.new_root_id,
                    height: res.new_height,
                    size: res.new_size,
                });
        Ok(())
    }

    pub fn commit(mut self) -> Result<TxnStatus> {
        for _ in 0..MAX_COMMIT_RETRIES {
            let staged_update = self.staged_update.take()
                .expect("Staged update should be set before commit");
            let res = self.tree.try_commit(
                    &self.tree_base_version,
                    staged_update,
                );
            if res.is_ok() {
                if let Some(epoch) = self.tree.get_epoch_mgr().get_current_thread_epoch() {
                    // Add all staged nodes to the epoch manager for reclamation
                    for node_id in self.reclaimed_nodes.drain(..) {
                        self.tree.get_epoch_mgr().add_reclaim_candidate(epoch, node_id);
                    }
                }
                self.changes.clear();
                return Ok(TxnStatus::Committed);
            } else {
                // Root changed — retry entire transaction
                self.initial_root_id = self.tree.get_root_id(); // Update initial root ID
                self.reclaimed_nodes.clear(); // reset collected reclaim info
                // reclaim staged nodes
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

    // Rebase the transaction by applying all changes to the tree
    fn rebase(&mut self) -> Result<()> {
        for op in &self.changes {
            match op {
                WriteOp::Insert(k, v) => {
                    let write_res = self.tree.insert_with_root(
                        k.clone(),
                        v.clone(),
                        self.tree.get_root_id(),
                    )?;
                    self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
                    self.staged_nodes.extend(write_res.staged_nodes);
                    self.staged_update = 
                            Some(StagedMetadata {
                                root_id: write_res.new_root_id,
                                height: write_res.new_height,
                                size: write_res.new_size,
                            });
                }
                WriteOp::Delete(k) => {
                    let write_res = self.tree.delete_with_root(
                        k,
                        self.tree.get_root_id(),
                    )?;
                    self.reclaimed_nodes.extend(write_res.reclaimed_nodes);
                    self.staged_nodes.extend(write_res.staged_nodes);
                    self.staged_update = 
                            Some(StagedMetadata {
                                root_id: write_res.new_root_id,
                                height: write_res.new_height,
                                size: write_res.new_size,
                            });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::{mock, predicate::*};
    
    mock! {
    }
    fn test_tree() -> (SharedBPlusTree<u64, String, MockStorage>, MockStorage) {
        // Create a mock storage and a shared BPlusTree instance for testing
        let storage = MockStorage::new();
        let tree = SharedBPlusTree::new(storage.clone());
        (tree, storage)
    }

    #[test]
    fn commit_happy_path() {
        let (tree, mocks) = test_tree();
        let base = BaseVersion { committed_ptr: tree.committed_ptr() };
        let staged = StagedMetadata { root_id: 42, height: 3, size: 10 };

        tree.try_commit(base, staged).unwrap();

        let m = tree.metadata();
        assert_eq!(m.root_id, 42);
        assert_eq!(m.txn_id, 1);
    }

    #[test]
    fn commit_retries_on_conflict() {
        let (tree, mocks) = test_tree();
        let base = BaseVersion { committed_ptr: tree.committed_ptr() };
        let staged = StagedMetadata { root_id: 42, height: 3, size: 10 };

        // Simulate a conflict
        mocks.expect_try_commit()
            .returning(|_, _| Err(anyhow::anyhow!("Conflict")));

        let result = tree.try_commit(base, staged);
        assert!(result.is_err());
    }

    // commit should abort if there is a storage failure - don't change the tree state (cas)
    #[test]
    fn commit_metadata_write_failure_is_abort() { /* ... */ }

    #[test]
    fn flush_failure_after_cas_keeps_published_state() { /* ... */ }
}
