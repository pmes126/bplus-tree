use crate::bplustree::tree::{SharedBPlusTree, BaseVersion, StagedMetadata, CommitError};
use crate::storage::ValueCodec;
use crate::storage::KeyCodec;
use crate::storage::{NodeStorage, MetadataStorage};
use anyhow::Result;
use std::fmt::Debug;
use std::sync::Arc;

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
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
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
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
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
    use crate::tests::common::{test_storage::TestStorage, test_tree, test_tree_with_noop_storage, TestHarness};
    use crate::storage::metadata::{Metadata};

    #[test]
    fn cas_mismatch_returns_rebase_required_with_no_side_effects() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, u64, TestStorage>(storage, 128);
        let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };

        // Simulate another writer already published
        #[cfg(any(test, feature="testing"))]
        h.tree.test_force_publish(&Metadata { root_node_id: 99, height: 2, size: 5, txn_id: 1, order: 128, checksum: 0 });

        let err = h.tree.try_commit(&base, StagedMetadata { root_id: 100, height: 3, size: 6 });
        assert!(matches!(err, Err(CommitError::RebaseRequired)));

        assert_eq!(h.storage.flush_count(), 0);
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 99);
        assert_eq!(m.txn_id, 1);
    }

    #[test]
    fn metadata_write_failure_aborts_before_publish() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
        h.storage.inject_commit_failure(true);

        let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };
        let err = h.tree.try_commit(&base, StagedMetadata { root_id: 2, height: 2, size: 2 })
            .unwrap_err();
        assert!(matches!(err, CommitError::Io(_)));

        // No publish, no flush, no epoch advance
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 0);
        assert_eq!(h.storage.flush_count(), 0);
    }

    #[test]
    fn flush_failure_after_publish_keeps_state() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
        h.storage.inject_flush_failure(true);

        let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };
        let err = h.tree.try_commit(&base, StagedMetadata { root_id: 7, height: 4, size: 11 })
            .unwrap_err();
        assert!(matches!(err, CommitError::Io(_)));

        // State already published
        let m = h.tree.metadata();
        println!("Metadata after failed flush: {:?}", m);
        assert_eq!(m.root_node_id, 7);
        assert_eq!(m.txn_id, 2);
    }

    //#[test]
    //fn gc_runs_after_success() {
    //    let storage = TestStorage::new(); // Reset the test storage state
    //    let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
    //    h.tree.get_epoch_mgr().set_oldest_active(10);
    //    h.tree.get_epoch_mgr().set_reclaim_list(vec![10, 11, 12]);

    //    let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };
    //    h.tree.try_commit(&base, StagedMetadata { root_id: 555, height: 3, size: 9 }).unwrap();

    //    assert_eq!(h.storage.freed_pages(), vec![10, 11, 12]);
    //}

    #[test]
    fn published_metadata_is_visible_immediately() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
        let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };
        h.tree.try_commit(&base, StagedMetadata { root_id: 777, height: 9, size: 123 }).unwrap();

        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 777);
        assert_eq!(m.height, 9);
        assert_eq!(m.size, 123);
    }

    // Optional: maybe validate staged inputs?
    //#[test]
    //fn invalid_staged_is_rejected_before_io() {
    //    let storage = TestStorage::new(); // Reset the test storage state
    //    let h = test_tree::<u64, Vec<u8>, TestStorage>(storage, 128);
    //    let base = BaseVersion { committed_ptr: h.tree.metadata_ptr() };
    //    // Say height == 0 is invalid in your tree:
    //    let err = h.tree.try_commit(&base, StagedMetadata { root_id: 1, height: 0, size: 0 })
    //        .unwrap_err();
    //    //assert!(matches!(err, CommitError::Invalid(_)));
    //    assert_eq!(h.storage.flush_count(), 0);
    //    assert!(h.storage.last_commit().is_none());
    //}
}
