use crate::bplustree::tree::{BaseVersion, SharedBPlusTree, StagedMetadata};
use crate::storage::{MetadataStorage, NodeStorage};
use anyhow::Result;

enum WriteOp<K, V> {
    Insert(K, V),
    Delete(K),
}

pub enum TxnStatus {
    Committed,
    Aborted,
}

pub const MAX_COMMIT_RETRIES: usize = 10;
// Typed shim that doesn't store the tree or codecs.
pub struct WriteTransaction<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    staged_update: Option<StagedMetadata>, // Staged metadata root ID
    tree_base_version: BaseVersion,        // Base version of the tree at transaction start
    changes: Vec<WriteOp<K, V>>,
    reclaimed_nodes: Vec<u64>, // Pages to be reclaimed
    staged_nodes: Vec<u64>,    // Pages to be reclaimed
    initial_root_id: u64,      // Current root ID of the tree
}

//pub struct WriteTransaction<K, V, KC, VC, S>
//where
//    K: Clone + Ord,
//    V: Clone,
//    KC: KeyCodec<K>,
//    VC: ValueCodec<V>,
//    S: NodeStorage<K, V, KC, VC> + MetadataStorage + Send + Sync + 'static,
//{
//    tree: SharedBPlusTree<K, V, KC, VC, S>,
//    staged_update: Option<StagedMetadata>, // Staged metadata root ID
//    tree_base_version: BaseVersion,        // Base version of the tree at transaction start
//    changes: Vec<WriteOp<K, V>>,
//    reclaimed_nodes: Vec<u64>, // Pages to be reclaimed
//    staged_nodes: Vec<u64>,    // Pages to be reclaimed
//    initial_root_id: u64,      // Current root ID of the tree
//}

impl<K, V> WriteTransaction<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    pub fn new<KC, VC, S>(tree: SharedBPlusTree<K, V, KC, VC, S>) -> Self
    where
        KC: crate::codec::KeyCodec<K>,
        VC: crate::codec::ValueCodec<V>,
        S: NodeStorage<K, V, KC, VC> + MetadataStorage + Send + Sync + 'static,
    {
        Self {
            staged_update: {
                // No staged update initially
                let res = tree.get_snapshot();
                Some(StagedMetadata {
                    root_id: res.root_id,
                    height: res.height,
                    size: res.size,
                })
            },
            staged_nodes: Vec::new(),
            tree_base_version: BaseVersion { committed_ptr: tree.get_metadata_ptr() },
            initial_root_id: tree.get_root_id(),
            changes: Vec::new(),
            reclaimed_nodes: Vec::new(),
        }
    }
    // Get the root ID of the intermediate staged tree, if there is one, otherwise return the
    // current root ID
    pub fn get_root_id(&self) -> u64 {
        self.staged_update
            .as_ref()
            .map_or(self.initial_root_id, |res| res.root_id)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.changes.push(WriteOp::Insert(key, value));
        Ok(())
    }

    // Stage only.
    pub fn delete(&mut self, key: &K) -> Result<()> {
        self.changes.push(WriteOp::Delete(key.clone()));
        Ok(())
    }

//    pub fn commit(&mut self) -> Result<TxnStatus> {
//        for _ in 0..MAX_COMMIT_RETRIES {
//            let staged_update = self
//                .staged_update
//                .take()
//                .expect("Staged update should be set before commit");
//            let res = self.tree.try_commit(&self.tree_base_version, staged_update);
//            if res.is_ok() {
//                self.changes.clear();
//                // Add all staged nodes to the epoch manager for reclamation, we use epoch 0 as
//                // there will no longer be any active readers for this transaction
//                for node_id in self.reclaimed_nodes.drain(..) {
//                    self.tree.get_epoch_mgr().add_reclaim_candidate(0, node_id);
//                }
//                return Ok(TxnStatus::Committed);
//            } else {
//                // Root changed — retry entire transaction
//                self.initial_root_id = self.tree.get_root_id(); // Update initial root ID
//                self.tree_base_version = BaseVersion {
//                    committed_ptr: self.tree.get_metadata_ptr(),
//                };
//                self.reclaimed_nodes.clear(); // reset collected reclaim info
//                for node_id in self.staged_nodes.drain(..) {
//                    self.tree.get_epoch_mgr().add_reclaim_candidate(0, node_id);
//                }
//                self.rebase()?;
//            }
//        }
//        Ok(TxnStatus::Aborted) // Too many retries, abort transaction
//    }

    // Replay staged ops from base/root; tree handles encoding inside.
    pub fn commit<KC, VC, S>(&mut self, tree: &SharedBPlusTree<K, V, KC, VC, S>) -> Result<TxnStatus>
    where
        KC: crate::codec::KeyCodec<K>,
        VC: crate::codec::ValueCodec<V>,
        S: NodeStorage<K, V, KC, VC> + MetadataStorage + Send + Sync + 'static,
    {
        for _ in 0..MAX_COMMIT_RETRIES {
            // Rebuild speculative state by replaying changes from the saved base root.
            let mut staged_update: Option<StagedMetadata> = None;
            let mut staged_nodes: Vec<u64> = Vec::new();
            let mut reclaimed_nodes_local: Vec<u64> = Vec::new();
            let mut current_root = self.initial_root_id;

            for op in &self.changes {
                match op {
                    WriteOp::Insert(k, v) => {
                        let wr = tree.insert_with_root(k.clone(), v.clone(), current_root)?;
                        reclaimed_nodes_local.extend(wr.reclaimed_nodes);
                        staged_nodes.extend(wr.staged_nodes);
                        current_root = wr.new_root_id;
                        staged_update = Some(StagedMetadata {
                            root_id: wr.new_root_id,
                            height: wr.new_height,
                            size: wr.new_size,
                        });
                    }
                    WriteOp::Delete(k) => {
                        let wr = tree.delete_with_root(k, current_root)?;
                        reclaimed_nodes_local.extend(wr.reclaimed_nodes);
                        staged_nodes.extend(wr.staged_nodes);
                        current_root = wr.new_root_id;
                        staged_update = Some(StagedMetadata {
                            root_id: wr.new_root_id,
                            height: wr.new_height,
                            size: wr.new_size,
                        });
                    }
                }
            }

            let staged_update = match staged_update {
                Some(su) => su,
                None => {
                    // No ops: try to publish a no-op metadata (same root) if your API requires it,
                    // or just return early. Here we no-op-commit to keep the flow consistent.
                    StagedMetadata {
                        root_id: current_root,
                        height: tree.get_height(),
                        size: tree.get_size(),
                    }
                }
            };

            let res = tree.try_commit(&self.tree_base_version, staged_update);
            if res.is_ok() {
                // Publish all reclaimed pages after success.
                for id in reclaimed_nodes_local.drain(..) {
                    tree.get_epoch_mgr().add_reclaim_candidate(0, id);
                }
                self.reclaimed_nodes.clear();
                self.changes.clear();
                return Ok(TxnStatus::Committed);
            } else {
                // Conflict: clean up speculative nodes, refresh base+root, and retry.
                for id in staged_nodes.drain(..) {
                    tree.get_epoch_mgr().add_reclaim_candidate(0, id);
                }
                self.reclaimed_nodes.clear();
                self.tree_base_version = BaseVersion { committed_ptr: tree.get_metadata_ptr() };
                self.initial_root_id = tree.get_root_id();
             }
         }
         Ok(TxnStatus::Aborted) // Too many retries, abort transaction
     }

    #[cfg(test)]
    pub fn get_reclaimed_nodes(&self) -> Vec<u64> {
        self.reclaimed_nodes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplustree::tree::CommitError;
    use crate::metadata::Metadata;
    use crate::codec::bincode::{BeU64, RawBuf};
    use crate::tests::common::{test_storage::TestStorage, test_tree};

    #[test]
    fn cas_mismatch_returns_rebase_required_with_no_side_effects() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, u64, BeU64, BeU64, TestStorage>(storage, 128);
        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };

        // Simulate another writer already published
        #[cfg(any(test, feature = "testing"))]
        h.tree.test_force_publish(&Metadata {
            root_node_id: 99,
            height: 2,
            size: 5,
            txn_id: 1,
            order: 128,
            checksum: 0,
        });

        let err = h.tree.try_commit(
            &base,
            StagedMetadata {
                root_id: 100,
                height: 3,
                size: 6,
            },
        );
        assert!(matches!(err, Err(CommitError::RebaseRequired)));

        assert_eq!(h.storage.flush_count(), 0);
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 99);
        assert_eq!(m.txn_id, 1);
    }

    #[test]
    fn metadata_write_failure_aborts_before_publish() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, BeU64, RawBuf, TestStorage>(storage, 128);
        h.storage.inject_commit_failure(true);

        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };
        let err = h
            .tree
            .try_commit(
                &base,
                StagedMetadata {
                    root_id: 2,
                    height: 2,
                    size: 2,
                },
            )
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
        let h = test_tree::<u64, Vec<u8>, BeU64, RawBuf, TestStorage>(storage, 128);
        h.storage.inject_flush_failure(true);

        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };
        let err = h
            .tree
            .try_commit(
                &base,
                StagedMetadata {
                    root_id: 7,
                    height: 4,
                    size: 11,
                },
            )
            .unwrap_err();
        assert!(matches!(err, CommitError::Io(_)));

        // State already published
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 7);
        assert_eq!(m.txn_id, 2);
    }

    #[test]
    fn gc_runs_after_success() {
        let storage = TestStorage::new();
        let h = test_tree::<u64, Vec<u8>, BeU64, RawBuf, TestStorage>(storage, 128);
        // Epoch will be advance after commit
        h.tree.get_epoch_mgr().set_oldest_active(1);
        h.tree.get_epoch_mgr().set_reclaim_list(1, vec![10, 11, 12]);

        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };
        h.tree
            .try_commit(
                &base,
                StagedMetadata {
                    root_id: 555,
                    height: 3,
                    size: 9,
                },
            )
            .unwrap();

        assert_eq!(h.storage.freed_pages(), vec![10, 11, 12]);
    }

    #[test]
    fn published_metadata_is_visible_immediately() {
        let storage = TestStorage::new(); // Reset the test storage state
        let h = test_tree::<u64, Vec<u8>, BeU64, RawBuf, TestStorage>(storage, 128);
        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };
        h.tree
            .try_commit(
                &base,
                StagedMetadata {
                    root_id: 777,
                    height: 9,
                    size: 123,
                },
            )
            .unwrap();

        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 777);
        assert_eq!(m.height, 9);
        assert_eq!(m.size, 123);
    }
}
