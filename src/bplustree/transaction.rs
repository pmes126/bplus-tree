//! Write transaction for the B+ tree with optimistic concurrency control.

use crate::bplustree::tree::{
    BaseVersion, SharedBPlusTree, StagedMetadata, TransactionTracker, TreeError,
};
use crate::storage::{HasEpoch, NodeStorage, PageStorage};

/// A single buffered write operation.
enum WriteOp<K, V> {
    Insert(K, V),
    Delete(K),
}

impl<K: AsRef<[u8]>, V> WriteOp<K, V> {
    fn key(&self) -> &[u8] {
        match self {
            WriteOp::Insert(k, _) => k.as_ref(),
            WriteOp::Delete(k) => k.as_ref(),
        }
    }
}

/// Indicates whether a transaction committed successfully or was aborted.
pub enum TxnStatus {
    /// Transaction was committed.
    Committed,
    /// Transaction was aborted after exceeding the maximum retry count.
    Aborted,
}

/// Maximum number of CAS retries before aborting a transaction.
pub const MAX_COMMIT_RETRIES: usize = 10;

/// Buffers a set of writes and commits them atomically via optimistic CAS.
pub struct WriteTransaction {
    /// Speculative metadata staged during this transaction.
    staged_update: Option<StagedMetadata>,
    /// Committed metadata pointer captured at transaction start.
    tree_base_version: BaseVersion,
    changes: Vec<WriteOp<Vec<u8>, Vec<u8>>>,
    /// Node IDs pending reclamation after a successful commit.
    reclaimed_nodes: Vec<u64>,
    /// Root node ID captured at transaction start.
    initial_root_id: u64,
}

impl WriteTransaction {
    /// Creates a new transaction rooted at the tree's current committed state.
    pub fn new<S, P>(tree: SharedBPlusTree<S, P>) -> Self
    where
        S: NodeStorage + HasEpoch + Send + Sync + 'static,
        P: PageStorage + Send + Sync + 'static,
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
            tree_base_version: BaseVersion {
                committed_ptr: tree.get_metadata_ptr(),
            },
            initial_root_id: tree.get_root_id(),
            changes: Vec::new(),
            reclaimed_nodes: Vec::new(),
        }
    }
    /// Returns the root ID of the staged tree, or the initial root if no writes have been staged.
    pub fn get_root_id(&self) -> u64 {
        self.staged_update
            .as_ref()
            .map_or(self.initial_root_id, |res| res.root_id)
    }

    /// Buffers an insert of the given key-value pair, maintaining sorted key order.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        let k = key.as_ref().to_vec();
        let pos = self.changes.partition_point(|op| op.key() <= k.as_slice());
        self.changes
            .insert(pos, WriteOp::Insert(k, value.as_ref().to_vec()));
    }

    /// Buffers a delete of the given key, maintaining sorted key order.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let k = key.as_ref().to_vec();
        let pos = self.changes.partition_point(|op| op.key() <= k.as_slice());
        self.changes.insert(pos, WriteOp::Delete(k));
    }

    /// Replays buffered operations and attempts to commit via CAS, retrying on conflicts.
    pub fn commit<S, P>(&mut self, tree: &SharedBPlusTree<S, P>) -> Result<TxnStatus, TreeError>
    where
        S: NodeStorage + HasEpoch + Send + Sync + 'static,
        P: PageStorage + Send + Sync + 'static,
    {
        for _ in 0..MAX_COMMIT_RETRIES {
            // Pin the epoch *before* reading root_id so that the pages
            // reachable from that root cannot be reclaimed while we walk them.
            // The guard is dropped before try_commit so that the commit's
            // epoch advance + reclamation pass is not blocked by our pin.
            let staged_update;
            let mut staged_nodes: Vec<u64> = Vec::new();
            let mut reclaimed_nodes_local: Vec<u64> = Vec::new();

            {
                let _guard = tree.epoch_mgr().pin();
                let mut current_root = self.initial_root_id;
                let mut staged: Option<StagedMetadata> = None;
                let mut tracker = TransactionTracker::new();

                for op in &self.changes {
                    match op {
                        WriteOp::Insert(k, v) => {
                            let wr = tree.put_with_root_tracked(
                                k.clone(),
                                v.clone(),
                                current_root,
                                &mut tracker,
                            )?;
                            reclaimed_nodes_local.extend(wr.reclaimed_nodes);
                            staged_nodes.extend(wr.staged_nodes);
                            current_root = wr.new_root_id;
                            staged = Some(StagedMetadata {
                                root_id: wr.new_root_id,
                                height: wr.new_height,
                                size: wr.new_size,
                            });
                        }
                        WriteOp::Delete(k) => {
                            let wr =
                                tree.delete_with_root_tracked(k, current_root, &mut tracker)?;
                            reclaimed_nodes_local.extend(wr.reclaimed_nodes);
                            staged_nodes.extend(wr.staged_nodes);
                            current_root = wr.new_root_id;
                            staged = Some(StagedMetadata {
                                root_id: wr.new_root_id,
                                height: wr.new_height,
                                size: wr.new_size,
                            });
                        }
                    }
                }

                staged_update = staged;
            } // _guard dropped here — epoch unpinned before commit

            let staged_update = match staged_update {
                Some(su) => su,
                None => {
                    // No ops were applied; commit a no-op to keep the flow consistent.
                    StagedMetadata {
                        root_id: self.initial_root_id,
                        height: tree.get_height(),
                        size: tree.get_size(),
                    }
                }
            };

            let res = tree.try_commit(&self.tree_base_version, staged_update);
            if res.is_ok() {
                // Register reclaimed (old) pages for deferred freeing at the
                // current epoch.  Readers pinned at an earlier epoch may still
                // be walking the old tree, so these pages must not be freed
                // until all such readers have unpinned.
                let epoch = tree.epoch_mgr().current();
                for id in reclaimed_nodes_local.drain(..) {
                    tree.epoch_mgr().add_reclaim_candidate(epoch, id);
                }
                self.reclaimed_nodes.clear();
                self.changes.clear();
                return Ok(TxnStatus::Committed);
            } else {
                // CAS conflict: discard speculative nodes, refresh base and root, then retry.
                // These pages were never published so no reader can reference them since global
                // epoch starts at 1;
                // epoch 0 ensures they are freed on the next reclamation pass.
                for id in staged_nodes.drain(..) {
                    tree.epoch_mgr().add_reclaim_candidate(0, id);
                }
                self.reclaimed_nodes.clear();
                self.tree_base_version = BaseVersion {
                    committed_ptr: tree.get_metadata_ptr(),
                };
                self.initial_root_id = tree.get_root_id();
            }
        }
        Ok(TxnStatus::Aborted)
    }

    #[cfg(test)]
    /// Returns the list of reclaimed node IDs; for testing only.
    pub fn get_reclaimed_nodes(&self) -> Vec<u64> {
        self.reclaimed_nodes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplustree::tree::CommitError;
    use crate::database::metadata::Metadata;
    use crate::tests::common::{test_storage::TestStorage, test_tree};

    #[test]
    fn cas_mismatch_returns_rebase_required_with_no_side_effects() {
        let storage = TestStorage::new();
        let h = test_tree::<TestStorage>(storage, 128);
        let base = BaseVersion {
            committed_ptr: h.tree.metadata_ptr(),
        };

        // Simulate another writer already published
        #[cfg(any(test, feature = "testing"))]
        h.tree.test_force_publish(&Metadata {
            id: 1,
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
        let storage = TestStorage::new();
        let h = test_tree::<TestStorage>(storage, 128);
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
        assert!(matches!(err, CommitError::Metadata(_)));

        // No publish, no flush, no epoch advance
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 0);
        assert_eq!(h.storage.flush_count(), 0);
    }

    #[test]
    fn flush_failure_after_publish_keeps_state() {
        let storage = TestStorage::new();
        let h = test_tree::<TestStorage>(storage, 128);
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
        assert!(matches!(err, CommitError::Storage(_)));

        // State already published
        let m = h.tree.metadata();
        assert_eq!(m.root_node_id, 7);
        assert_eq!(m.txn_id, 2);
    }

    #[test]
    fn gc_runs_after_success() {
        let storage = TestStorage::new();
        let h = test_tree::<TestStorage>(storage, 128);
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
        let storage = TestStorage::new();
        let h = test_tree::<TestStorage>(storage, 128);
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
