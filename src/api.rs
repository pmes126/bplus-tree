//! Embedded API: bytes-first wrapper + typed façade built on SharedBPlusTree.
//!
//! - DbBytes<S>: keys/values are Vec<u8> (order = lexicographic).
//! - TypedDb<K,V>: uses internal KeyCodec/ValueCodec that are already implemented.
//!
//! This module is intentionally sync and embedded-only.

//! Embedded API: bytes-first + typed façade over SharedBPlusTree.
//! DbBytes/TypedDb, streaming iterators, and a write txn.

use std::marker::PhantomData;

use crate::bplustree::iterator::BPlusTreeIter;
use crate::bplustree::tree::{BPlusTree, BaseVersion, SharedBPlusTree, StagedMetadata};
use crate::storage::{MetadataStorage, NodeStorage};

pub use crate::bplustree::transaction::WriteTransaction as WriteTxn;

use std::fmt::Debug;

// ============================
// KV Error type
// ============================

pub use crate::bplustree::tree::{CommitError, TreeError};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ApiError {
    #[error(transparent)]
    Internal(#[from] TreeError),
    #[error(transparent)]
    Commit(#[from] CommitError),
}

pub type Result<T> = std::result::Result<T, ApiError>;

// ============================
// Bytes-level DB (Vec<u8>)
// ============================

#[derive(Clone)]
pub struct DbBytes<S>
where
    S: NodeStorage<Vec<u8>, Vec<u8>> + MetadataStorage + Send + Sync + 'static,
{
    inner: SharedBPlusTree<Vec<u8>, Vec<u8>, S>,
}

impl<S> DbBytes<S>
where
    S: NodeStorage<Vec<u8>, Vec<u8>> + MetadataStorage + Send + Sync + 'static,
{
    /// Build from a storage backend and a B+tree order.
    pub fn new(storage: S, order: usize) -> Result<Self> {
        let tree = BPlusTree::<Vec<u8>, Vec<u8>, S>::new(storage, order)?;
        Ok(Self {
            inner: SharedBPlusTree::new(tree),
        })
    }

    /// Get by raw key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.search(&key.to_vec()).map_err(ApiError::from)
    }

    /// Put raw key/value.
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let base_version = BaseVersion {
            committed_ptr: self.inner.get_metadata_ptr(),
        };
        let res = self.inner.insert(key.to_vec(), val.to_vec())?;
        let staged_update = StagedMetadata {
            root_id: res.new_root_id,
            height: res.new_height,
            size: res.new_size,
        };
        self.inner.try_commit(&base_version, staged_update)?;
        Ok(())
    }

    /// Delete by key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let root_id = self.inner.get_root_id();
        let base_version = BaseVersion {
            committed_ptr: self.inner.get_metadata_ptr(),
        };
        let res = self.inner.delete_with_root(&key.to_vec(), root_id)?;

        let staged_update = StagedMetadata {
            root_id: res.new_root_id,
            height: res.new_height,
            size: res.new_size,
        };
        self.inner.try_commit(&base_version, staged_update)?;
        Ok(())
    }

    /// Streaming scan over [start, end). Returns None if tree is empty.
    pub fn scan_range<'a>(&'a self, start: &[u8], end: &[u8]) -> Result<Option<BytesIter<'a, S>>> {
        let it_opt = self.inner.search_in_range(&start.to_vec(), &end.to_vec())?;
        Ok(it_opt.map(|inner| BytesIter { inner }))
    }

    /// Collect up to `limit` pairs in [start, end).
    pub fn scan_range_collect(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        if let Some(mut it) = self.scan_range(start, end)? {
            let mut out = Vec::with_capacity(limit.min(1024));
            for _ in 0..limit {
                if let Some(kv) = it.next() {
                    out.push(kv?);
                } else {
                    break;
                }
            }
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }

    /// Begin a batched write transaction (single commit).
    pub fn begin_write(&self) -> Result<WriteTxnBytes> {
        Ok(WriteTxnBytes::new(self.inner.clone()))
    }

    pub fn get_inner(&self) -> &SharedBPlusTree<Vec<u8>, Vec<u8>, S> {
        &self.inner
    }
}

// Streaming iterator (bytes)
pub struct BytesIter<'a, S>
where
    S: NodeStorage<Vec<u8>, Vec<u8>> + MetadataStorage + Send + Sync + 'static,
{
    inner: BPlusTreeIter<'a, Vec<u8>, Vec<u8>, S>,
}

impl<'a, S> Iterator for BytesIter<'a, S>
where
    S: NodeStorage<Vec<u8>, Vec<u8>> + MetadataStorage + Send + Sync + 'static,
{
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| res.map_err(ApiError::from))
    }
}

// ============================
// Typed façade (K,V with codecs)
// ============================

#[derive(Clone)]
pub struct TypedDb<K, V, S>
where
    K: Ord + Clone,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    inner: SharedBPlusTree<K, V, S>,
    _pd: PhantomData<(K, V)>,
}

impl<K, V, S> TypedDb<K, V, S>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    /// Build typed DB from your existing typed tree.
    pub fn from_tree(tree: BPlusTree<K, V, S>) -> Self {
        Self {
            inner: SharedBPlusTree::new(tree).clone(),
            _pd: PhantomData,
        }
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.inner.search(key).map_err(ApiError::from)
    }

    pub fn put(&self, key: K, val: V) -> Result<()> {
        let base_version = BaseVersion {
            committed_ptr: self.inner.get_metadata_ptr(),
        };
        let res = self.inner.insert(key, val)?;
        let staged_update = StagedMetadata {
            root_id: res.new_root_id,
            height: res.new_height,
            size: res.new_size,
        };
        self.inner.try_commit(&base_version, staged_update)?;
        Ok(())
    }

    pub fn delete(&self, key: &K) -> Result<()> {
        let root_id = self.inner.get_root_id();
        let base_version = BaseVersion {
            committed_ptr: self.inner.get_metadata_ptr(),
        };
        let res = self.inner.delete_with_root(&key, root_id)?;

        let staged_update = StagedMetadata {
            root_id: res.new_root_id,
            height: res.new_height,
            size: res.new_size,
        };
        self.inner.try_commit(&base_version, staged_update)?;
        Ok(())
    }

    pub fn scan_range<'a>(&'a self, start: &K, end: &K) -> Result<Option<TypedIter<'a, K, V, S>>> {
        let it_opt = self.inner.search_in_range(start, end)?;
        Ok(it_opt.map(|inner| TypedIter { inner }))
    }

    pub fn begin_write(&self) -> Result<TypedWriteTxn<K, V>> {
        Ok(TypedWriteTxn::new(self.inner.clone()))
    }

    pub fn get_inner(&self) -> &SharedBPlusTree<K, V, S> {
        &self.inner
    }
}

// Streaming iterator (typed)
pub struct TypedIter<'a, K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    inner: BPlusTreeIter<'a, K, V, S>,
}

impl<'a, K, V, S> Iterator for TypedIter<'a, K, V, S>
where
    K: Clone + Ord,
    V: Clone,
    S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
{
    type Item = Result<(K, V)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| res.map_err(ApiError::from))
    }
}

// ============================================================

/// Options for opening the embedded DB.
#[derive(Clone, Debug)]
pub struct DbOptions {
    /// B+-tree order / max fanout (children per internal node).
    pub order: usize,
    // room for more: page_size, fsync, cache_cap, prealloc, etc.
}
impl Default for DbOptions {
    fn default() -> Self {
        Self { order: 64 }
    }
}

/// Generic builder over a concrete storage backend `S`.
pub struct DbBuilder<S> {
    storage: S,
    opts: DbOptions,
}

impl<S> DbBuilder<S> {
    /// Start a builder with a storage backend (configure the storage itself upstream).
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            opts: DbOptions::default(),
        }
    }

    /// Replace the full options bag.
    pub fn options(mut self, opts: DbOptions) -> Self {
        self.opts = opts;
        self
    }

    /// Set just the B+-tree order (fanout).
    pub fn order(mut self, order: usize) -> Self {
        self.opts.order = order;
        self
    }

    /// Build the **bytes-level** API (Vec<u8> keys/values).
    pub fn build_bytes(self) -> Result<DbBytes<S>>
    where
        S: NodeStorage<Vec<u8>, Vec<u8>> + MetadataStorage + Send + Sync + 'static,
    {
        DbBytes::new(self.storage, self.opts.order)
    }

    /// Build the **typed** API using your KeyCodec/ValueCodec.
    pub fn build_typed<K, V>(self) -> Result<TypedDb<K, V, S>>
    where
        K: Ord + Clone + Debug,
        V: Clone + Debug,
        S: NodeStorage<K, V> + MetadataStorage + Send + Sync + 'static,
    {
        let tree = BPlusTree::<K, V, S>::new(self.storage, self.opts.order)?;
        Ok(TypedDb::from_tree(tree))
    }
}

// ============================================================

/// Bytes-level write txn (Vec<u8>, Vec<u8>)
pub type WriteTxnBytes = WriteTxn<Vec<u8>, Vec<u8>>;

/// Typed write txn (K, V)
pub type TypedWriteTxn<K, V> = WriteTxn<K, V>;
