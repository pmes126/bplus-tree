//! Embedded database façade.
//!
//! `Db::open` initialises a [`Database`] and exposes `create_tree` / `open_tree`
//! to obtain typed [`Tree`] handles. All storage details are encapsulated inside
//! the [`Database`] layer — this module never touches storage types directly.

use std::marker::PhantomData;
use std::path::Path;

use crate::api::ApiError;
use crate::bplustree::iterator::BPlusTreeIter;
use crate::bplustree::transaction::{TxnStatus, WriteTransaction};
use crate::bplustree::tree::SharedBPlusTree;
use crate::codec::kv::{KeyCodec, ValueCodec};
use crate::database::{self, Database};
use crate::keyfmt::KeyFormat;
use crate::keyfmt::raw::RawFormat;
use crate::storage::file_page_storage::FilePageStorage;
use crate::storage::paged_node_storage::PagedNodeStorage;

type InnerTree = SharedBPlusTree<'static, PagedNodeStorage<FilePageStorage>, FilePageStorage>;

/// Embedded database handle.
///
/// The inner [`Database`] is intentionally leaked (`Box::leak`) so that trees
/// can hold `&'static` references to storage. Call [`Db::close`] to reclaim
/// the allocation when the database is no longer needed.
pub struct Db {
    database: &'static Database<FilePageStorage>,
}

// SAFETY: Database<FilePageStorage> is Send+Sync (FilePageStorage uses Arc<File> + atomics).
// The &'static ref is just a leaked Box, safe to share.
unsafe impl Send for Db {}
unsafe impl Sync for Db {}

impl Db {
    /// Opens (or creates) the database rooted at `dir`.
    ///
    /// The directory must already exist. On first open the data file and
    /// manifest are created automatically.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, ApiError> {
        let db = database::open::<FilePageStorage, _>(dir)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        let database: &'static Database<FilePageStorage> = Box::leak(Box::new(db));
        Ok(Self { database })
    }

    /// Creates a new named tree and returns a typed handle.
    pub fn create_tree<K, V>(&self, name: &str, order: u64) -> Result<Tree<K, V>, ApiError>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let key_format = KeyFormat::Raw(RawFormat);
        let tree_meta = self
            .database
            .create_tree(name, K::ENCODING, key_format, order, None)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        let inner = self
            .database
            .bind_tree(&tree_meta)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        Ok(Tree {
            inner,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Opens an existing named tree from the catalog.
    pub fn open_tree<K, V>(&self, name: &str) -> Result<Tree<K, V>, ApiError>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let tree_meta = self
            .database
            .describe_tree(name)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        let inner = self
            .database
            .bind_tree(&tree_meta)
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        Ok(Tree {
            inner,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Opens an existing tree, or creates one with the given `order` if it
    /// does not exist yet.
    pub fn tree<K, V>(&self, name: &str, order: u64) -> Result<Tree<K, V>, ApiError>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        match self.open_tree(name) {
            Ok(t) => Ok(t),
            Err(_) => self.create_tree(name, order),
        }
    }

    /// Reclaims the leaked [`Database`] allocation.
    ///
    /// # Safety
    ///
    /// The caller must ensure that **no** [`Tree`], [`WriteTxn`], or
    /// [`RangeIter`] handles derived from this `Db` are still alive.
    /// Using a tree handle after `close` is undefined behaviour.
    pub unsafe fn close(self) {
        // Persist freelist so freed pages survive restart.
        if let Err(e) = self.database.checkpoint_freelist() {
            eprintln!("warning: failed to checkpoint freelist: {e}");
        }
        let ptr =
            self.database as *const Database<FilePageStorage> as *mut Database<FilePageStorage>;
        drop(unsafe { Box::from_raw(ptr) });
    }
}

// ---------------------------------------------------------------------------
// Tree<K, V>
// ---------------------------------------------------------------------------

/// Typed handle to a single B+ tree inside a [`Db`].
pub struct Tree<K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    inner: InnerTree,
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,
}

impl<K, V> Tree<K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    /// Inserts or replaces the value for `key`.
    pub fn put(&self, key: &K, value: &V) -> Result<(), ApiError> {
        let mut txn = WriteTransaction::new(self.inner.clone());
        txn.insert(key.encode(), value.encode());
        match txn.commit(&self.inner)? {
            TxnStatus::Committed => Ok(()),
            TxnStatus::Aborted => Err(ApiError::TxnAborted),
        }
    }

    /// Returns the value for `key`, or `None` if the key is absent.
    pub fn get(&self, key: &K) -> Result<Option<V>, ApiError> {
        let kb = key.encode();
        match self.inner.search(kb)? {
            Some(bytes) => Ok(Some(V::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Deletes the value for `key`. Returns an error if the key is not found.
    pub fn delete(&self, key: &K) -> Result<(), ApiError> {
        let mut txn = WriteTransaction::new(self.inner.clone());
        txn.delete(key.encode());
        match txn.commit(&self.inner)? {
            TxnStatus::Committed => Ok(()),
            TxnStatus::Aborted => Err(ApiError::TxnAborted),
        }
    }

    /// Starts a batched write transaction.
    pub fn txn(&self) -> WriteTxn<'_, K, V> {
        WriteTxn {
            inner: WriteTransaction::new(self.inner.clone()),
            tree: self,
        }
    }

    /// Returns a forward range iterator from `start` (inclusive) to `end`
    /// (exclusive).
    pub fn range(&self, start: &K, end: &K) -> Result<RangeIter<'_, K, V>, ApiError> {
        let start_bytes = start.encode();
        let end_bytes = end.encode();
        let inner = self.inner.search_range(&start_bytes, Some(&end_bytes))?;
        Ok(RangeIter {
            inner,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Returns a forward range iterator from `start` (inclusive) to the end
    /// of the tree.
    pub fn range_from(&self, start: &K) -> Result<RangeIter<'_, K, V>, ApiError> {
        let start_bytes = start.encode();
        let inner = self.inner.search_range(&start_bytes, None)?;
        Ok(RangeIter {
            inner,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Returns the number of entries currently in the tree.
    pub fn len(&self) -> u64 {
        self.inner.get_size()
    }

    /// Returns true if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// WriteTxn
// ---------------------------------------------------------------------------

/// Batched write transaction with optimistic CAS commit.
pub struct WriteTxn<'t, K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    inner: WriteTransaction,
    tree: &'t Tree<K, V>,
}

impl<'t, K, V> WriteTxn<'t, K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    /// Stages an insert of `key` → `value`.
    pub fn insert(&mut self, key: &K, value: &V) {
        self.inner.insert(key.encode(), value.encode());
    }

    /// Stages a delete of `key`.
    pub fn delete(&mut self, key: &K) {
        self.inner.delete(key.encode());
    }

    /// Commits all staged operations. Returns `Err(ApiError::TxnAborted)` if
    /// the retry budget is exhausted.
    pub fn commit(mut self) -> Result<(), ApiError> {
        match self.inner.commit(&self.tree.inner)? {
            TxnStatus::Committed => Ok(()),
            TxnStatus::Aborted => Err(ApiError::TxnAborted),
        }
    }
}

// ---------------------------------------------------------------------------
// RangeIter
// ---------------------------------------------------------------------------

type InnerNodeStorage = PagedNodeStorage<FilePageStorage>;

/// Typed forward-range iterator over `(K, V)` pairs.
///
/// Wraps a bytes-level [`BPlusTreeIter`] and decodes keys and values via
/// [`KeyCodec`] / [`ValueCodec`].
pub struct RangeIter<'t, K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    inner: BPlusTreeIter<'t, InnerNodeStorage>,
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,
}

impl<'t, K, V> Iterator for RangeIter<'t, K, V>
where
    K: KeyCodec,
    V: ValueCodec,
{
    type Item = Result<(K, V), ApiError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key_bytes, val_bytes) = match self.inner.next()? {
            Ok(pair) => pair,
            Err(e) => return Some(Err(e.into())),
        };
        let key = match K::decode(&key_bytes) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };
        let value = match V::decode(&val_bytes) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok((key, value)))
    }
}
