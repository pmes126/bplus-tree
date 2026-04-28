//! Database layer: catalog, manifest, metadata, and superblock management.
//!
//! [`Database`] owns one [`PageStorage`] instance (via `Arc<S>`) shared between:
//! - `node_storage: PagedNodeStorage<S>` — pluggable node encoding strategy
//! - `meta_storage: Arc<S>` — raw page I/O for metadata slots and superblock
//!
//! Both point to the same `Arc<S>`, so page-allocation counters stay in sync.

pub mod catalog;
pub mod manifest;
pub mod metadata;
pub mod superblock;

use crate::api::{KeyEncodingId, KeyLimits, TreeId};
use crate::bplustree::NodeView;
use crate::bplustree::tree::{BPlusTree, SharedBPlusTree};
use crate::database::catalog::{Catalog, TreeMeta};
use crate::database::manifest::ManifestRec;
use crate::database::manifest::reader::ManifestReader;
use crate::database::manifest::writer::ManifestWriter;
use crate::database::metadata::Metadata;
use crate::database::superblock::{
    FREELIST_SNAPSHOT_VERSION, SUPERBLOCK_MAGIC, SUPERBLOCK_VERSION, Superblock,
    read_freepages_snapshot, write_freepages_snapshot,
};
use crate::keyfmt::KeyFormat;
use crate::layout::PAGE_SIZE;
use crate::page::LeafPage;
use crate::storage::epoch::EpochManager;
use crate::storage::metadata_manager::MetadataManager;
use crate::storage::paged_node_storage::PagedNodeStorage;
use crate::storage::{NodeStorage, PageStorage};

use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use thiserror::Error;
use zerocopy::AsBytes;

/// Errors that can occur during database operations.
#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
    #[error("metadata error: {0}")]
    Metadata(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("version mismatch: {0}")]
    VersionMismatch(String),
    #[error("database is locked by another process")]
    Locked,
}

/// Page 0 is reserved for the superblock.
const SUPERBLOCK_PAGE: u64 = 0;

/// Holds all open state for a single database instance.
///
/// A single `Arc<S>` page-storage instance is shared between `node_storage`
/// (pluggable node encoding) and `meta_storage` (raw metadata / superblock I/O),
/// keeping page-allocation counters in sync.
pub struct Database<S: PageStorage + Send + Sync + 'static> {
    node_storage: Arc<PagedNodeStorage<S>>,
    meta_storage: Arc<S>,
    epoch_mgr: Arc<EpochManager>,
    manifest: Mutex<ManifestWriter>,
    catalog: RwLock<Catalog>,
    format_version: u32,
    base_path: std::path::PathBuf,
    /// Exclusive lock file handle. Held for the lifetime of the database to
    /// prevent concurrent access from another process. The `flock` is released
    /// automatically when this `File` is dropped.
    _lock_file: File,
}

impl<S: PageStorage + Send + Sync + 'static> Database<S> {
    // -----------------------------------------------------------------
    // Tree lifecycle
    // -----------------------------------------------------------------

    /// Creates a new named tree: writes an empty root leaf, seeds the A/B
    /// metadata pages, appends a manifest record, and updates the catalog.
    pub fn create_tree(
        &self,
        name: &str,
        enc: KeyEncodingId,
        key_format: KeyFormat,
        order: u64,
        limits: Option<KeyLimits>,
    ) -> Result<TreeMeta, DatabaseError> {
        let id = self.alloc_tree_id(name);

        // Allocate metadata page slots via the raw page storage.
        let meta_a = self.meta_storage.as_ref().allocate_page()?;
        let meta_b = self.meta_storage.as_ref().allocate_page()?;

        // Write an initial empty root leaf via the node storage.
        let root_view = NodeView::Leaf {
            page: LeafPage::new(key_format),
            page_id: None,
        };
        let root_id = self.node_storage.write_node_view(&root_view)?;

        // Seed both metadata pages.
        let init_meta = Metadata {
            root_node_id: root_id,
            id,
            txn_id: 1,
            height: 1,
            order,
            size: 0,
            checksum: 0,
        };
        MetadataManager::commit_metadata_with_object(
            self.meta_storage.as_ref(),
            meta_a,
            &init_meta,
        )
        .map_err(|e| DatabaseError::Metadata(e.to_string()))?;
        MetadataManager::commit_metadata_with_object(
            self.meta_storage.as_ref(),
            meta_b,
            &init_meta,
        )
        .map_err(|e| DatabaseError::Metadata(e.to_string()))?;

        // Append manifest record.
        let rec = ManifestRec::CreateTree {
            seq: 0,
            id,
            name: name.to_string(),
            key_format,
            key_encoding: enc,
            encoding_version: 1,
            key_limits: limits,
            meta_a,
            meta_b,
            order,
            root_id,
            height: 1,
            size: 0,
        };

        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(rec.clone())?;
        w.fsync()?;
        drop(w);

        // Replay into catalog with the assigned sequence number.
        let mut committed = rec;
        if let ManifestRec::CreateTree { seq: s, .. } = &mut committed {
            *s = seq;
        }

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&committed);

        cat.metas
            .get(&id)
            .cloned()
            .ok_or_else(|| DatabaseError::NotFound("tree not found after create".into()))
    }

    /// Looks up a tree by ID and returns its catalog metadata.
    pub fn open_tree(&self, id: &TreeId) -> Result<TreeMeta, DatabaseError> {
        let cat = self.catalog.read().unwrap();
        cat.get_by_id(id)
            .cloned()
            .ok_or_else(|| DatabaseError::NotFound("tree not found".into()))
    }

    /// Looks up a tree by name and returns its catalog metadata.
    pub fn describe_tree(&self, name: &str) -> Result<TreeMeta, DatabaseError> {
        let cat = self.catalog.read().unwrap();
        cat.get_by_name(name)
            .cloned()
            .ok_or_else(|| DatabaseError::NotFound(format!("tree {name:?} not found")))
    }

    /// Builds a [`SharedBPlusTree`] backed by this database's storage.
    ///
    /// Storage is shared via `Arc`, so the returned tree is independently owned
    /// and can outlive the borrow on `&self`.
    pub fn bind_tree(
        &self,
        tree_meta: &TreeMeta,
    ) -> Result<SharedBPlusTree<PagedNodeStorage<S>, S>, DatabaseError> {
        let meta = MetadataManager::read_active_meta(
            self.meta_storage.as_ref(),
            tree_meta.meta_a,
            tree_meta.meta_b,
        )
        .map_err(|e| DatabaseError::Metadata(e.to_string()))?;

        let bpt = BPlusTree::open(
            Arc::clone(&self.node_storage),
            Arc::clone(&self.meta_storage),
            meta,
            tree_meta.meta_a,
            tree_meta.meta_b,
            tree_meta.keyfmt_id,
            tree_meta.key_encoding,
            Arc::clone(&self.epoch_mgr),
        );

        Ok(SharedBPlusTree::new(bpt))
    }

    /// Writes the current freelist and next-page-id to a snapshot file.
    ///
    /// Called during graceful shutdown so that freed pages are restored on
    /// the next open, preventing page-id exhaustion after many deletes.
    pub fn checkpoint_freelist(&self) -> Result<(), DatabaseError> {
        let freelist_path = self.base_path.join("freelist.snapshot");
        let freed = self.meta_storage.as_ref().get_freelist();
        let next_pid = self.meta_storage.as_ref().get_next_page_id();
        write_freepages_snapshot(&freelist_path, FREELIST_SNAPSHOT_VERSION, next_pid, &freed)?;
        Ok(())
    }

    /// Returns the on-disk format version read from the superblock.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    // -----------------------------------------------------------------
    // Rename / drop
    // -----------------------------------------------------------------

    /// Renames an existing tree, recording the change in the manifest.
    pub fn rename_tree(&self, id: &TreeId, new_name: &str) -> Result<(), DatabaseError> {
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(DatabaseError::NotFound("tree not found".into()));
            }
        }
        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(ManifestRec::RenameTree {
            seq: 0,
            id: *id,
            new_name: new_name.to_string(),
        })?;
        w.fsync()?;
        drop(w);

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&ManifestRec::RenameTree {
            seq,
            id: *id,
            new_name: new_name.to_string(),
        });
        Ok(())
    }

    /// Removes a tree from the catalog and records the deletion in the manifest.
    pub fn drop_tree(&self, id: &TreeId) -> Result<(), DatabaseError> {
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(DatabaseError::NotFound("tree not found".into()));
            }
        }
        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(ManifestRec::DeleteTree { seq: 0, id: *id })?;
        w.fsync()?;
        drop(w);

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&ManifestRec::DeleteTree { seq, id: *id });
        Ok(())
    }

    /// Returns the names of all trees currently in the catalog.
    pub fn list_trees(&self) -> Vec<String> {
        let cat = self.catalog.read().unwrap();
        cat.by_name.keys().cloned().collect()
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    fn alloc_tree_id(&self, name: &str) -> TreeId {
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut hasher);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        ts.hash(&mut hasher);
        hasher.finish()
    }
}

// ---------------------------------------------------------------------------
// Superblock helpers
// ---------------------------------------------------------------------------

fn read_superblock<S: PageStorage>(storage: &S) -> Result<Superblock, DatabaseError> {
    let mut buf = [0u8; PAGE_SIZE];
    storage.read_page(SUPERBLOCK_PAGE, &mut buf)?;
    let sb = Superblock::from_bytes(
        buf[..std::mem::size_of::<Superblock>()]
            .try_into()
            .expect("superblock size fits in page"),
    )
    .map_err(DatabaseError::Io)?;
    sb.validate().map_err(DatabaseError::Io)?;
    Ok(*sb)
}

fn write_superblock<S: PageStorage>(storage: &S) -> Result<(), DatabaseError> {
    let sb = Superblock {
        magic: SUPERBLOCK_MAGIC,
        version: SUPERBLOCK_VERSION,
        gen_id: 1,
        page_size: PAGE_SIZE as u64,
        next_page_id: 0,
        freelist_head: 0,
        crc32c: 0,
        _pad: 0,
    }
    .with_crc();
    let mut buf = [0u8; PAGE_SIZE];
    let sb_bytes = sb.as_bytes();
    buf[..sb_bytes.len()].copy_from_slice(sb_bytes);
    storage.write_page_at_offset(SUPERBLOCK_PAGE, &buf)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// File locking
// ---------------------------------------------------------------------------

/// Attempts to acquire an exclusive `flock` on `path`, returning the open file
/// handle on success. The lock is released automatically when the file is closed
/// (i.e. when the returned `File` is dropped).
fn try_lock_file(path: &Path) -> Result<File, DatabaseError> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;

    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            return Err(DatabaseError::Locked);
        }
        return Err(DatabaseError::Io(err));
    }
    Ok(file)
}

// ---------------------------------------------------------------------------
// database::open — recovery entry point
// ---------------------------------------------------------------------------

/// Opens or creates a [`Database`] from a directory.
///
/// On a fresh directory: writes the superblock, creates an empty manifest.
/// On an existing directory: validates the superblock version, replays the
/// manifest, and reconciles catalog metadata against on-disk pages.
///
/// An exclusive file lock (`db.lock`) is held for the lifetime of the returned
/// [`Database`]. If another process already holds the lock,
/// [`DatabaseError::Locked`] is returned.
pub fn open<S, P>(base_path: P) -> Result<Database<S>, DatabaseError>
where
    S: PageStorage + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let base = base_path.as_ref();

    // Acquire the exclusive lock before touching any other files.
    let lock_file = try_lock_file(&base.join("db.lock"))?;

    let data_path = base.join("data.db");
    let manifest_path = base.join("manifest.log");

    let is_fresh = !data_path.exists();

    let storage = Arc::new(S::open(&data_path)?);
    let epoch_mgr = Arc::new(EpochManager::new());
    let node_storage = Arc::new(PagedNodeStorage::from_parts(
        Arc::clone(&storage),
        Arc::clone(&epoch_mgr),
    ));

    let format_version = if is_fresh {
        write_superblock(storage.as_ref())?;
        SUPERBLOCK_VERSION
    } else {
        let sb = read_superblock(storage.as_ref())?;
        sb.version
    };

    let (catalog, manifest) = if is_fresh {
        let cat = Catalog::new();
        let manifest = ManifestWriter::open(&manifest_path, 0)?;
        (cat, manifest)
    } else {
        let mut reader = ManifestReader::open(&manifest_path)?;
        let (mut catalog, last_seq) = replay_manifest(&mut reader)?;

        // Reconcile catalog with metadata pages (source of truth after crash).
        for meta in catalog.metas.values_mut() {
            if let Ok(page) =
                MetadataManager::read_active_meta(storage.as_ref(), meta.meta_a, meta.meta_b)
            {
                meta.root_id = page.root_node_id;
                meta.height = page.height;
                meta.size = page.size;
            }
        }

        let manifest = ManifestWriter::open(&manifest_path, last_seq)?;
        (catalog, manifest)
    };

    // Restore freelist from snapshot if present.
    let freelist_path = base.join("freelist.snapshot");
    if freelist_path.exists() {
        match read_freepages_snapshot(&freelist_path, 0) {
            Ok((next_pid, freed_ids)) => {
                storage.as_ref().set_next_page_id(next_pid)?;
                storage.as_ref().set_freelist(freed_ids)?;
            }
            Err(e) => {
                eprintln!("warning: failed to read freelist snapshot: {e}");
            }
        }
    }

    Ok(Database {
        node_storage,
        meta_storage: storage,
        epoch_mgr,
        manifest: Mutex::new(manifest),
        catalog: RwLock::new(catalog),
        format_version,
        base_path: base.to_path_buf(),
        _lock_file: lock_file,
    })
}

/// Replays all manifest records into a fresh catalog.
fn replay_manifest(reader: &mut ManifestReader) -> Result<(Catalog, u64), DatabaseError> {
    let mut catalog = Catalog::new();
    let mut last_seq = 0u64;
    while let Some(rec) = reader.read_next()? {
        last_seq = seq_of(&rec);
        catalog.replay_record(&rec);
    }
    Ok((catalog, last_seq))
}

fn seq_of(rec: &ManifestRec) -> u64 {
    match rec {
        ManifestRec::CreateTree { seq, .. } => *seq,
        ManifestRec::RenameTree { seq, .. } => *seq,
        ManifestRec::DeleteTree { seq, .. } => *seq,
    }
}
