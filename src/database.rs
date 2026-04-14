//! Database layer: catalog, manifest, metadata, and superblock management.

pub mod catalog;
pub mod manifest;
pub mod metadata;
pub mod superblock;

use crate::api::{KeyEncodingId, KeyLimits, TreeId};
use crate::database::catalog::{Catalog, TreeMeta};
use crate::database::manifest::ManifestRec;
use crate::database::manifest::reader::ManifestReader;
use crate::database::manifest::writer::ManifestWriter;
use crate::database::metadata::Metadata;
use crate::keyfmt::KeyFormat;
use crate::storage::epoch::EpochManager;
use crate::storage::metadata_manager::MetadataManager;
use crate::storage::{PageStorage, StorageError};

use anyhow::Result;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

/// Holds all open state for a single database instance.
pub struct Database<S: PageStorage> {
    storage: Arc<S>,
    manifest: Mutex<ManifestWriter>,
    catalog: RwLock<Catalog>,
    epoch_mgr: EpochManager,
}

impl<S: PageStorage> Database<S> {
    /// Core constructor — accepts already-initialized objects. No file I/O.
    pub fn new(
        storage: S,
        manifest: ManifestWriter,
        catalog: Catalog,
        epoch_mgr: EpochManager,
    ) -> Self {
        Self {
            storage: Arc::new(storage),
            manifest: Mutex::new(manifest),
            catalog: RwLock::new(catalog),
            epoch_mgr,
        }
    }

    /// Allocates two metadata page slots and returns an initial [`Metadata`] value.
    pub fn bootstrap_metadata(
        &self,
        id: TreeId,
        order: usize,
    ) -> Result<(u64, u64, Metadata), std::io::Error> {
        let meta_a = self.storage.allocate_page()?;
        let meta_b = self.storage.allocate_page()?;
        let metadata = Metadata {
            root_node_id: 0,
            id,
            txn_id: 0,
            height: 0,
            order,
            size: 0,
            checksum: 0,
        };
        Ok((meta_a, meta_b, metadata))
    }

    /// Creates a new named tree, records it in the manifest, and returns its metadata.
    pub fn create_tree(
        &self,
        name: &str,
        enc: KeyEncodingId,
        key_format: KeyFormat,
        order: usize,
        limits: Option<KeyLimits>,
    ) -> Result<TreeMeta, std::io::Error> {
        let id = self.alloc_tree_id(name);
        let (meta_a, meta_b, metadata) = self.bootstrap_metadata(id, order)?;

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
            order: order as u64,
            root_id: metadata.root_node_id,
            height: metadata.height as u64,
            size: metadata.size as u64,
        };

        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(rec.clone())?;
        w.fsync()?;
        drop(w);

        // Replay with the assigned seq so catalog bookkeeping stays accurate.
        let mut committed = rec;
        if let ManifestRec::CreateTree { seq: s, .. } = &mut committed {
            *s = seq;
        }

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&committed);

        let meta = cat
            .metas
            .get(&id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "tree not found"))?
            .clone();
        Ok(meta)
    }

    /// Renames an existing tree, recording the change in the manifest.
    pub fn rename_tree(&self, id: &TreeId, new_name: &str) -> anyhow::Result<()> {
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(anyhow::anyhow!("tree not found"));
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
    pub fn drop_tree(&self, id: &TreeId) -> anyhow::Result<()> {
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(anyhow::anyhow!("tree not found"));
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

    /// Looks up a tree by ID and returns its metadata.
    pub fn open_tree(&self, id: &TreeId) -> anyhow::Result<TreeMeta> {
        let cat = self.catalog.read().unwrap();
        cat.get_by_id(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tree not found"))
    }

    /// Allocates a fresh tree ID based on a hash of the name and current time.
    pub fn alloc_tree_id(&self, name: &str) -> TreeId {
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

/// Opens a [`Database`] from a directory that contains a `data.db` page file and a
/// `manifest.log`. Handles all file I/O, manifest replay, and per-tree metadata
/// reconciliation before handing fully-initialized objects to [`Database::new`].
///
/// Intended as a temporary entry point until a dedicated opener / builder layer is
/// introduced above this module.
pub fn open<S: PageStorage, P: AsRef<Path>>(base_path: P) -> Result<Database<S>> {
    let base = base_path.as_ref();

    let storage = S::open(base.join("data.db"))?;

    let manifest_path = base.join("manifest.log");
    let mut reader = ManifestReader::open(&manifest_path)?;
    let (mut catalog, last_seq) = replay_manifest(&mut reader)?;

    // Metadata pages are the source of truth for each tree's committed state; reconcile
    // anything that diverged (e.g. after a crash between a metadata write and manifest flush).
    for meta in catalog.metas.values_mut() {
        let page = MetadataManager::read_active_meta(&storage, meta.meta_a, meta.meta_b)?;
        if (page.root_node_id, page.height, page.size) != (meta.root_id, meta.height, meta.size) {
            meta.root_id = page.root_node_id;
            meta.height = page.height;
            meta.size = page.size;
        }
    }

    let manifest = ManifestWriter::open(&manifest_path, last_seq)?;
    Ok(Database::new(
        storage,
        manifest,
        catalog,
        EpochManager::new(),
    ))
}

/// Replays all manifest records into a fresh catalog and returns the last sequence number.
fn replay_manifest(reader: &mut ManifestReader) -> Result<(Catalog, u64)> {
    let mut catalog = Catalog::new();
    let mut last_seq = 0u64;
    while let Some(rec) = reader.next().map_err(StorageError::Io)? {
        last_seq = seq_of(&rec);
        catalog.replay_record(&rec);
    }
    Ok((catalog, last_seq))
}

/// Extracts the sequence number from any manifest record variant.
fn seq_of(rec: &ManifestRec) -> u64 {
    match rec {
        ManifestRec::CreateTree { seq, .. } => *seq,
        ManifestRec::RenameTree { seq, .. } => *seq,
        ManifestRec::DeleteTree { seq, .. } => *seq,
        ManifestRec::Checkpoint { seq } => *seq,
    }
}
