//! [`NodeStorage`] implementation backed by a [`PageStorage`] instance.

pub use crate::storage::paged_node_storage;

use crate::bplustree::NodeView;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::database::manifest::writer::ManifestWriter;
use crate::layout::PAGE_SIZE;
use crate::storage::epoch::EpochManager;
use crate::storage::{HasEpoch, NodeStorage, PageStorage, StorageError};

use std::path::Path;
use std::sync::{Arc, Mutex};

/// A [`NodeStorage`] that encodes node views as pages and delegates I/O to a [`PageStorage`].
pub struct PagedNodeStorage<S: PageStorage> {
    store: S,
    epoch_mgr: Arc<EpochManager>,
    manifest: Mutex<ManifestWriter>,
}

impl<S: PageStorage> HasEpoch for PagedNodeStorage<S>
where
    S: Send + Sync + 'static,
{
    fn epoch_mgr(&self) -> &Arc<EpochManager> {
        &self.epoch_mgr
    }
}

impl<S: PageStorage> PagedNodeStorage<S>
where
    S: Send + Sync + 'static,
{
    /// Opens (or creates) a [`PagedNodeStorage`] from the given data and manifest paths.
    pub fn new<P: AsRef<Path>>(storage_path: P, manifest_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: S::open(storage_path)?,
            epoch_mgr: EpochManager::new_shared(),
            manifest: Mutex::new(ManifestWriter::open(manifest_path.as_ref(), 0)?),
        })
    }
}
impl<S: PageStorage> NodeStorage for PagedNodeStorage<S>
where
    S: Send + Sync + 'static,
{
    fn read_node_view(&self, page_id: u64) -> Result<Option<NodeView>, StorageError> {
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        NoopNodeViewCodec::decode(&buf).map(|view| Ok(Some(view)))?
    }

    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let res = self.store.write_page(buf)?;
        Ok(res)
    }

    fn write_node_view_at_offset(
        &self,
        node_view: &NodeView,
        offset: u64,
    ) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let res = self.store.write_page_at_offset(offset, buf)?;
        Ok(res)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.store.flush()
    }

    fn free_node(&self, id: u64) -> Result<(), std::io::Error> {
        self.store.free_page(id)
    }
}
