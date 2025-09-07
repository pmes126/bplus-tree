use crate::bplustree::Node;
use crate::bplustree::NodeView;
use crate::codec::bincode::DefaultNodeCodec;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::codec::{KeyCodec, NodeCodec, ValueCodec};
use crate::layout::PAGE_SIZE;
use crate::metadata::{
    METADATA_PAGE_1, METADATA_PAGE_2, MetadataPage, calculate_checksum, new_metadata_page,
    new_metadata_page_with_object,
};
use crate::storage::{Metadata, MetadataStorage, NodeStorage, PageStorage, StorageError};

use anyhow::Result;
use std::path::Path;
use zerocopy::AsBytes;

pub use crate::storage::file_store;

pub struct FileStore<S: PageStorage> {
    store: S,
}

impl<S: PageStorage> FileStore<S> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: S::init(path)?,
        })
    }
}

impl<S: PageStorage> MetadataStorage for FileStore<S> {
    fn read_metadata(&self, slot: u8) -> Result<MetadataPage, std::io::Error> {
        if slot > METADATA_PAGE_2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid metadata slot",
            ));
        }
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(slot as u64, &mut buf)?;

        let metadata = MetadataPage::from_bytes(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Validate checksum
        let checksum = metadata.data.checksum;
        let calculated_checksum = calculate_checksum(metadata);
        if checksum != calculated_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Metadata checksum mismatch",
            ));
        }
        Ok(*metadata)
    }

    fn write_metadata(&self, slot: u8, meta: &mut MetadataPage) -> Result<(), std::io::Error> {
        let checksum = calculate_checksum(meta);
        meta.data.checksum = checksum;
        let buf = meta.as_bytes();
        self.store.write_page_at_offset(slot as u64, buf)?;
        Ok(())
    }

    fn read_current_root(&self) -> Result<u64, std::io::Error> {
        let meta0 = self.read_metadata(METADATA_PAGE_1)?;
        let meta1 = self.read_metadata(METADATA_PAGE_2)?;
        let root_node_id = if meta0.data.txn_id > meta1.data.txn_id {
            meta0.data.root_node_id
        } else {
            meta1.data.root_node_id
        };
        Ok(root_node_id)
    }

    fn get_metadata(&self) -> Result<Metadata, std::io::Error> {
        let meta0 = self.read_metadata(METADATA_PAGE_1)?;
        let meta1 = self.read_metadata(METADATA_PAGE_2)?;
        if meta0.data.txn_id >= meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }

    fn commit_metadata(
        &self,
        slot: u8,
        txn_id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page(root, txn_id, 0, height, order, size);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
    }

    fn commit_metadata_with_object(
        &self,
        slot: u8,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page_with_object(metadata);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
    }
}

impl<S: PageStorage, K, V> NodeStorage<K, V> for FileStore<S>
where
    K: KeyCodec + Ord,
    V: ValueCodec,
{
    fn read_node(&self, page_id: u64) -> Result<Option<Node<K, V>>, StorageError>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        DefaultNodeCodec::decode(&buf).map_or(Ok(None), |node| Ok(Some(node)))
    }

    fn write_node(&self, node: &Node<K, V>) -> Result<u64, StorageError>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let buf = DefaultNodeCodec::encode(node)?;
        let res = self.store.write_page(&buf)?;
        Ok(res)
    }

    fn read_node_view(&self, page_id: u64) -> Result<Option<NodeView>, StorageError> {
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        NoopNodeViewCodec::decode(&buf).map(|view| Ok(Some(view)))?
    }

    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let res = self.store.write_page(&buf)?;
        Ok(res)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.store.flush()
    }

    fn free_node(&self, id: u64) -> Result<(), std::io::Error> {
        self.store.free_page(id)
    }
}
