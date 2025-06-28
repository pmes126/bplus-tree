use std::{fs::File, io::{self, Read, Write, Seek, SeekFrom}, collections::HashMap};
use bincode;
use crate::bplustree::{Node, NodeId};
use crate::storage::NodeStorage;

const PAGE_SIZE: usize = 4096;

struct OffSetEntry {
    offset: u64,
    length: u32,
}

#[derive(Debug)]
struct FlatFile<K, V> {
    file: File,
    index: HashMap<NodeId, OffSetEntry>, // node_id -> file offset
    next_offset: u64,
    _marker: std::marker::PhantomData<(K, V)>
}

// Implement a constructor for FlatFile
impl<K, V> FlatFile<K, V> {
    fn new<P: AsRef<Path>>(path: P) -> Option<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        Self {
            file,
            let next = self.file.seek(SeekFrom::End(0))?
            index: HashMap::new(),
            next_offset: next,
            _marker: std::marker::PhantomData,
        }
    }
}

// Implement the NodeStorage trait for FlatFile
impl<K, V> NodeStorage for FlatFile<K, V>
where K: Serialize + DeserializeOwned + Ord + Clone,
      V: Serialize + DeserializeOwned + Clone,
{
    // Read a node from the flat file by its ID
    fn read_node(&mut self, id: NodeId) -> Node<K, V> {
        let entry = self.index.get(&id).expect("Missing offset entry");
        self.file.seek(SeekFrom::Start(entry.offset)).unwrap();

        // Read the length of the serialized data
        let mut len_buf = [0u8; 4];
        self.file.read_exact(&mut len_buf).unwrap();
        let length = u32::from_le_bytes(len_buf);

        // Read the serialized data
        let mut buf = vec![0u8; length as usize];
        self.file.read_exact(&mut buf).unwrap();
        bincode::deserialize(&buf).unwrap()
    }

    // Write a node to the flat file and update the index
    fn write_node(&mut self, id: NodeId, node: &Node<K, V>) {
        let data = bincode::serialize(node).unwrap();
        let length = data.len() as u32;
        let offset = self.file_end;

        self.file.seek(SeekFrom::Start(offset)).unwrap();
        // Write the length of the serialized data
        self.file.write_all(&length.to_le_bytes()).unwrap();
        // Pad data to next multiple of PAGE_SIZE
        let mut padded_data = data;
        let total_len = padded_data.len() + 4; // include length prefix
        let pad_len = (PAGE_SIZE - (total_len % PAGE_SIZE)) % PAGE_SIZE;
        padded_data.extend(vec![0u8; pad_len]);
        // Write the serialized data
        self.file.write_all(&padded_data).unwrap();
        self.file.flush().unwrap();

        self.index.insert(id, OffsetEntry { offset, length });
        self.file_end += length + pad_len as u64;
    }

    // Delete a node from the flat file by its ID
    fn delete_node(&mut self, id: NodeId) {
        if let Some(entry) = self.index.remove(&id) {
            // Mark the space as free (could implement a more sophisticated free list)
            // For simplicity, we just remove the entry from the index
        }
    }
}
