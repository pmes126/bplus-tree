use crate::bplustree::{Node, NodeId};
use crate::storage::NodeStorage;
use bincode;
use serde::{Serialize, de::DeserializeOwned};
use std::{fs::{File, OpenOptions}, io::{Read, Write, Seek, SeekFrom, Result}, collections::HashMap};

const PAGE_SIZE: usize = 4096;
const WORD_SIZE: usize = 8; // Assuming a word size of 8 bytes for u64

#[derive(Debug)]
struct OffSetEntry {
    offset: u64,
    length: u64,
}

#[derive(Debug)]
pub struct FlatFile<K, V> {
    file: File,
    index: HashMap<NodeId, OffSetEntry>, // node_id -> file offset
    next_offset: u64,
    _marker: std::marker::PhantomData<(K, V)>
}

// Implement a constructor for FlatFile
impl<K, V> FlatFile<K, V> {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        // Initialize the file and read existing entries
        Ok(
            Self {
                next_offset: file.seek(SeekFrom::End(0))?,
                file,
                index: HashMap::new(),
                _marker: std::marker::PhantomData,
            }
        )
    }
}

// Implement the NodeStorage trait for FlatFile
impl<K, V> NodeStorage<K, V> for FlatFile<K, V>
    where
      K: Serialize + DeserializeOwned + Ord + Clone,
      V: Serialize + DeserializeOwned + Clone,
{
    // Read a node from the flat file by its ID
    fn read_node(&mut self, id: NodeId) -> Result<Option<Node<K, V, NodeId>>> {
        if let Some(entry) = self.index.get(&id) {
            // If the offest is invalid, return an error
            if entry.offset >= self.next_offset {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid offset"));
            }
            // If the entry exists, seek to the offset
            self.file.seek(SeekFrom::Start(entry.offset))?;
            // Read the length of the serialized data
            let mut len_buf = [0u8; WORD_SIZE];
            self.file.read_exact(&mut len_buf)?;
            let length = u64::from_le_bytes(len_buf);
            if length == 0 {
                // If the length is zero, return None
                return Ok(None);
            }
            if length != entry.length {
                // If the length does not match the index, return an error
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Length mismatch"));
            }
            // Read the serialized data
            let mut buf = vec![0u8; length as usize];
            self.file.read_exact(&mut buf)?;
            // Deserialize the data into a Node and return it
            bincode::deserialize::<Node<K, V, NodeId>>(&buf)
                .map(Some)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
       } else {
           // If the entry does not exist, return None
           Ok(None)
       }

    }

    // Write a node to the flat file and update the index
    fn write_node(&mut self, id: NodeId, node: &Node<K, V, NodeId>) -> Result<()> {
        let data = bincode::serialize(node)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let length = data.len() as u64;
        let offset = self.next_offset;

        self.file.seek(SeekFrom::Start(offset))?;
        // Pad data to next multiple of PAGE_SIZE
        let total_len = data.len() + 4; // include length prefix
        //if offset + total_len as u64 > PAGE_SIZE as u64 {
        //    // If the current offset plus data exceeds PAGE_SIZE, we need to seek to the next page
        //    let next_page = offset + PAGE_SIZE as u64 - (offset % PAGE_SIZE as u64);
        //    self.file.seek(SeekFrom::Start(next_page))?;
        //}
        // Write the length of the serialized data
        self.file.write_all(&length.to_le_bytes())?;
        // Write the serialized data
        self.file.write_all(&data)?;
        self.file.flush()?;

        self.index.insert(id, OffSetEntry { offset, length });
        self.next_offset += total_len as u64; // Update the next offset
        Ok(())
    }

    // Flush the file to ensure all changes are written
    fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }

    // Get the root node ID (not implemented, just a placeholder)
    fn get_root(&self) -> Result<u64> {
        Ok(0) // Placeholder, should return the actual root node ID
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_node() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        let node = Node::Leaf { 
            keys: vec![1u64, 2, 3],
            values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            next: None,
        };
        let node_id: NodeId = 1u64;
        storage.write_node(node_id, &node)?;
        let read_node = storage.read_node(1)?;
        println!("Read node: {:?}", read_node);
        assert!(read_node.is_some(), "Node should be read successfully");
        assert_eq!(read_node.unwrap(), node);
        Ok(())
    }
    /*
    #[test]
    fn read_non_existent_node() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        let read_node = storage.read_node(999)?;
        assert!(read_node.is_none(), "Reading a non-existent node should return None");
        Ok(())
    }
    #[test]
    fn write_multiple_nodes() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        for i in 1..=100 {
            let node = Node::Leaf { 
                keys: vec![i],
                values: vec![format!("value_{}", i)],
                next: None,
            };
            storage.write_node(i, &node)?;
            let read_node = storage.read_node(i)?;
            assert!(read_node.is_some(), "Node {} should be read successfully", i);
            assert_eq!(read_node.unwrap(), node, "Node {} read does not match written", i);
        }
        Ok(())
    }
    #[test]
    fn write_and_read_large_node() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        let large_node = Node::Leaf { 
            keys: vec![1u64; 10000], // 10000 keys
            values: vec!["large_value".to_string(); 10000], // 10000 values
            next: None,
        };
        let node_id: NodeId = 2;
        storage.write_node(node_id, &large_node)?;
        let read_node = storage.read_node(node_id)?;
        assert!(read_node.is_some(), "Large node should be read successfully");
        assert_eq!(read_node.unwrap(), large_node);
        Ok(())
    }
    #[test]
    fn write_and_read_multiple_large_nodes() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        for i in 1..=10 {
            let large_node = Node::Leaf { 
                keys: vec![i; 1000], // 1000 keys
                values: vec![format!("value_{}", i); 1000], // 1000 values
                next: None,
            };
            storage.write_node(i, &large_node)?;
            let read_node = storage.read_node(i)?;
            assert!(read_node.is_some(), "Large node {} should be read successfully", i);
            assert_eq!(read_node.unwrap(), large_node, "Node {} read does not match written", i);
        }
        Ok(())
    }
    #[test]
    fn write_and_read_empty_node() -> Result<()> {
        let mut storage = FlatFile::<u64, String>::new("test_flatfile.bin").unwrap();
        let empty_node = Node::Leaf { 
            keys: vec![],
            values: vec![],
            next: None,
        };
        let node_id: NodeId = 3;
        storage.write_node(node_id, &empty_node)?;
        let read_node = storage.read_node(node_id)?;
        assert!(read_node.is_some(), "Empty node should be read successfully");
        assert_eq!(read_node.unwrap(), empty_node);
        Ok(())
    }
    */
}
