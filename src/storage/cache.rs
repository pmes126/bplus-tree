use lru::LruCache;
use crate::bplustree::{Node, NodeId};
use crate::storage::{NodeStorage};
use std::io;
use std::num::NonZeroUsize;

// CacheLayer is a decorator around a backend storage that caches nodes in memory.
pub struct CacheLayer<K, V, B: NodeStorage<K, V>> {
    backend: B,
    cache: LruCache<NodeId, Node<K, V, NodeId>>,
}

// Implement the initialization for CacheLayer with a specified capacity and backend storage.
impl<K, V, B> CacheLayer<K, V, B>
where
    K: serde::Serialize + for<'de> serde::Deserialize <'de> + Clone,
    V: serde::Serialize + for<'de> serde::Deserialize <'de> + Clone,
    B: NodeStorage<K, V>,
{
    fn new(capacity: usize, backend: B) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("Invalid cache capacity")),
            backend,
        }
    }
}

// Implement the NodeStorage trait
impl<K, V, B> NodeStorage<K, V> for CacheLayer<K, V, B>
    where
    K: serde::Serialize + for<'de> serde::Deserialize <'de> + Clone,
    V: serde::Serialize + for<'de> serde::Deserialize <'de> + Clone,
    B: NodeStorage<K, V>,
{
    fn read_node(&mut self, id: u64) -> io::Result<Option<Node<K, V, NodeId>>> {
        if let Some(node) = self.cache.get(&id) {
            return Ok(Some(node.clone()));
        }
        let node = self.backend.read_node(id)?;
        if let Some(n) = &node {
            self.cache.put(id, n.clone());
        } else {
            // If the node is not found in the backend, return None
            return Ok(None);
        }
        Ok(node)
    }

    fn write_node(&mut self, id: NodeId, node: &Node<K, V, NodeId>) -> io::Result<()> {
        self.cache.put(id, node.clone()).ok_or(io::Error::other(
            "Cache write failed: cache is full or node already exists",
        ))?;
        self.backend.write_node(id, node)
    }
    
    fn flush(&mut self) -> io::Result<()> {
        self.backend.flush()
    }

    fn get_root(&self) -> io::Result<u64> {
        self.backend.get_root()
    }
} 
