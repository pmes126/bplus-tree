use lru::LruCache;
use crate::bplustree::Node;
use crate::storage::{NodeStorage, FlatFile};
use std::io;
use std::NonZerosize;

// CacheLayer is a decorator around a backend storage that caches nodes in memory.
struct CacheLayer<K, V, B: BackendStorage<K, V>> {
    backend: B,
    cache: LruCache<NodeId, Node<K, V>>,
}

// Implement the initialization for CacheLayer with a specified capacity and backend storage.
impl<K, V, B> CacheLayer<K, V, B>
where
    K: serde::Serialize + serde::de::DeserializeOwned + Clone,
    V: serde::Serialize + serde::de::DeserializeOwned + Clone,
    B: BackendStorage<K, V>,
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
    K: serde::Serialize + serde::de::DeserializeOwned + Clone,
    V: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    fn read_node(&mut self, id: u64) -> io::Result<Node<K, V>> {
        if let Some(node) = self.cache.get(&id) {
            return Ok(node.clone())
        }
        let node = self.backend.get_node(id)?;
        self.cache.put(id, node.clone());
        Ok(node)
    }

    fn write_node(&mut self, id: NodeId, node: &Node<K, V>) -> io::Result<()> {
        self.cache.put(id, node.clone());
        self.backend.write_node(id, node)
    }
    
    fn delete_node(&mut self, id: NodeId) {
        self.cache.pop(&id);
        self.backend.delete_node(id);
    }
}
            
