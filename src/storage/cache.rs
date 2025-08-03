//use crate::bplustree::{Node, NodeId};
//use crate::storage::{KeyCodec, ValueCodec, NodeStorage};
//use lru::LruCache;
//use std::io;
//use std::num::NonZeroUsize;
//
//const CACHE_CAPACITY: usize = 100; // Default cache capacity
////
//// CacheLayer is a decorator around a backend storage that caches nodes in memory.
//pub struct CacheLayer<K, V, B: NodeStorage<K, V>> 
//where
//    K: KeyCodec + Clone,
//    V: ValueCodec + Clone,
//{
//    backend: B,
//    cache: LruCache<NodeId, Node<K, V>>,
//}
//
//// Implement the initialization for CacheLayer with a specified capacity and backend storage.
//impl<K, V, B> CacheLayer<K, V, B>
//where
//    K: KeyCodec + Clone,
//    V: ValueCodec + Clone,
//    B: NodeStorage<K, V>,
//{
//    fn new(capacity: usize, backend: B) -> Self {
//        Self {
//            cache: LruCache::new(NonZeroUsize::new(capacity).expect("Invalid cache capacity")),
//            backend,
//        }
//    }
//}
//
//// Implement the NodeStorage trait
//impl<K, V, B> NodeStorage<K, V> for CacheLayer<K, V, B>
//    where
//    K: KeyCodec + Clone,
//    V: ValueCodec + Clone,
//    B: NodeStorage<K, V>,
//{
//    fn read_node(&self, id: u64) -> Result<Option<Node<K, V>>, anyhow::Error> {
//        if let Some(node) = self.cache.peek(&id) {
//            // If the node is found in the cache, we return a deep copy of it.
//            return Ok(Some(node.clone()));
//        }
//        let node = self.backend.read_node(id)?;
//        if let Some(n) = node.clone() { // Clone the node into the cache
//            self.cache.put(id, n);
//            Ok(node)
//        } else {
//            // If the node is not found in the backend, return None
//            Ok(None)
//        }
//    }
//
//    fn write_node(&mut self, node: &Node<K, V>) -> Result<u64, anyhow::Error> {
//        // Write the node to the backend storage
//        let id = self.backend.write_node(node)?;
//        self.cache.put(id, node.clone()).ok_or(io::Error::other(
//            "Cache write failed: cache is full or node already exists", // TODO rethink this error message
//        ))?;
//        Ok(id)
//    }
//    
//    fn flush(&mut self) -> io::Result<()> {
//        self.backend.flush()
//    }
//
//    fn free_node(&mut self, id: u64) -> Result<(), io::Error> {
//        // Remove the node from the cache
//        self.cache.pop(&id);
//        // Free the node in the backend storage
//        self.backend.free_node(id)
//    }
//} 
