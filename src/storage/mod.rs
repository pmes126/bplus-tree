pub mod cache;
pub mod flatfile;

pub use cache::CacheLayer;
pub use flatfile::FlatFile;

trait NodeStorage<K, V> {
    fn read_node(&mut self, id: NodeId) -> io::Result<Node<K, V>>;
    fn write_node(&mut self, id: NodeId, node: &Node<K, V>) -> io::Result<()>;
    fn delete_node(&mut self, id: NodeId);
}
