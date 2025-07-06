pub mod cache;
pub mod flatfile;

pub use crate::bplustree::{Node, NodeId};
pub use std::io::Result;

pub trait NodeStorage<K, V> {
    fn write_node(&mut self, id: NodeId, node: &Node<K, V, NodeId>) -> Result<()>;
    fn read_node(&mut self, id: NodeId) -> Result<Option<Node<K, V, NodeId>>>;
    fn flush(&mut self) -> Result<()>;
    fn get_root(&self) -> Result<u64>;
}
