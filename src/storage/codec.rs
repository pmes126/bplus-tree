use crate::layout::PAGE_SIZE;
use crate::storage::page::LeafPage;
use crate::storage::page::InternalPage;
use crate::storage::{KeyCodec, ValueCodec, NodeCodec};
use crate::bplustree::Node;

use std::io::{Error};
pub struct DefaultNodeCodec;

impl KeyCodec for u64 {
    fn encode_key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts((self as *const u64) as *const u8, 8) }
    }

    fn decode_key(buf: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        u64::from_le_bytes(arr)
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        u64::decode_key(a).cmp(&u64::decode_key(b))
    }
}

impl ValueCodec for u64 {
    fn encode_value(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts((self as *const u64) as *const u8, 8) }
    }

    fn decode_value(buf: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        u64::from_le_bytes(arr)
    }
}

impl KeyCodec for String {
    fn encode_key(&self) -> &[u8] {
        self.as_bytes()
    }

    fn decode_key(buf: &[u8]) -> Self {
        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        String::decode_key(a).cmp(&String::decode_key(b))
    }
}

impl ValueCodec for String {
    fn encode_value(&self) -> &[u8] {
        self.as_bytes()
    }

    fn decode_value(buf: &[u8]) -> Self {
        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
    }
}

impl KeyCodec for Vec<u8> {
    fn encode_key(&self) -> &[u8] {
        self.as_slice()
    }

    fn decode_key(buf: &[u8]) -> Self {
        buf.to_vec()
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

impl<K, V> NodeCodec<K, V> for DefaultNodeCodec
where
    K: KeyCodec + Copy + Ord,
    V: ValueCodec + Copy,
{
    fn decode(buf: &[u8; PAGE_SIZE]) -> Node<K, V> {
        match buf[0] {
        1 => {
            // Leaf node
            let page = LeafPage::from_bytes(buf)?
            let mut leaf = Node::Leaf {
                    keys: Vec::with_capacity(page.len() as usize),
                    values: Vec::with_capacity(page.len() as usize),
                    next: None,
            };

            for i in 0..page.header.entry_count as usize {
                let (key_bytes, value_bytes) = page.get_entry(i);
                leaf.keys.push(K::decode_key(key_bytes));
                leaf.values.push(V::decode_value(value_bytes));
            }
            leaf
        }
        0 => {
            // Internal node
            let page = unsafe { &*(buf.as_ptr() as *const InternalPage) };
            let mut internal = Node::Internal {
                keys: Vec::with_capacity(page.header.len as usize),
                children: Vec::with_capacity(page.header.len as usize + 1), // +1 for rightmost child
            };
            for i in 0..page.header.len as usize {
                let (key_bytes, child_ptr) = page.get_entry(i);
                internal.keys.push(K::decode_key(key_bytes));
                internal.children.push(child_ptr);
            }
            internal
        }
        _ => panic!("Invalid node type tag in page"),
        }    
    }

    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], Error> {
        match node {
            Node::Leaf { keys, values, .. } =>  {
                let mut page = LeafPage::new();
                let entries = keys.iter().zip(values.iter());
    
                for (key_ref, value_ref) in entries {
                    let key = key_ref.encode_key();
                    let value = value_ref.encode_value();
                    if let Err(e) = page.insert_entry(key, value) {
                        panic!("LeafPage overflow during encode: {}", e);
                    }
                }
    
                page.to_bytes()
            }
            Node::Internal { keys, children } => {
                let mut page = InternalPage::new();
                page.header.leftmost_child = children[0]; // Set the leftmost child pointer
                let entries = keys.iter().zip(children.iter().skip(1)); // skip the first child, as
                // it's the leftmost child
    
                for (key_ref, child_ref) in entries {
                    let key = key_ref.encode_key();
                    if let Err(e) = page.insert_entry(key, *child_ref) {
                        panic!("InternalPage overflow during encode: {}", e);
                    }
                }

                page.to_bytes()
            }   
        }
    }
}
