use crate::layout::PAGE_SIZE;
use crate::storage::page::LeafPage;
use crate::storage::page::InternalPage;
use crate::storage::page::INTERNAL_NODE_TAG;
use crate::storage::page::LEAF_NODE_TAG;
use crate::storage::{KeyCodec, ValueCodec, NodeCodec};
use crate::bplustree::Node;
use thiserror::Error;

pub struct DefaultNodeCodec;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("Error decoding value: {msg}")]
    DecodeFailure {
        msg: String,
    },
    #[error("Error encoding value: {msg}")]
    EncodeFailure {
        msg: String,
    },
    #[error("Error converting from byte slice: {source}")]
    FromSliceError {
        #[from]
        source: std::array::TryFromSliceError,
    },
    #[error("IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}

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

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b) // bytewise lexicographic compare (memcmp-ish)
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

impl ValueCodec for Vec<u8> {
    fn encode_value(&self) -> &[u8] {
        self.as_slice()
    }

    fn decode_value(buf: &[u8]) -> Self {
        buf.to_vec()
    }
}

impl<K, V> NodeCodec<K, V> for DefaultNodeCodec
where
    K: KeyCodec + Ord,
    V: ValueCodec,
{
    fn decode(buf: &[u8; PAGE_SIZE]) -> Result<Node<K, V>, CodecError> {
        let node_type =  u64::from_le_bytes(buf[0..8].try_into()
            .map_err(|e| CodecError::FromSliceError { source: e })?);
        match node_type {
        LEAF_NODE_TAG => {
            // Leaf node
            let page = LeafPage::from_bytes(buf).
                map_err(|e| CodecError::DecodeFailure {
                        msg: e.to_string(),
                    })?;
            let mut leaf = Node::Leaf {
                    keys: Vec::new(),
                    values: Vec::new(),
            };

            if let Node::Leaf { keys, values } = &mut leaf {
                for i in 0..page.header.entry_count as usize {
                    let (key_bytes, value_bytes) = page.get_entry(i).
                        map_err(|e| CodecError::DecodeFailure {
                                msg: e.to_string(),
                            })?;
                    keys.push(K::decode_key(key_bytes));
                    values.push(V::decode_value(value_bytes));
                }
            }
            Ok(leaf)
        }
        INTERNAL_NODE_TAG => {
            // Internal node
            let page = InternalPage::from_bytes(buf).
                map_err(|e| CodecError::DecodeFailure {
                        msg: e.to_string(),
                    })?;
            let mut internal = Node::Internal {
                keys: Vec::with_capacity(page.header.entry_count as usize),
                children: Vec::with_capacity(page.header.entry_count as usize + 1), // +1 for rightmost child
            };
            if let Node::Internal { keys, children } = &mut internal {
                children.push(page.header.leftmost_child); // Add the leftmost child pointer
                for i in 0..page.header.entry_count as usize {
                    let (key_bytes, child_ptr) = page.get_entry(i).
                        map_err(|e| CodecError::DecodeFailure {
                                msg: e.to_string(),
                            })?;
                    keys.push(K::decode_key(key_bytes));
                    children.push(child_ptr);
                }
            }
            Ok(internal)
        }
        _ => Err(CodecError::DecodeFailure { msg: "Invalid node type tag in page".to_string() })
        }    
    }

    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], CodecError> {
        match node {
            Node::Leaf { keys, values } =>  {
                let mut page = LeafPage::new();
                { 
                for (key_ref, value_ref) in keys.iter().zip(values.iter()) {
                    let key = key_ref.encode_key();
                    let value = value_ref.encode_value();
                    page.insert_entry(key.as_ref(), value.as_ref()).
                        map_err(|e| CodecError::EncodeFailure {
                                msg: e.to_string(),
                            })?;
                }
                }
                page.to_bytes().map_err(|e| CodecError::EncodeFailure { msg: e.to_string() }).copied()
            }
            Node::Internal { keys, children } => {
                let mut page = InternalPage::new();
                page.header.leftmost_child = children[0]; // Set the leftmost child pointer
                let entries = keys.iter().zip(children.iter().skip(1)); // skip the first child, as
                // it's the leftmost child
    
                for (key_ref, child_ref) in entries {
                    let key = key_ref.encode_key();
                    page.insert_entry(key, *child_ref).
                        map_err(|e| CodecError::EncodeFailure {
                                msg: e.to_string(),
                            })?;
                }
                page.to_bytes().map_err(|e| CodecError::EncodeFailure { msg: e.to_string() }).copied()
            }   
        }
    }
}
