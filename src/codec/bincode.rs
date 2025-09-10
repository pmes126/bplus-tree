use crate::bplustree::{Node, NodeView};
use crate::codec::{CodecError, KeyCodec, NodeCodec, ValueCodec};
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::InternalPage;
use crate::page::LEAF_NODE_TAG;
use crate::page::LeafPage;
use crate::page::leaf;

pub struct DefaultNodeCodec;
pub struct NoopNodeViewCodec;

const MAX_KEY_SIZE: usize = 256; // Maximum key size for internal nodes
const MAX_VAL_SIZE: usize = 256; // Maximum key size for internal nodes

pub struct BeU64;
pub struct Utf8;
pub struct RawBuf;

impl KeyCodec<u64> for BeU64 {
    fn encode_key(key: &u64, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<u64>();
        out[..size].copy_from_slice(&key.to_be_bytes());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(buf.try_into().map_err(|e| CodecError::FromSliceError { source: e })?))
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        debug_assert_eq!(a.len(), core::mem::size_of::<u64>());
        debug_assert_eq!(b.len(), core::mem::size_of::<u64>());
        a.cmp(b) // bytewise lexicographic compare == numeric compare for BE fixed-width
    }

    #[inline]
    fn encoded_len(_key: &u64) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl ValueCodec<u64> for BeU64 {
    fn encode_value(v: &u64, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<u64>();
        out[..size].copy_from_slice(&v.to_be_bytes());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(buf.try_into().map_err(|e| CodecError::FromSliceError { source: e })?))
    }

    #[inline]
    fn encoded_len(_value: &u64) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl KeyCodec<i64> for BeU64 {
    fn encode_key(key: &i64, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<i64>();
        let t = (*key as u64) ^ 0x8000_0000_0000_0000u64;
        out[..size].copy_from_slice(&t.to_be_bytes());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Result<i64, CodecError> {
        let u = u64::from_be_bytes(buf.try_into().map_err(|e| CodecError::FromSliceError { source: e })?);
        Ok((u ^ 0x8000_0000_0000_0000u64) as i64)
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        debug_assert_eq!(a.len(), core::mem::size_of::<u64>());
        debug_assert_eq!(b.len(), core::mem::size_of::<u64>());
        a.cmp(b) // bytewise lexicographic compare == numeric compare for BE fixed-width
    }

    #[inline]
    fn encoded_len(_key: &i64) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl KeyCodec<String> for Utf8 {
    #[inline]
    fn encode_key(key: &String, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = key.len();
        buf[..size].copy_from_slice(key.as_bytes());
        Ok(size)
    }

    // We need to copy the bytes to a Vec to ensure they are owned
    #[inline]
    fn decode_key(buf: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(buf.to_vec()).map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b) // bytewise lexicographic compare (memcmp-ish)
    }

    #[inline]
    fn encoded_len(k: &String) -> usize {
        k.len()
    }
}

impl ValueCodec<String> for Utf8 {
    fn encode_value(v: &String, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = v.len();
        buf[..size].copy_from_slice(v.as_bytes());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(buf.to_vec()).map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })
    }

    #[inline]
    fn encoded_len(v: &String) -> usize {
        v.len()
    }
}

impl KeyCodec<Vec<u8>> for RawBuf {
    fn encode_key(k: &Vec<u8>, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = k.len();
        buf[..size].copy_from_slice(k.as_slice());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(buf.to_vec())
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[inline]
    fn encoded_len(key: &Vec<u8>) -> usize {
        key.len()
    }
}

impl ValueCodec<Vec<u8>> for RawBuf {
    fn encode_value(value: &Vec<u8>, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = value.len();
        out[..size].copy_from_slice(value.as_slice());
        Ok(size)
    }

    fn decode_value(bytes: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(bytes.to_vec())
    }

    #[inline]
    fn encoded_len(value: &Vec<u8>) -> usize {
        value.len()
    }
}

impl<K, V, KC, VC> NodeCodec<K, V, KC, VC> for DefaultNodeCodec
where
    K: Ord + Clone,
    V: Clone,
    KC: KeyCodec<K>,
    VC: ValueCodec<V>,
{
    fn decode(buf: &[u8; PAGE_SIZE]) -> Result<Node<K, V>, CodecError> {
        let node_type = u8::from_le_bytes(
            buf[0..1]
                .try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        );
        let scratch: &mut Vec<u8> = &mut Vec::with_capacity(MAX_KEY_SIZE);
        match node_type {
            LEAF_NODE_TAG => {
                // Leaf node
                let page = LeafPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                let mut leaf = Node::Leaf {
                    keys: Vec::new(),
                    values: Vec::new(),
                };

                if let Node::Leaf { keys, values } = &mut leaf {
                    for i in 0..page.key_count() as usize {
                        let (key_bytes, value_bytes) = page
                            .get_kv_at(i, scratch.as_mut())
                            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                        keys.push(KC::decode_key(key_bytes)?);
                        values.push(VC::decode_value(value_bytes)?);
                    }
                }
                Ok(leaf)
            }
            INTERNAL_NODE_TAG => {
                // Internal node
                let page = InternalPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                let mut internal = Node::Internal {
                    keys: Vec::with_capacity(page.header.entry_count as usize),
                    children: Vec::with_capacity(page.header.entry_count as usize + 1), // +1 for rightmost child
                };
                if let Node::Internal { keys, children } = &mut internal {
                    children.push(page.header.leftmost_child); // Add the leftmost child pointer
                    for i in 0..page.header.entry_count as usize {
                        let (key_bytes, child_ptr) = page
                            .get_entry(i)
                            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                        keys.push(KC::decode_key(key_bytes)?);
                        children.push(child_ptr);
                    }
                }
                Ok(internal)
            }
            _ => Err(CodecError::DecodeFailure {
                msg: "Invalid node type tag in page".to_string(),
            }),
        }
    }

    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], CodecError>
    {
        match node {
            Node::Leaf { keys, values } => {
                let mut page = LeafPage::new(0u8);
                {
                    let mut encode_buf_key: Vec<u8> = Vec::with_capacity(MAX_KEY_SIZE);
                    let mut encode_buf_val: Vec<u8> = Vec::with_capacity(MAX_VAL_SIZE);
                    for (key_ref, value_ref) in keys.iter().zip(values.iter()) {
                        encode_buf_key.resize(KC::encoded_len(key_ref), 0);
                        encode_buf_val.resize(VC::encoded_len(value_ref), 0);
                        KC::encode_key(key_ref, encode_buf_key.as_mut())
                            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                        VC::encode_value(value_ref, encode_buf_val.as_mut())
                            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                        page.insert_encoded(&encode_buf_key, &encode_buf_val)
                            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                    }
                }
                page.to_bytes()
                    .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })
                    .copied()
            }
            Node::Internal { keys, children } => {
                let mut page = InternalPage::new();
                page.header.leftmost_child = children[0]; // Set the leftmost child pointer and skip it
                let entries = keys.iter().zip(children.iter().skip(1));
                let mut encode_buf: Vec<u8> = Vec::with_capacity(MAX_KEY_SIZE);
                for (key_ref, child_ref) in entries {
                    KC::encode_key(key_ref, encode_buf.as_mut())
                        .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                    page.insert_entry(&encode_buf, *child_ref)
                        .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                }
                page.to_bytes()
                    .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })
                    .copied()
            }
        }
    }
}

impl NoopNodeViewCodec {
    pub fn decode(buf: &[u8; PAGE_SIZE]) -> Result<NodeView, CodecError> {
        
        let node_type = u8::from_le_bytes(
            buf[0..1]
                .try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        );
        match node_type {
            LEAF_NODE_TAG => {
                // Leaf node
                let page = LeafPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                Ok(NodeView::Leaf { page: *page })
            }
            INTERNAL_NODE_TAG => {
                // Internal node
                let page = InternalPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                Ok(NodeView::Internal { page: *page })
            }
            _ => Err(CodecError::DecodeFailure {
                msg: "Invalid node type tag in page".to_string(),
            }),
        }
    }

    pub fn encode(node: &NodeView) -> Result<[u8; PAGE_SIZE], CodecError> {
        match node {
            NodeView::Leaf { page } => page
                .to_bytes()
                .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })
                .copied(),
            NodeView::Internal { page } => page
                .to_bytes()
                .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })
                .copied(),
        }
    }
}
