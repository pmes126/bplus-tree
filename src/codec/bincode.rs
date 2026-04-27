use crate::bplustree::{Node, NodeView};
use crate::codec::{
    CodecError, KeyCodec, KeyCodecDefault, NodeCodec, ValueCodec, ValueCodecDefault,
};
use crate::keyfmt::raw::RawFormat;
use crate::keyfmt::{KeyFormat, ScratchBuf};
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::InternalPage;
use crate::page::LEAF_NODE_TAG;
use crate::page::LeafPage;

// initial capacity for encoding buffers
const INIT_ENC_CAP: usize = 256;

pub struct BeU64;
pub struct Utf8;
pub struct RawBuf;

const KEY_FORMAT_DEFAULT: KeyFormat = KeyFormat::Raw(RawFormat);

impl KeyCodec<u64> for BeU64 {
    fn encode_key(key: &u64, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<u64>();
        out[..size].copy_from_slice(&key.to_be_bytes());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            buf.try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        ))
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
        Ok(u64::from_be_bytes(
            buf.try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        ))
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
        let u = u64::from_be_bytes(
            buf.try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        );
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

impl ValueCodec<i64> for BeU64 {
    fn encode_value(v: &i64, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<i64>();
        let t = (*v as u64) ^ 0x8000_0000_0000_0000u64;
        out[..size].copy_from_slice(&t.to_be_bytes());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Result<i64, CodecError> {
        let u = u64::from_be_bytes(
            buf.try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        );
        Ok((u ^ 0x8000_0000_0000_0000u64) as i64)
    }

    #[inline]
    fn encoded_len(_value: &i64) -> usize {
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
        String::from_utf8(buf.to_vec())
            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })
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
        String::from_utf8(buf.to_vec())
            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })
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

// ---- Default Codec mappings ----

impl KeyCodecDefault<u64> for () {
    type Codec = BeU64;
}
impl ValueCodecDefault<u64> for () {
    type Codec = BeU64;
}

impl KeyCodecDefault<i64> for () {
    type Codec = BeU64;
}
impl ValueCodecDefault<i64> for () {
    type Codec = BeU64;
}

impl KeyCodecDefault<String> for () {
    type Codec = Utf8;
}
impl ValueCodecDefault<String> for () {
    type Codec = Utf8;
}

impl KeyCodecDefault<Vec<u8>> for () {
    type Codec = RawBuf;
}
impl ValueCodecDefault<Vec<u8>> for () {
    type Codec = RawBuf;
}

//=======NodeCodec implementation using default codecs for K and V =======
// This codec uses the default KeyCodec and ValueCodec for K and V respectively
// It will decode a page into a Node<K,V> by decoding the keys and values using the default
// codecs
pub struct DefaultNodeCodec<KC, VC> {
    _marker_k: std::marker::PhantomData<KC>,
    _marker_v: std::marker::PhantomData<VC>,
}

impl<K, V, KC, VC> NodeCodec<K, V> for DefaultNodeCodec<KC, VC>
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
        let scratch: &mut ScratchBuf = &mut ScratchBuf::new();
        match node_type {
            LEAF_NODE_TAG => {
                // Leaf node
                let page = LeafPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                let mut leaf = Node::Leaf {
                    keys: Vec::new(),
                    values: Vec::new(),
                };

                if let Node::Leaf::<K, V> { keys, values } = &mut leaf {
                    for i in 0..page.key_count() as usize {
                        let (key_bytes, value_bytes) = page
                            .get_kv_at(i, scratch)
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
                    keys: Vec::with_capacity(page.key_count() as usize),
                    children: Vec::with_capacity(page.key_count() as usize + 1), // +1 for rightmost child
                };
                let scratch: &mut ScratchBuf = &mut ScratchBuf::new();
                if let Node::Internal { keys, children } = &mut internal {
                    for i in 0..page.key_count() as usize {
                        let key_bytes = page
                            .get_key_at(i, scratch)
                            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                        keys.push(KC::decode_key(key_bytes)?);
                    }
                    for i in 0..page.key_count() as usize + 1 {
                        let child_ptr = page
                            .read_child_at(i)
                            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
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

    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], CodecError> {
        match node {
            Node::Leaf { keys, values } => {
                let mut page = LeafPage::new(KEY_FORMAT_DEFAULT);
                {
                    let mut encode_buf_key: Vec<u8> = Vec::with_capacity(INIT_ENC_CAP);
                    let mut encode_buf_val: Vec<u8> = Vec::with_capacity(INIT_ENC_CAP);
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
                let mut page = InternalPage::new(KEY_FORMAT_DEFAULT);
                let leftmost_child = children.first().ok_or(CodecError::EncodeFailure {
                    msg: "Internal node must have at least one child".to_string(),
                })?;
                page.write_child_at(0, *leftmost_child)
                    .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                let child_iter = children.iter().skip(1);

                let mut encode_buf: Vec<u8> = Vec::with_capacity(INIT_ENC_CAP);
                for (idx, (key_ref, child)) in keys.iter().zip(child_iter).enumerate() {
                    encode_buf.resize(KC::encoded_len(key_ref), 0);
                    KC::encode_key(key_ref, encode_buf.as_mut())
                        .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                    page.insert_separator(idx, &encode_buf, *child)
                        .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                }
                page.to_bytes()
                    .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })
                    .copied()
            }
        }
    }
}

// A Codec for transforming between a NodeView and a buffer
pub struct NoopNodeViewCodec;

// A NodeViewCodec simply wraps the page encoding/decoding without transforming to/from
// Node<K,V>.
impl NoopNodeViewCodec {
    pub fn decode(buf: &[u8; PAGE_SIZE]) -> Result<NodeView, CodecError> {
        let node_type = buf.first().copied().ok_or(CodecError::DecodeFailure {
            msg: "Buffer too small to read node type".to_string(),
        })?;
        match node_type {
            LEAF_NODE_TAG => {
                // Leaf node
                let page = LeafPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                Ok(NodeView::Leaf {
                    page: *page,
                    page_id: None,
                })
            }
            INTERNAL_NODE_TAG => {
                // Internal node
                let page = InternalPage::from_bytes(buf)
                    .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                Ok(NodeView::Internal {
                    page: *page,
                    page_id: None,
                })
            }
            _ => Err(CodecError::DecodeFailure {
                msg: "Invalid node type tag in page".to_string(),
            }),
        }
    }

    pub fn encode(node: &NodeView) -> Result<&[u8; PAGE_SIZE], CodecError> {
        match node {
            NodeView::Leaf { page, .. } => page
                .to_bytes()
                .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() }),
            NodeView::Internal { page, .. } => page
                .to_bytes()
                .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplustree::Node;
    use crate::codec::NodeCodec;

    #[test]
    fn test_beu64_key_codec() {
        let key: u64 = 1234567890123456789;
        let mut buf = [0u8; 8];
        let encoded_size = BeU64::encode_key(&key, &mut buf).unwrap();
        assert_eq!(encoded_size, 8);
        //let decoded_key: u64 = BeU64::decode_key(&buf).unwrap();
        //assert_eq!(key, decoded_key);
    }

    #[test]
    fn test_utf8_key_codec() {
        let key = String::from("hello");
        let mut buf = [0u8; 10];
        let encoded_size = Utf8::encode_key(&key, &mut buf).unwrap();
        assert_eq!(encoded_size, 5);
        let decoded_key = Utf8::decode_key(&buf[..encoded_size]).unwrap();
        assert_eq!(key, decoded_key);
    }

    #[test]
    fn test_rawbuf_key_codec() {
        let key = vec![1u8, 2, 3, 4, 5];
        let mut buf = [0u8; 10];
        let encoded_size = RawBuf::encode_key(&key, &mut buf).unwrap();
        assert_eq!(encoded_size, 5);
        let decoded_key = RawBuf::decode_key(&buf[..encoded_size]).unwrap();
        assert_eq!(key, decoded_key);
    }

    #[test]
    fn test_node_codec_leaf() {
        let node = Node::Leaf {
            keys: vec![1u64, 2, 3],
            values: vec![10u64, 20, 30],
        };
        let encoded_page =
            <DefaultNodeCodec<BeU64, BeU64> as NodeCodec<u64, u64>>::encode(&node).unwrap();
        let decoded_node =
            <DefaultNodeCodec<BeU64, BeU64> as NodeCodec<u64, u64>>::decode(&encoded_page).unwrap();
        assert_eq!(node.get_keys(), decoded_node.get_keys());
    }

    #[test]
    fn test_node_codec_internal() {
        let node = Node::Internal {
            keys: vec![1u64, 2, 3, 4],
            children: vec![100u64, 200, 300, 400, 500],
        };
        let encoded_page =
            <DefaultNodeCodec<BeU64, BeU64> as NodeCodec<u64, u64>>::encode(&node).unwrap();
        let decoded_node =
            <DefaultNodeCodec<BeU64, BeU64> as NodeCodec<u64, u64>>::decode(&encoded_page).unwrap();
        assert_eq!(node.get_keys(), decoded_node.get_keys());
        if let Node::Internal { children, .. } = &node {
            if let Node::Internal {
                children: decoded_children,
                ..
            } = &decoded_node
            {
                assert_eq!(children, decoded_children);
            } else {
                panic!("Decoded node is not internal");
            }
        }
    }
}
