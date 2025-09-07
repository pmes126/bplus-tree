use crate::bplustree::{Node, NodeView};
use crate::codec::{CodecError, KeyCodec, NodeCodec, ValueCodec};
use crate::layout::PAGE_SIZE;
use crate::page::INTERNAL_NODE_TAG;
use crate::page::InternalPage;
use crate::page::LEAF_NODE_TAG;
use crate::page::LeafPage;

pub struct DefaultNodeCodec;
pub struct NoopNodeViewCodec;

const MAX_KEY_SIZE: usize = 256; // Maximum key size for internal nodes
const MAX_VAL_SIZE: usize = 256; // Maximum key size for internal nodes

impl KeyCodec for u64 {
    fn encode_key(&self, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<u64>();
        out[..size].copy_from_slice(&self.to_be_bytes());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        //u64::from_le_bytes(arr)
        u64::from_be_bytes(arr)
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        u64::decode_key(a).cmp(&u64::decode_key(b))
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl ValueCodec for u64 {
    fn encode_value(&self, out: &mut [u8]) -> Result<usize, CodecError> {
        let size = std::mem::size_of::<u64>();
        out[..size].copy_from_slice(&self.to_be_bytes());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        u64::from_le_bytes(arr)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl KeyCodec for String {
    fn encode_key(&self, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = self.len();
        buf[..size].copy_from_slice(self.as_bytes());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Self {
        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b) // bytewise lexicographic compare (memcmp-ish)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }
}

impl ValueCodec for String {
    fn encode_value(&self, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = self.len();
        buf[..size].copy_from_slice(self.as_bytes());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Self {
        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }
}

impl KeyCodec for Vec<u8> {
    fn encode_key(&self, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = self.len();
        buf[..size].copy_from_slice(self.as_slice());
        Ok(size)
    }

    fn decode_key(buf: &[u8]) -> Self {
        buf.to_vec()
    }

    #[inline]
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }
}

impl ValueCodec for Vec<u8> {
    fn encode_value(&self, buf: &mut [u8]) -> Result<usize, CodecError> {
        let size = self.len();
        buf[..size].copy_from_slice(self.as_slice());
        Ok(size)
    }

    fn decode_value(buf: &[u8]) -> Self {
        buf.to_vec()
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }
}

impl<K, V> NodeCodec<K, V> for DefaultNodeCodec
where
    K: KeyCodec + Ord,
    V: ValueCodec,
{
    fn decode(buf: &[u8; PAGE_SIZE]) -> Result<Node<K, V>, CodecError> {
        let node_type = u64::from_le_bytes(
            buf[0..8]
                .try_into()
                .map_err(|e| CodecError::FromSliceError { source: e })?,
        );
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
                    for i in 0..page.header.entry_count as usize {
                        let (key_bytes, value_bytes) = page
                            .get_entry(i)
                            .map_err(|e| CodecError::DecodeFailure { msg: e.to_string() })?;
                        keys.push(K::decode_key(key_bytes));
                        values.push(V::decode_value(value_bytes));
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
                        keys.push(K::decode_key(key_bytes));
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
    where
        K: KeyCodec + Sized,
        V: ValueCodec + Sized,
    {
        match node {
            Node::Leaf { keys, values } => {
                let mut page = LeafPage::new();
                {
                    let mut encode_buf_key: Vec<u8> = Vec::with_capacity(MAX_KEY_SIZE);
                    let mut encode_buf_val: Vec<u8> = Vec::with_capacity(MAX_VAL_SIZE);
                    for (key_ref, value_ref) in keys.iter().zip(values.iter()) {
                        encode_buf_key.resize(key_ref.encoded_len(), 0);
                        encode_buf_val.resize(value_ref.encoded_len(), 0);
                        key_ref
                            .encode_key(encode_buf_key.as_mut())
                            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                        value_ref
                            .encode_value(encode_buf_val.as_mut())
                            .map_err(|e| CodecError::EncodeFailure { msg: e.to_string() })?;
                        page.insert_entry(&encode_buf_key, &encode_buf_val)
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
                    encode_buf.resize(key_ref.encoded_len(), 0);
                    key_ref
                        .encode_key(encode_buf.as_mut())
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
        let node_type = u64::from_le_bytes(
            buf[0..8]
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
