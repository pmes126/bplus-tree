//! Manifest log record definitions and binary encoding.

pub mod reader;
pub mod writer;

use crate::api::{KeyEncodingId, KeyLimits, TreeId};
use crate::keyfmt::KeyFormat;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;

const TAG_CREATE_TREE: u8 = 1;
const TAG_DELETE_TREE: u8 = 2;
const TAG_RENAME_TREE: u8 = 3;

/// An append-only log record describing a change to the tree catalog.
#[derive(Debug, Clone)]
pub enum ManifestRec {
    /// A new B+ tree was created.
    CreateTree {
        /// Manifest sequence number assigned at write time.
        seq: u64,
        /// Stable numeric identifier for the new tree.
        id: TreeId,
        /// Page ID of metadata slot A.
        meta_a: u64,
        /// Page ID of metadata slot B.
        meta_b: u64,
        /// Logical name of the tree.
        name: String,
        /// Key encoding strategy.
        key_encoding: KeyEncodingId,
        /// On-page key layout.
        key_format: KeyFormat,
        /// On-page encoding version.
        encoding_version: u16,
        /// Optional key length constraints.
        key_limits: Option<KeyLimits>,
        /// B+ tree order (branching factor).
        order: u64,
        /// Initial root node page ID.
        root_id: u64,
        /// Initial tree height.
        height: u64,
        /// Initial entry count.
        size: u64,
    },
    /// An existing tree was renamed.
    RenameTree {
        /// Manifest sequence number.
        seq: u64,
        /// ID of the tree being renamed.
        id: TreeId,
        /// New logical name.
        new_name: String,
    },
    /// An existing tree was deleted.
    DeleteTree {
        /// Manifest sequence number.
        seq: u64,
        /// ID of the tree being deleted.
        id: TreeId,
    },
    /// A durable checkpoint was written (no catalog change).
    Checkpoint {
        /// Manifest sequence number.
        seq: u64,
    },
}

/// An in-memory collection of manifest records.
pub struct ManifestLog {
    /// Ordered list of manifest records.
    pub recs: Vec<ManifestRec>,
}

/// An open handle to a manifest file on disk.
pub struct ManifestFile {
    /// Path to the manifest file.
    pub path: PathBuf,
    /// Open file handle.
    pub file: File,
}

impl ManifestRec {
    /// Encodes this record into the given writer.
    pub fn encode(&self, mut w: impl Write) -> io::Result<()> {
        match self {
            ManifestRec::CreateTree {
                seq,
                id,
                name,
                meta_a,
                meta_b,
                key_encoding,
                key_format,
                encoding_version,
                key_limits,
                order,
                root_id,
                height,
                size,
            } => {
                w.write_all(&[TAG_CREATE_TREE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&id.to_le_bytes());
                write_string(&mut payload, name)?;
                payload.extend_from_slice(&meta_a.to_le_bytes());
                payload.extend_from_slice(&meta_b.to_le_bytes());
                payload.extend_from_slice(&(*key_encoding as u64).to_le_bytes());
                payload.extend_from_slice(&(key_format.id() as u64).to_le_bytes());
                payload.extend_from_slice(&(*encoding_version as u64).to_le_bytes());
                payload.extend_from_slice(&order.to_le_bytes());
                payload.extend_from_slice(&root_id.to_le_bytes());
                payload.extend_from_slice(&height.to_le_bytes());
                payload.extend_from_slice(&size.to_le_bytes());
                if let Some(limits) = key_limits {
                    payload.push(1); // 1 = has limits
                    payload.extend_from_slice(&limits.min_len.to_le_bytes());
                    payload.extend_from_slice(&limits.max_len.to_le_bytes());
                } else {
                    payload.push(0); // 0 = no limits
                }

                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::DeleteTree { seq, id } => {
                w.write_all(&[TAG_DELETE_TREE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&id.to_le_bytes());
                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::RenameTree { seq, id, new_name } => {
                w.write_all(&[TAG_RENAME_TREE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&id.to_le_bytes());
                write_string(&mut payload, new_name)?;

                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::Checkpoint { seq } => {
                w.write_all(&[0])?; // 0 = checkpoint tag
                let payload = seq.to_le_bytes();
                write_len_prefixed_payload(&mut w, &payload)
            }
        }
    }

    /// Decodes a record from the given reader.
    pub fn decode(mut r: impl Read) -> io::Result<Self> {
        let mut tag = [0u8; size_of::<u8>()];
        r.read_exact(&mut tag)?;

        let payload = read_len_prefixed_payload(&mut r)?;
        let mut cur = &payload[..];

        match tag[0] {
            TAG_CREATE_TREE => {
                let seq = read_u64(&mut cur)?;
                let id = read_u64(&mut cur)?;
                let name = read_string(&mut cur)?;
                let meta_a = read_u64(&mut cur)?;
                let meta_b = read_u64(&mut cur)?;
                let key_encoding = KeyEncodingId::try_from(read_u64(&mut cur)?).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid key encoding id")
                })?;
                let key_format_id = read_u64(&mut cur)? as u16;
                let encoding_version = read_u64(&mut cur)? as u16;
                let order = read_u64(&mut cur)?;
                let root_id = read_u64(&mut cur)?;
                let height = read_u64(&mut cur)?;
                let size = read_u64(&mut cur)?;
                let has_limits = {
                    let mut b = [0u8; 1];
                    cur.read_exact(&mut b)?;
                    b[0] != 0
                };
                let key_limits = if has_limits {
                    let min_len = read_u64(&mut cur)? as u32;
                    let max_len = read_u64(&mut cur)? as u32;
                    Some(KeyLimits { min_len, max_len })
                } else {
                    None
                };
                let key_format = KeyFormat::from_id(key_format_id as u8).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown key format id: {}", key_format_id),
                ))?;

                Ok(Self::CreateTree {
                    seq,
                    id,
                    name,
                    meta_a,
                    meta_b,
                    key_encoding,
                    key_format,
                    encoding_version,
                    key_limits,
                    order,
                    root_id,
                    height,
                    size,
                })
            }
            TAG_DELETE_TREE => {
                let seq = read_u64(&mut cur)?;
                let id = read_u64(&mut cur)?;
                Ok(Self::DeleteTree { seq, id })
            }
            TAG_RENAME_TREE => {
                let seq = read_u64(&mut cur)?;
                let id = read_u64(&mut cur)?;
                let new_name = read_string(&mut cur)?;
                Ok(Self::RenameTree { seq, id, new_name })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown manifest tag: {}", other),
            )),
        }
    }
}

/// Reads a little-endian u64 from `r`.
fn read_u64(mut r: impl Read) -> io::Result<u64> {
    let mut buf = [0u8; size_of::<u64>()];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Writes a length-prefixed UTF-8 string.
fn write_string(mut w: impl Write, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string too long"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(bytes)?;
    Ok(())
}

/// Reads a length-prefixed UTF-8 string.
fn read_string(mut r: impl Read) -> io::Result<String> {
    let mut len_buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut str_buf = vec![0u8; len];
    r.read_exact(&mut str_buf)?;
    String::from_utf8(str_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Writes a u32 length prefix followed by `payload`.
fn write_len_prefixed_payload(mut w: impl Write, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;

    w.write_all(&len.to_le_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

/// Reads a u32 length prefix and returns the following payload bytes.
fn read_len_prefixed_payload(mut r: impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(payload)
}
