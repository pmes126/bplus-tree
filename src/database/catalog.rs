use crate::api::{KeyEncodingId, TreeId};
use crate::database::manifest::{ManifestRec, ManifestRec::*};
use crate::keyfmt::KeyFormat;

use std::collections::HashMap;
/// Persistent catalog entry describing one logical B+ tree.
#[derive(Clone)]
pub struct TreeMeta {
    /// Stable opaque identifier for this tree (never reused).
    pub id: TreeId,
    /// Human-readable logical name (may change via rename).
    pub name: String,
    /// Comparator / byte ordering (e.g. big-endian u64, zigzag i64, UTF-8).
    pub key_encoding: KeyEncodingId,
    /// On-page key layout (e.g. Raw, PrefixRestarts).
    pub keyfmt_id: KeyFormat,
    /// On-page layout version for forward compatibility.
    pub format_version: u16,
    /// Page ID of metadata slot A.
    pub meta_a: u64,
    /// Page ID of metadata slot B.
    pub meta_b: u64,
    /// Currently committed root node page ID.
    pub root_id: u64,
    /// Current height of the B+ tree.
    pub height: usize,
    /// Approximate number of entries (copied from the metadata page).
    pub size: usize,
    /// Order of the B+ tree (copied from the metadata page).
    pub order: usize,
    /// Last manifest sequence number that modified this record.
    pub last_seq: u64,
}

/// In-memory catalog rebuilt by replaying the manifest log on startup.
///
/// Serves as the authoritative routing table from tree name or ID to its [`TreeMeta`].
#[derive(Clone)]
pub struct Catalog {
    /// Maps logical tree names to their numeric IDs.
    pub by_name: HashMap<String, TreeId>,
    /// Maps numeric tree IDs to their full metadata.
    pub metas: HashMap<TreeId, TreeMeta>,
    /// Next manifest sequence number to assign.
    pub next_seq: u64,
}

impl Catalog {
    /// Creates an empty catalog with sequence numbering starting at 1.
    pub fn new() -> Self {
        Self {
            by_name: HashMap::new(),
            metas: HashMap::new(),
            next_seq: 1,
        }
    }

    /// Looks up a tree by its logical name.
    pub fn get_by_name(&self, name: &str) -> Option<&TreeMeta> {
        self.by_name.get(name).and_then(|id| self.metas.get(id))
    }

    /// Looks up a tree by its numeric ID.
    pub fn get_by_id(&self, id: &TreeId) -> Option<&TreeMeta> {
        self.metas.get(id)
    }

    /// Applies a single manifest record to update the in-memory catalog state.
    pub fn replay_record(&mut self, rec: &ManifestRec) {
        match rec.clone() {
            CreateTree {
                seq,
                id,
                name,
                key_encoding,
                key_limits,
                key_format,
                encoding_version,
                meta_a,
                meta_b,
                root_id,
                order,
                height,
                size,
            } => {
                self.by_name.insert(name.clone(), id.clone());
                self.metas.insert(
                    id.clone(),
                    TreeMeta {
                        id: id.clone(),
                        name: name.clone(),
                        key_encoding,
                        keyfmt_id: key_format,
                        meta_a,
                        meta_b,
                        format_version: encoding_version,
                        order: order as usize,
                        root_id,
                        height: height as usize,
                        size: size as usize,
                        last_seq: seq,
                    },
                );
                let _key_limits = key_limits; // TODO: store and enforce key limits.
                self.next_seq = self.next_seq.max(seq + 1);
            }
            RenameTree { seq, id, new_name } => {
                if let Some(m) = self.metas.get_mut(&id) {
                    self.by_name.remove(&m.name);
                    m.name = new_name.clone();
                    self.by_name.insert(new_name.clone(), id.clone());
                    m.last_seq = seq;
                    self.next_seq = self.next_seq.max(seq + 1);
                }
            }
            DeleteTree { seq, id } => {
                if let Some(m) = self.metas.remove(&id) {
                    self.by_name.remove(&m.name);
                    self.next_seq = self.next_seq.max(seq + 1);
                }
            }
            Checkpoint { seq } => {
                self.next_seq = self.next_seq.max(seq + 1);
            }
        }
    }
}
