//! Superblock and freelist snapshot helpers for the page store.

use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

/// Magic number identifying a superblock ("SUPR" in ASCII).
pub const SUPERBLOCK_MAGIC: u32 = 0x53555052;
/// Current on-disk format version. Bumped on breaking layout changes.
pub const SUPERBLOCK_VERSION: u32 = 1;
const SUPERBLOCK_SIZE: usize = std::mem::size_of::<Superblock>();

/// Magic number for a freelist snapshot file ("FLS1" in little-endian).
pub const FREELIST_SNAPSHOT_MAGIC: u32 = 0x314C5346;
/// Version of the freelist snapshot format.
pub const FREELIST_SNAPSHOT_VERSION: u16 = 1;
/// Size in bytes of a [`FreeListSnaphotHeader`].
pub const FREELIST_SNAPSHOT_HEADER_SIZE: usize = std::mem::size_of::<FreeListSnaphotHeader>();

/// Reads the superblock from `pages.data` at the given byte offset.
pub fn read_superblock(path: &std::path::Path, offset: u64) -> Result<Superblock, std::io::Error> {
    let page_path = path.join("pages.data");
    let file = std::fs::OpenOptions::new().read(true).open(page_path)?;
    let mut buf = [0u8; size_of::<Superblock>()];
    file.read_exact_at(&mut buf, offset)?;
    let sb = Superblock::from_bytes(&buf)?;
    Ok(*sb)
}

/// Writes the current freelist to a snapshot file.
///
/// TODO: implement as a linked list of pages — after a long operation the freed pages may not
/// fit in a single page.
pub fn write_freepages_snapshot(
    path: &Path,
    version: u16,
    next_pid: u64,
    ids: &[u64],
) -> Result<(), std::io::Error> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let hdr = FreeListSnaphotHeader {
        magic: FREELIST_SNAPSHOT_MAGIC,
        version,
        _pad: 0,
        next_page_id: next_pid,
        count: ids.len() as u32,
        _pad2: 0,
    };
    f.write_all(hdr.as_bytes())?;
    for &pid in ids {
        f.write_all(&pid.to_le_bytes())?;
    }
    Ok(())
}

/// Reads a freelist snapshot and returns `(next_page_id, freed_page_ids)`.
pub fn read_freepages_snapshot(
    path: &Path,
    offset: u64,
) -> Result<(u64, Vec<u64>), std::io::Error> {
    let mut f = std::fs::OpenOptions::new().read(true).open(path)?;
    let mut buf = [0u8; FREELIST_SNAPSHOT_HEADER_SIZE];
    f.read_exact_at(&mut buf, offset)?;
    let hdr = FreeListSnaphotHeader::from_bytes(&buf)?;
    hdr.validate()?;
    let mut ids = vec![0u64; hdr.count as usize];
    for slot in &mut ids {
        let mut b = [0u8; 8];
        f.read_exact(&mut b)?;
        *slot = u64::from_le_bytes(b);
    }
    Ok((hdr.next_page_id, ids))
}

/// Fixed-location page that stores critical page-store metadata.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct Superblock {
    /// Magic identifier for the superblock format.
    pub magic: u32,
    /// Format version number.
    pub version: u32,
    /// Monotonically increasing generation counter.
    pub gen_id: u64,
    /// Page size used by this store.
    pub page_size: u64,
    /// Next page ID to allocate.
    pub next_page_id: u64,
    /// Head page of the freelist chain (0 = none).
    pub freelist_head: u64,
    /// CRC-32C over the superblock fields.
    pub crc32c: u32,
    pub _pad: u32,
}

/// Header of a freelist snapshot page.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct FreeListSnaphotHeader {
    /// Magic number ("FLS1").
    pub magic: u32,
    /// Snapshot format version.
    pub version: u16,
    pub _pad: u16,
    /// Next page ID at snapshot time.
    pub next_page_id: u64,
    /// Number of freed page IDs recorded in this snapshot.
    pub count: u32,
    pub _pad2: u32,
}

impl Superblock {
    /// Interprets a fixed-size buffer as a [`Superblock`] reference.
    pub fn from_bytes(buf: &[u8; SUPERBLOCK_SIZE]) -> Result<&Self, std::io::Error> {
        Superblock::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode Superblock",
        ))
    }

    /// Validates the magic number and version of this superblock.
    pub fn validate(&self) -> Result<(), std::io::Error> {
        if self.magic != SUPERBLOCK_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Superblock magic",
            ));
        }
        if self.version != SUPERBLOCK_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported manifest version",
            ));
        }
        Ok(())
    }
}

impl FreeListSnaphotHeader {
    /// Interprets a fixed-size buffer as a [`FreeListSnaphotHeader`] reference.
    pub fn from_bytes(buf: &[u8; FREELIST_SNAPSHOT_HEADER_SIZE]) -> Result<&Self, std::io::Error> {
        FreeListSnaphotHeader::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode FreeListSnaphotHeader",
        ))
    }

    /// Validates the magic number and version of this freelist snapshot header.
    pub fn validate(&self) -> Result<(), std::io::Error> {
        if self.magic != FREELIST_SNAPSHOT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Snapshot header magic",
            ));
        }
        if self.version != FREELIST_SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported manifest version",
            ));
        }
        Ok(())
    }
}
