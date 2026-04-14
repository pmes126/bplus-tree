//! Append-only writer for the manifest log.

use crate::database::manifest::ManifestRec;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

/// Appends [`ManifestRec`] entries to the manifest log file, assigning monotonic sequence numbers.
pub struct ManifestWriter {
    file: File,
    /// Next sequence number to assign.
    pub seq: u64,
}

impl ManifestWriter {
    /// Opens or creates the manifest log at `path`, resuming sequence numbering from `start_seq`.
    pub fn open(path: &Path, start_seq: u64) -> io::Result<Self> {
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        // Optional: scan tail to detect last good seq; otherwise trust start_seq from replay.
        Ok(Self {
            file: f,
            seq: start_seq,
        })
    }

    /// Assigns the next sequence number to `rec`, encodes it, and appends it to the log.
    ///
    /// Returns the sequence number assigned to the record.
    pub fn append(&mut self, mut rec: ManifestRec) -> io::Result<u64> {
        self.seq += 1;
        set_seq(&mut rec, self.seq);

        // TODO: add crc32c framing.
        rec.encode(self.file.by_ref())?;
        self.file.flush()?;
        Ok(self.seq)
    }

    /// Flushes and syncs the manifest file to durable storage.
    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Sets the sequence number field on any [`ManifestRec`] variant.
fn set_seq(rec: &mut ManifestRec, seq: u64) {
    match rec {
        ManifestRec::CreateTree { seq: s, .. } => *s = seq,
        ManifestRec::RenameTree { seq: s, .. } => *s = seq,
        ManifestRec::DeleteTree { seq: s, .. } => *s = seq,
        ManifestRec::Checkpoint { seq: s } => *s = seq,
    }
}
