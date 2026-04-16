//! Sequential reader for the manifest log.

use crate::database::manifest::ManifestRec;
use std::{
    fs::File,
    io::{self},
    path::Path,
};

/// Reads [`ManifestRec`] entries sequentially from a manifest log file.
pub struct ManifestReader {
    file: File,
}

impl ManifestReader {
    /// Opens an existing manifest log at `path` for sequential reading.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    /// Reads and decodes the next record from the log.
    ///
    /// Returns `None` at end of file, or an error on a corrupt or truncated record.
    pub fn read_next(&mut self) -> io::Result<Option<ManifestRec>> {
        // TODO: add CRC framing verification.
        ManifestRec::decode(&mut self.file).map(Some)
    }
}
