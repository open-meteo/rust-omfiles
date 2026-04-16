//! In-memory backend implementation for OmFiles.

use crate::{
    errors::OmFilesError,
    traits::{OmFileReaderBackend, OmFileWriterBackend},
    utils::byte_range::checked_byte_range,
};

/// In-memory backend implementation for OmFiles.
///
/// Implements the [`OmFileReaderBackend`](`OmFileReaderBackend`) and [`OmFileWriterBackend`](`OmFileWriterBackend`) traits.
#[derive(Debug)]
pub struct InMemoryBackend {
    data: Vec<u8>,
}

impl InMemoryBackend {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    // This is required in tests to corrupt the written data on purpose.
    #[cfg(test)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }
}

impl OmFileWriterBackend for &mut InMemoryBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesError> {
        self.data.extend_from_slice(data);
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesError> {
        // No-op for in-memory backend
        Ok(())
    }
}

impl OmFileReaderBackend for InMemoryBackend {
    type Bytes<'a> = &'a [u8];

    fn count(&self) -> usize {
        self.data.len()
    }

    fn prefetch_data(&self, _offset: usize, _count: usize) {
        // No-op for in-memory backend
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<Self::Bytes<'_>, OmFilesError> {
        let range = checked_byte_range(offset, count, self.data.len())?;
        Ok(&self.data[range])
    }
}
