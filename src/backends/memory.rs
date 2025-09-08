//! In-memory backend implementation for OmFiles.

use crate::{
    errors::OmFilesRsError,
    traits::{OmFileReaderBackend, OmFileWriterBackend},
};

#[derive(Debug)]
pub struct InMemoryBackend {
    data: Vec<u8>,
}

impl InMemoryBackend {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl OmFileWriterBackend for &mut InMemoryBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.data.extend_from_slice(data);
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
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

    fn get_bytes(&self, offset: u64, count: u64) -> Result<Self::Bytes<'_>, OmFilesRsError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        Ok(&self.data[index_range])
    }
}
