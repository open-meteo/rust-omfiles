use omfiles::{
    InMemoryBackend, OmFilesError,
    traits::{OmFileReaderBackend, OmFileReaderBackendAsync},
};
use std::fs::{self};
use std::sync::Arc;

pub struct AsyncMemory(pub Arc<InMemoryBackend>);

impl OmFileReaderBackendAsync for AsyncMemory {
    type Bytes = Vec<u8>;

    fn count_async(&self) -> usize {
        self.0.count()
    }

    async fn get_bytes_async(&self, offset: u64, count: u64) -> Result<Vec<u8>, OmFilesError> {
        Ok(self.0.get_bytes(offset, count)?.to_vec())
    }
}

pub fn remove_file_if_exists(file: &str) {
    if fs::metadata(file).is_ok() {
        fs::remove_file(file).unwrap();
    }
}
