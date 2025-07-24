use crate::backend::mmapfile::{MAdvice, MmapFile, MmapType};
use crate::core::c_defaults::{c_error_string, new_data_read, new_index_read};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_t, OmError_t, om_decoder_decode_chunks, om_decoder_next_data_read,
    om_decoder_next_index_read,
};
use std::borrow::Cow;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::os::raw::c_void;

pub trait OmFileWriterBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError>;
    fn synchronize(&self) -> Result<(), OmFilesRsError>;
}

/// A trait for reading byte data from different storage backends.
/// Provides methods for reading bytes either by reference or as owned data,
/// as well as functions for prefetching and pre-reading data.
pub trait OmFileReaderBackend: Send + Sync {
    /// Length in bytes
    fn count(&self) -> usize;
    fn needs_prefetch(&self) -> bool;
    fn prefetch_data(&self, offset: usize, count: usize);
    fn pre_read(&self, offset: usize, count: usize) -> Result<(), OmFilesRsError>;

    /// Returns a reference to a slice of bytes from the backend, starting at `offset` and reading `count` bytes.
    /// At least one of `get_bytes` or `get_bytes_owned` must be implemented.
    fn get_bytes(&self, _offset: u64, _count: u64) -> Result<&[u8], OmFilesRsError> {
        Err(OmFilesRsError::NotImplementedError(
            "You need to implement either get_bytes or get_bytes_owned!".to_string(),
        ))
    }

    /// Returns an owned Vec<u8> containing bytes from the backend, starting at `offset` and reading `count` bytes.
    /// At least one of `get_bytes` or `get_bytes_owned` must be implemented.
    fn get_bytes_owned(&self, _offset: u64, _count: u64) -> Result<Vec<u8>, OmFilesRsError> {
        Err(OmFilesRsError::NotImplementedError(
            "You need to implement either get_bytes or get_bytes_owned!".to_string(),
        ))
    }

    /// Returns a reference to a slice of bytes from the backend, starting at `offset` and reading `count` bytes.
    /// At least one of `get_bytes` or `get_bytes_owned` must be implemented.
    ///
    /// This method is a fallback implementation that uses `get_bytes_owned` if `get_bytes` is not implemented.
    /// It is using Cow semantics to avoid unnecessary cloning.
    fn get_bytes_with_fallback<'a>(
        &'a self,
        offset: u64,
        count: u64,
    ) -> Result<Cow<'a, [u8]>, OmFilesRsError> {
        match self.get_bytes(offset, count) {
            Ok(bytes) => Ok(Cow::Borrowed(bytes)),
            Err(e) => Ok(Cow::Owned(self.forward_unimplemented_error(e, || {
                self.get_bytes_owned(offset, count)
            })?)),
        }
    }

    fn forward_unimplemented_error<'a, F, T>(
        &'a self,
        e: OmFilesRsError,
        fallback_fn: F,
    ) -> Result<T, OmFilesRsError>
    where
        F: FnOnce() -> Result<T, OmFilesRsError>,
    {
        match e {
            OmFilesRsError::NotImplementedError(_) => fallback_fn(),
            _ => Err(e),
        }
    }

    fn decode<OmType: OmFileArrayDataType>(
        &self,
        decoder: &OmDecoder_t,
        into: &mut ArrayD<OmType>,
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesRsError> {
        let into_ptr = into
            .as_slice_mut()
            .ok_or(OmFilesRsError::ArrayNotContiguous)?
            .as_mut_ptr();

        let mut index_read = new_index_read(decoder);
        unsafe {
            // Loop over index blocks and read index data
            while om_decoder_next_index_read(decoder, &mut index_read) {
                let index_data =
                    self.get_bytes_with_fallback(index_read.offset, index_read.count)?;

                let mut data_read = new_data_read(&index_read);

                let mut error = OmError_t::ERROR_OK;

                // Loop over data blocks and read compressed data chunks
                while om_decoder_next_data_read(
                    decoder,
                    &mut data_read,
                    index_data.as_ptr() as *const c_void,
                    index_read.count,
                    &mut error,
                ) {
                    let data_data =
                        self.get_bytes_with_fallback(data_read.offset, data_read.count)?;

                    if !om_decoder_decode_chunks(
                        decoder,
                        data_read.chunkIndex,
                        data_data.as_ptr() as *const c_void,
                        data_read.count,
                        into_ptr as *mut c_void,
                        chunk_buffer.as_mut_ptr() as *mut c_void,
                        &mut error,
                    ) {
                        let error_string = c_error_string(error);
                        return Err(OmFilesRsError::DecoderError(error_string));
                    }
                }
                if error != OmError_t::ERROR_OK {
                    let error_string = c_error_string(error);
                    return Err(OmFilesRsError::DecoderError(error_string));
                }
            }
        }
        Ok(())
    }
}

pub trait OmFileReaderBackendAsync: Send + Sync {
    /// Length in bytes
    fn count_async(&self) -> usize;

    fn get_bytes_async(
        &self,
        _offset: u64,
        _count: u64,
    ) -> impl Future<Output = Result<Vec<u8>, OmFilesRsError>> + Send;
}

fn map_io_error(e: std::io::Error) -> OmFilesRsError {
    OmFilesRsError::FileWriterError {
        errno: e.raw_os_error().unwrap_or(0),
        error: e.to_string(),
    }
}

impl OmFileWriterBackend for &File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all().map_err(|e| map_io_error(e))?;
        Ok(())
    }
}

impl OmFileWriterBackend for File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all().map_err(|e| map_io_error(e))?;
        Ok(())
    }
}

impl OmFileReaderBackend for MmapFile {
    fn count(&self) -> usize {
        self.data.len()
    }

    fn needs_prefetch(&self) -> bool {
        true
    }

    fn prefetch_data(&self, offset: usize, count: usize) {
        self.prefetch_data_advice(offset, count, MAdvice::WillNeed);
    }

    fn pre_read(&self, _offset: usize, _count: usize) -> Result<(), OmFilesRsError> {
        // No-op for mmaped file
        Ok(())
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<&[u8], OmFilesRsError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        match self.data {
            MmapType::ReadOnly(ref mmap) => Ok(&mmap[index_range]),
            MmapType::ReadWrite(ref mmap_mut) => Ok(&mmap_mut[index_range]),
        }
    }
}

impl OmFileReaderBackendAsync for MmapFile {
    fn count_async(&self) -> usize {
        self.data.len()
    }

    async fn get_bytes_async(&self, offset: u64, count: u64) -> Result<Vec<u8>, OmFilesRsError> {
        let data = self.get_bytes(offset, count);
        Ok(data?.to_vec())
    }
}

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
    fn count(&self) -> usize {
        self.data.len()
    }

    fn needs_prefetch(&self) -> bool {
        false
    }

    fn prefetch_data(&self, _offset: usize, _count: usize) {
        // No-op for in-memory backend
    }

    fn pre_read(&self, _offset: usize, _count: usize) -> Result<(), OmFilesRsError> {
        // No-op for in-memory backend
        Ok(())
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<&[u8], OmFilesRsError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        Ok(&self.data[index_range])
    }
}
