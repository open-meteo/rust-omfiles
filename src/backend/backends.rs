use crate::backend::mmapfile::{MAdvice, MmapFile, MmapType};
use crate::core::c_defaults::{c_error_string, new_data_read, new_index_read};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_t, OmError_t, om_decoder_decode_chunks, om_decoder_next_data_read,
    om_decoder_next_index_read,
};
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::ops::Deref;
use std::os::raw::c_void;

pub trait OmFileWriterBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError>;
    fn synchronize(&self) -> Result<(), OmFilesRsError>;
}

/// A trait for reading byte data from different storage backends.
/// Provides methods for reading bytes either by reference or as owned data,
/// as well as functions for prefetching and pre-reading data.
pub trait OmFileReaderBackend: Send + Sync {
    /// The type of the byte container returned by `get_bytes`.
    /// This can be a borrowed slice (`&'a [u8]`) or an owned container (`Vec<u8>`).
    /// The `Deref` bound allows us to treat it like a slice `&[u8]` easily.
    type Bytes<'a>: Deref<Target = [u8]> + Send + Sync
    where
        Self: 'a;

    /// Length in bytes
    fn count(&self) -> usize;

    /// Prefetch data for future access. E.g. madvice on memory mapped files
    fn prefetch_data(&self, offset: usize, count: usize);

    /// Returns a container of bytes from the backend.
    /// This might be a borrowed slice for zero-copy backends (like mmap)
    /// or an owned `Vec<u8>` for others (like file IO).
    fn get_bytes(&self, _offset: u64, _count: u64) -> Result<Self::Bytes<'_>, OmFilesRsError>;

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
                let index_data = self.get_bytes(index_read.offset, index_read.count)?;

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
                    let data_data = self.get_bytes(data_read.offset, data_read.count)?;

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
    type Bytes<'a> = &'a [u8];

    fn count(&self) -> usize {
        self.data.len()
    }

    fn prefetch_data(&self, offset: usize, count: usize) {
        self.prefetch_data_advice(offset, count, MAdvice::WillNeed);
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<Self::Bytes<'_>, OmFilesRsError> {
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
