use crate::core::c_defaults::new_index_read;
use crate::core::c_defaults::{c_error_string, create_uninit_decoder, new_data_read};
use crate::{errors::OmFilesError, variable::OmVariablePtr};
use om_file_format_sys::{
    OmDecoder_indexRead_t, OmDecoder_t, OmError_t, OmRange_t, om_decoder_decode_chunks,
    om_decoder_init, om_decoder_next_data_read, om_decoder_next_index_read,
    om_decoder_read_buffer_size,
};
use std::ffi::c_void;

/// This is wrapping the OmDecoder_t struct to guarantee Send and Sync on it.
/// This is safe to do, because the underlying C-library does not modify the
/// const pointers contained within the OmDecoder_t struct.
pub struct WrappedDecoder {
    pub(crate) decoder: OmDecoder_t,
    // We need to store the read parameters, so their lifetime
    // remains valid throughout the use of the decoder
    #[allow(dead_code)]
    read_count: Vec<u64>,
    #[allow(dead_code)]
    read_offset: Vec<u64>,
}

unsafe impl Send for WrappedDecoder {}
unsafe impl Sync for WrappedDecoder {}

impl WrappedDecoder {
    /// Initialize the decoder with read parameters
    pub(crate) fn new(
        variable: OmVariablePtr,
        dims: u64,
        read_offset: Vec<u64>,
        read_count: Vec<u64>,
        cube_offset: Option<&[u64]>,
        cube_dim: Option<&[u64]>,
        io_size_merge: u64,
        io_size_max: u64,
    ) -> Result<Self, OmFilesError> {
        let mut decoder = unsafe { create_uninit_decoder() };
        let error = unsafe {
            om_decoder_init(
                &mut decoder,
                *variable,
                dims,
                read_offset.as_ptr(),
                read_count.as_ptr(),
                cube_offset.map_or(std::ptr::null(), |slice| slice.as_ptr()),
                cube_dim.map_or(std::ptr::null(), |slice| slice.as_ptr()),
                io_size_merge,
                io_size_max,
            )
        };

        if error != OmError_t::ERROR_OK {
            let error_string = c_error_string(error);
            return Err(OmFilesError::DecoderError(error_string));
        }

        Ok(Self {
            decoder,
            read_offset,
            read_count,
        })
    }

    /// Get the required buffer size for decoding
    pub fn buffer_size(&self) -> usize {
        unsafe { om_decoder_read_buffer_size(&self.decoder) as usize }
    }

    /// Decode a chunk safely
    pub fn decode_chunk(
        &self,
        chunk_index: OmRange_t,
        data: &[u8],
        output: &mut [u8], // Raw bytes of output array
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesError> {
        let mut error = OmError_t::ERROR_OK;

        let success = unsafe {
            om_decoder_decode_chunks(
                &self.decoder,
                chunk_index,
                data.as_ptr() as *const c_void,
                data.len() as u64,
                output.as_mut_ptr() as *mut c_void,
                chunk_buffer.as_mut_ptr() as *mut c_void,
                &mut error,
            )
        };

        if !success {
            let error_string = c_error_string(error);
            return Err(OmFilesError::DecoderError(error_string));
        }

        Ok(())
    }

    pub fn new_index_read(&self) -> OmDecoder_indexRead_t {
        new_index_read(&self.decoder)
    }

    /// Process the next index block
    pub fn next_index_read(&self, index_read: &mut OmDecoder_indexRead_t) -> bool {
        unsafe { om_decoder_next_index_read(&self.decoder, index_read) }
    }

    /// Process data reads for an index block
    pub fn process_data_reads<F>(
        &self,
        index_read: &OmDecoder_indexRead_t,
        index_data: &[u8],
        mut callback: F,
    ) -> Result<(), OmFilesError>
    where
        F: FnMut(u64, u64, OmRange_t) -> Result<(), OmFilesError>,
    {
        let mut data_read = new_data_read(index_read);
        let mut error = OmError_t::ERROR_OK;

        while unsafe {
            om_decoder_next_data_read(
                &self.decoder,
                &mut data_read,
                index_data.as_ptr() as *const c_void,
                index_data.len() as u64,
                &mut error,
            )
        } {
            if error != OmError_t::ERROR_OK {
                let error_string = c_error_string(error);
                return Err(OmFilesError::DecoderError(error_string));
            }
            // Pass relevant data to the callback
            callback(data_read.offset, data_read.count, data_read.chunkIndex)?;
        }

        Ok(())
    }
}
