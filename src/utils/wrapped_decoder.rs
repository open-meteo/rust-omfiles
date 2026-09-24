use crate::core::c_defaults::new_index_read;
use crate::core::c_defaults::{c_error_string, create_uninit_decoder, new_data_read};
use crate::traits::{OmFileArrayDataType, OmFileReaderBackend};
use crate::{errors::OmFilesError, variable::OmVariablePtr};
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_indexRead_t, OmDecoder_t, OmError_t, OmRange_t, om_decoder_decode_chunks,
    om_decoder_init, om_decoder_next_data_read, om_decoder_next_index_read,
    om_decoder_read_buffer_size,
};
use std::ffi::c_void;

/// Owns read parameters and borrows every other allocation referenced by C.
/// Moving this wrapper does not move any of the pointed-to allocations.
pub(crate) struct WrappedDecoder<'a> {
    decoder: OmDecoder_t,
    // These fields anchor C pointers; they are never mutated or reallocated.
    _variable: &'a OmVariablePtr,
    _cube_offset: &'a [u64],
    _cube_dimensions: &'a [u64],
    _read_count: Vec<u64>,
    _read_offset: Vec<u64>,
}

// SAFETY: Owned vectors remain allocated and unchanged when the wrapper moves.
// The shared references keep metadata and cube parameters alive and immutable
// for 'a; their referents are Sync. C retains no pointers into the wrapper itself.
unsafe impl Send for WrappedDecoder<'_> {}
// SAFETY: C only reads the decoder configuration and its backing allocations
// after initialization. Mutable iterator state, output, and scratch storage are
// supplied separately for each operation, not stored in this shared wrapper.
unsafe impl Sync for WrappedDecoder<'_> {}

impl<'a> WrappedDecoder<'a> {
    /// Initialize the decoder with read parameters
    pub(crate) fn new(
        variable: &'a OmVariablePtr,
        read_offset: Vec<u64>,
        read_count: Vec<u64>,
        cube_offset: &'a [u64],
        cube_dim: &'a [u64],
        io_size_merge: u64,
        io_size_max: u64,
    ) -> Result<Self, OmFilesError> {
        let mut decoder = unsafe { create_uninit_decoder() };
        let error = unsafe {
            om_decoder_init(
                &mut decoder,
                variable.as_ptr(),
                read_count.len() as u64,
                read_offset.as_ptr(),
                read_count.as_ptr(),
                cube_offset.as_ptr(),
                cube_dim.as_ptr(),
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
            _variable: variable,
            _cube_offset: cube_offset,
            _cube_dimensions: cube_dim,
            _read_offset: read_offset,
            _read_count: read_count,
        })
    }

    /// Read and decode synchronously through the backend.
    pub(crate) fn decode<OmType: OmFileArrayDataType, Backend: OmFileReaderBackend>(
        &self,
        backend: &Backend,
        into: &mut ArrayD<OmType>,
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesError> {
        let decoder = &self.decoder;
        let into_ptr = into
            .as_slice_mut()
            .ok_or(OmFilesError::ArrayNotContiguous)?
            .as_mut_ptr();

        let mut index_read = new_index_read(decoder);
        unsafe {
            // Loop over index blocks and read index data
            while om_decoder_next_index_read(decoder, &mut index_read) {
                let index_data = backend.get_bytes(index_read.offset, index_read.count)?;

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
                    let data_data = backend.get_bytes(data_read.offset, data_read.count)?;

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
                        return Err(OmFilesError::DecoderError(error_string));
                    }
                }
                if error != OmError_t::ERROR_OK {
                    let error_string = c_error_string(error);
                    return Err(OmFilesError::DecoderError(error_string));
                }
            }
        }
        Ok(())
    }

    /// Get the required buffer size for decoding
    pub(crate) fn buffer_size(&self) -> usize {
        unsafe { om_decoder_read_buffer_size(&self.decoder) as usize }
    }

    /// Decode a chunk using this decoder configuration
    pub(crate) fn decode_chunk(
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

    pub(crate) fn new_index_read(&self) -> OmDecoder_indexRead_t {
        new_index_read(&self.decoder)
    }

    /// Process the next index block
    pub(crate) fn next_index_read(&self, index_read: &mut OmDecoder_indexRead_t) -> bool {
        unsafe { om_decoder_next_index_read(&self.decoder, index_read) }
    }

    /// Process data reads for an index block
    pub(crate) fn process_data_reads<F>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        InMemoryBackend, OmCompressionType, reader::OmFileReader, traits::OmArrayVariableImpl,
        writer::OmFileWriter,
    };
    use std::sync::Arc;

    #[test]
    fn moved_decoder_reads_into_offset_destination() -> Result<(), OmFilesError> {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WrappedDecoder<'_>>();
        assert_send_sync::<OmVariablePtr>();

        let mut backend = InMemoryBackend::new(vec![]);
        let mut writer = OmFileWriter::new(&mut backend, 1024);
        let mut array = writer.prepare_array::<i32>(
            vec![2, 2],
            vec![1, 2],
            OmCompressionType::PforDelta2d,
            1.0,
            0.0,
        )?;
        let values = ArrayD::from_shape_vec(vec![2, 2], vec![1, 2, 3, 4]).unwrap();
        array.write_data(values.view(), None, None)?;
        let array = array.finalize();
        let root = writer.write_array(array, "data", &[])?;
        writer.write_trailer(root)?;
        drop(writer);

        let reader = OmFileReader::new(Arc::new(backend))?;
        let array = reader.expect_array()?;
        let cube_offset = vec![1, 1];
        let cube_dimensions = vec![3, 3];
        let decoder =
            array.prepare_read_parameters::<i32>(&[0..2, 0..2], &cube_offset, &cube_dimensions)?;
        let backend = reader.backend.as_ref();

        // Move the decoder while its borrowed metadata and cube parameters
        // remain on this thread. Read parameters are owned by the decoder.
        let output = std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let mut output = ArrayD::<i32>::from_elem(vec![3, 3], -1);
                    let mut scratch = vec![0; decoder.buffer_size()];
                    decoder.decode(backend, &mut output, &mut scratch)?;
                    Ok::<_, OmFilesError>(output)
                })
                .join()
                .unwrap()
        })?;
        assert_eq!(
            output.as_slice().unwrap(),
            &[-1, -1, -1, -1, 1, 2, -1, 3, 4]
        );
        Ok(())
    }
}
