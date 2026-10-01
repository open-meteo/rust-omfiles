use crate::core::c_defaults::new_index_read;
use crate::core::c_defaults::{c_error_string, create_uninit_decoder, new_data_read};
use crate::traits::{OmFileArrayDataType, OmFileReaderBackend};
use crate::{errors::OmFilesError, variable::OmVariablePtr};
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_indexRead_t, OmDecoder_t, OmError_t, OmRange_t, om_decoder_decode_chunks,
    om_decoder_init, om_decoder_next_data_read, om_decoder_next_index_read,
    om_decoder_read_buffer_size, om_variable_get_dimensions_count, om_variable_get_type,
};
use std::ffi::c_void;
use std::ops::Range;

/// Binds validated read parameters to a destination and owns decoder scratch storage.
/// Moving this wrapper does not move any of the pointed-to allocations.
pub(crate) struct WrappedDecoder<'config, 'output, T: OmFileArrayDataType> {
    decoder: OmDecoder_t,
    output: &'output mut [T],
    chunk_buffer: Vec<u8>,
    // These fields anchor C pointers; they are never mutated or reallocated.
    _variable: &'config OmVariablePtr,
    _cube_offset: &'config [u64],
    _cube_dimensions: Vec<u64>,
    _read_count: Vec<u64>,
    _read_offset: Vec<u64>,
}

// SAFETY: Owned vectors remain allocated and unchanged when the wrapper moves.
// The shared references keep metadata and cube offsets alive and immutable
// for 'config; their referents are Sync. The destination is exclusively borrowed
// and T is Send. C retains no pointers into the wrapper itself, and all writes
// require &mut self, including writes to the owned scratch buffer.
unsafe impl<T: OmFileArrayDataType + Send> Send for WrappedDecoder<'_, '_, T> {}

impl<'config, 'output, T: OmFileArrayDataType> WrappedDecoder<'config, 'output, T> {
    /// Validate the destination and initialize the decoder for this read.
    pub(crate) fn new(
        variable: &'config OmVariablePtr,
        into: &'output mut ArrayD<T>,
        dim_read: &[Range<u64>],
        cube_offset: &'config [u64],
        io_size_merge: u64,
        io_size_max: u64,
    ) -> Result<Self, OmFilesError> {
        if unsafe { om_variable_get_type(variable.as_ptr()) } as u8 != T::DATA_TYPE_ARRAY as u8 {
            return Err(OmFilesError::InvalidDataType);
        }
        let rank = dim_read.len();
        if unsafe { om_variable_get_dimensions_count(variable.as_ptr()) } != rank as u64
            || cube_offset.len() != rank
            || into.ndim() != rank
        {
            return Err(OmFilesError::MismatchingCubeDimensionLength);
        }
        if !into.is_standard_layout() {
            return Err(OmFilesError::ArrayNotContiguous);
        }

        // C requires row-major destination dimensions.
        let cube_dim: Vec<u64> = into.shape().iter().map(|&dim| dim as u64).collect();
        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        for ((&offset, &count), &dimension) in cube_offset.iter().zip(&read_count).zip(&cube_dim) {
            if offset.checked_add(count).is_none_or(|end| end > dimension) {
                return Err(OmFilesError::OffsetAndCountExceedDimension {
                    offset,
                    count,
                    dimension,
                });
            }
        }
        let output = into
            .as_slice_mut()
            .ok_or(OmFilesError::ArrayNotContiguous)?;

        let mut decoder = unsafe { create_uninit_decoder() };
        // The checked ranks match C's parameter arrays. Their backing allocations
        // are retained by the wrapper for every later decoder call.
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
            output,
            chunk_buffer: Vec::new(),
            _variable: variable,
            _cube_offset: cube_offset,
            _cube_dimensions: cube_dim,
            _read_offset: read_offset,
            _read_count: read_count,
        })
    }

    /// Read and decode synchronously through the backend.
    pub(crate) fn decode<Backend: OmFileReaderBackend>(
        &mut self,
        backend: &Backend,
    ) -> Result<(), OmFilesError> {
        let mut index_read = self.new_index_read();
        while self.next_index_read(&mut index_read) {
            let index_data = backend.get_bytes(index_read.offset, index_read.count)?;
            let mut data_read = new_data_read(&index_read);
            let mut error = OmError_t::ERROR_OK;
            while unsafe {
                om_decoder_next_data_read(
                    &self.decoder,
                    &mut data_read,
                    index_data.as_ptr() as *const c_void,
                    index_read.count,
                    &mut error,
                )
            } {
                let data = backend.get_bytes(data_read.offset, data_read.count)?;
                self.decode_chunk(data_read.chunkIndex, &data)?;
            }
            if error != OmError_t::ERROR_OK {
                return Err(OmFilesError::DecoderError(c_error_string(error)));
            }
        }
        Ok(())
    }

    /// Decode a chunk using this decoder configuration
    pub(crate) fn decode_chunk(
        &mut self,
        chunk_index: OmRange_t,
        data: &[u8],
    ) -> Result<(), OmFilesError> {
        if self.chunk_buffer.is_empty() {
            let size = unsafe { om_decoder_read_buffer_size(&self.decoder) } as usize;
            self.chunk_buffer.resize(size, 0);
        }
        let mut error = OmError_t::ERROR_OK;

        // Construction fixes the destination type and geometry. Both writable
        // buffers remain alive and exclusively borrowed for this call.
        let success = unsafe {
            om_decoder_decode_chunks(
                &self.decoder,
                chunk_index,
                data.as_ptr() as *const c_void,
                data.len() as u64,
                self.output.as_mut_ptr() as *mut c_void,
                self.chunk_buffer.as_mut_ptr() as *mut c_void,
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
        let mut output = ArrayD::<i32>::from_elem(vec![3, 3], -1);
        let mut decoder =
            array.prepare_read_parameters::<i32>(&mut output, &[0..2, 0..2], &cube_offset)?;
        let backend = reader.backend.as_ref();

        // Borrowed metadata and offsets stay alive while the decoder moves.
        std::thread::scope(|scope| scope.spawn(move || decoder.decode(backend)).join().unwrap())?;
        assert_eq!(
            output.as_slice().unwrap(),
            &[-1, -1, -1, -1, 1, 2, -1, 3, 4]
        );
        Ok(())
    }
}
