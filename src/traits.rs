use crate::core::c_defaults::{c_error_string, new_data_read, new_index_read};
use crate::core::data_types::{DataType, OmFileArrayDataType};
use crate::errors::OmFilesError;
use crate::io::reader::OmFileReader;
use crate::io::variable::{OmOffsetSize, OmVariableContainer};
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_t, OmError_t, om_decoder_decode_chunks, om_decoder_next_data_read,
    om_decoder_next_index_read, om_variable_get_children,
};
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Range};
use std::os::raw::c_void;

pub trait OmFileWriterBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesError>;
    fn synchronize(&self) -> Result<(), OmFilesError>;
}

/// A trait for reading byte data synchronously from different storage backends.
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
    fn get_bytes(&self, _offset: u64, _count: u64) -> Result<Self::Bytes<'_>, OmFilesError>;

    fn decode<OmType: OmFileArrayDataType>(
        &self,
        decoder: &OmDecoder_t,
        into: &mut ArrayD<OmType>,
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesError> {
        let into_ptr = into
            .as_slice_mut()
            .ok_or(OmFilesError::ArrayNotContiguous)?
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
}

/// A trait for reading byte data asynchronously from different storage backends.
pub trait OmFileReaderBackendAsync: Send + Sync {
    /// Length in bytes
    fn count_async(&self) -> usize;

    fn get_bytes_async(
        &self,
        _offset: u64,
        _count: u64,
    ) -> impl Future<Output = Result<Vec<u8>, OmFilesError>> + Send;
}

pub(crate) trait OmFileVariableImpl {
    fn variable(&self) -> &OmVariableContainer;
}

/// Any variable in an OmFile.
///
/// Any variable has a name, data type, and number of children.
/// Any variable stored in an OmFile should implement this trait.
pub trait OmFileVariable {
    /// Returns the data type of the variable
    fn data_type(&self) -> DataType;
    /// Returns the name of the variable, if available
    fn get_name(&self) -> Option<String>;
    /// Returns the number of children of the variable
    fn number_of_children(&self) -> u32;
}

// Blanket implementation for OmFileVariable for types implementing OmFileVariableImpl
impl<T: OmFileVariableImpl> OmFileVariable for T {
    fn data_type(&self) -> DataType {
        unsafe {
            DataType::try_from(
                om_file_format_sys::om_variable_get_type(*self.variable().variable) as u8,
            )
            .expect("Invalid data type")
        }
    }

    fn get_name(&self) -> Option<String> {
        unsafe {
            let name = om_file_format_sys::om_variable_get_name(*self.variable().variable);
            if name.size == 0 {
                return None;
            }
            let bytes = std::slice::from_raw_parts(name.value as *const u8, name.size as usize);
            String::from_utf8(bytes.to_vec()).ok()
        }
    }

    fn number_of_children(&self) -> u32 {
        unsafe { om_file_format_sys::om_variable_get_children_count(*self.variable().variable) }
    }
}

/// A scalar variable in an OmFile.
pub trait ScalarOmVariable {
    /// Read a scalar value of the specified type
    fn read_scalar<T: crate::core::data_types::OmFileScalarDataType>(&self) -> Option<T>;
}

// Blanket implementation for ScalarOmVariable
impl<U: OmFileVariableImpl + OmFileVariable> ScalarOmVariable for U {
    /// Read a scalar value of the specified type
    fn read_scalar<T: crate::core::data_types::OmFileScalarDataType>(&self) -> Option<T> {
        if T::DATA_TYPE_SCALAR != self.data_type() {
            return None;
        }

        let mut ptr: *mut std::os::raw::c_void = std::ptr::null_mut();
        let mut size: u64 = 0;

        let error = unsafe {
            om_file_format_sys::om_variable_get_scalar(
                *self.variable().variable,
                &mut ptr,
                &mut size,
            )
        };

        if error != om_file_format_sys::OmError_t::ERROR_OK || ptr.is_null() {
            return None;
        }

        // Safety: ptr points to a valid memory region of 'size' bytes
        // that contains data of the expected type
        let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, size as usize) };

        Some(T::from_raw_bytes(bytes))
    }
}

pub(crate) trait ArrayOmVariableImpl {
    fn io_size_max(&self) -> u64;
    fn io_size_merge(&self) -> u64;
}

/// An array variable in an OmFile.
pub trait ArrayOmVariable {
    /// Returns the compression type of the variable
    fn compression(&self) -> crate::core::compression::CompressionType;
    /// Returns the scale factor of the variable
    fn scale_factor(&self) -> f32;
    /// Returns the add offset of the variable
    fn add_offset(&self) -> f32;
    /// Returns the dimensions of the variable
    fn get_dimensions(&self) -> &[u64];
    /// Returns the chunk dimensions of the variable
    fn get_chunk_dimensions(&self) -> &[u64];

    /// Prepare common parameters for reading data
    fn prepare_read_parameters<T: OmFileArrayDataType>(
        &self,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<crate::io::wrapped_decoder::WrappedDecoder, OmFilesError>;
}

// Blanket implementation for any type that implements the private trait
impl<U: ArrayOmVariableImpl + OmFileVariableImpl> ArrayOmVariable for U {
    /// Returns the compression type of the variable
    fn compression(&self) -> crate::core::compression::CompressionType {
        unsafe {
            crate::core::compression::CompressionType::try_from(
                om_file_format_sys::om_variable_get_compression(*self.variable().variable) as u8,
            )
            .expect("Invalid compression type")
        }
    }

    /// Returns the scale factor of the variable
    fn scale_factor(&self) -> f32 {
        unsafe { om_file_format_sys::om_variable_get_scale_factor(*self.variable().variable) }
    }

    /// Returns the add offset of the variable
    fn add_offset(&self) -> f32 {
        unsafe { om_file_format_sys::om_variable_get_add_offset(*self.variable().variable) }
    }

    /// Returns the dimensions of the variable
    fn get_dimensions(&self) -> &[u64] {
        unsafe {
            let dims = om_file_format_sys::om_variable_get_dimensions(*self.variable().variable);
            std::slice::from_raw_parts(dims.values, dims.count as usize)
        }
    }

    /// Returns the chunk dimensions of the variable
    fn get_chunk_dimensions(&self) -> &[u64] {
        unsafe {
            let chunks = om_file_format_sys::om_variable_get_chunks(*self.variable().variable);
            std::slice::from_raw_parts(chunks.values, chunks.count as usize)
        }
    }

    /// Prepare common parameters for reading data
    fn prepare_read_parameters<T: OmFileArrayDataType>(
        &self,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<crate::io::wrapped_decoder::WrappedDecoder, OmFilesError> {
        let n_dimensions_read = dim_read.len();
        let n_dims = self.get_dimensions().len();

        // Validate dimension counts
        if n_dims != n_dimensions_read
            || n_dimensions_read != into_cube_offset.len()
            || n_dimensions_read != into_cube_dimension.len()
        {
            return Err(OmFilesError::MismatchingCubeDimensionLength);
        }

        // Prepare read parameters
        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

        // Initialize decoder
        let decoder = crate::io::wrapped_decoder::WrappedDecoder::new(
            self.variable().variable,
            n_dimensions_read as u64,
            read_offset,
            read_count,
            into_cube_offset,
            into_cube_dimension,
            self.io_size_merge(),
            self.io_size_max(),
        )?;

        Ok(decoder)
    }
}

pub(crate) trait OmFileReadableImpl<Backend: OmFileReaderBackend>:
    OmFileVariableImpl + OmFileVariable
{
    fn new_with_variable(&self, variable: OmVariableContainer) -> OmFileReader<Backend>;
    fn backend(&self) -> &Backend;

    fn get_child(&self, index: u32) -> Option<OmFileReader<Backend>> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !unsafe {
            om_variable_get_children(*self.variable().variable, index, 1, &mut offset, &mut size)
        } {
            return None;
        }

        let offset_size = OmOffsetSize::new(offset, size);
        self.init_child_from_offset_size(offset_size).ok()
    }

    fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        let child_variable = self
            .backend()
            .get_bytes(offset_size.offset, offset_size.size)?
            .to_vec();

        Ok(self.new_with_variable(OmVariableContainer::new(child_variable, Some(offset_size))))
    }

    /// Helper function that recursively collects variable metadata
    fn collect_variable_metadata(
        &self,
        mut current_path: Vec<String>,
        result: &mut HashMap<String, OmOffsetSize>,
    ) {
        // Add current variable's metadata if it has a name and offset_size
        // TODO: This requires for names to be unique
        if let Some(name) = self.get_name() {
            if let Some(offset_size) = &self.variable().offset_size {
                current_path.push(name.to_string());
                // Create hierarchical key
                let path_str = current_path
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join("/");

                result.insert(path_str, offset_size.clone());
            }
        }

        // Process children
        let num_children = self.number_of_children();
        for i in 0..num_children {
            let child_path = current_path.clone();
            if let Some(child) = self.get_child(i) {
                child.collect_variable_metadata(child_path, result);
            }
        }
    }
}

/// A trait that allows for child traversal and metadata retrieval.
pub trait OmFileReadable<Backend: OmFileReaderBackend>: OmFileVariable {
    /// Returns a HashMap mapping variable names to their offset and size
    /// This function needs to traverse the entire variable tree, therefore
    /// it is best to make sure that variable metadata is close to each other
    /// at the end of the file (before the trailer). The caller could then
    /// make sure that this part of the file is loaded/cached in memory
    fn get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize>;

    /// Returns an OmFileReader at the given child index
    fn get_child(&self, index: u32) -> Option<OmFileReader<Backend>>;

    /// Initializes an OmFileReader from an offset and size
    fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError>;
}

// Blanket implementation
impl<T, Backend> OmFileReadable<Backend> for T
where
    T: OmFileReadableImpl<Backend>,
    Backend: OmFileReaderBackend,
{
    fn get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize> {
        let mut result = HashMap::new();
        self.collect_variable_metadata(Vec::new(), &mut result);
        result
    }

    fn get_child(&self, index: u32) -> Option<OmFileReader<Backend>> {
        OmFileReadableImpl::get_child(self, index)
    }

    fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        OmFileReadableImpl::init_child_from_offset_size(self, offset_size)
    }
}
