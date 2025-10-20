//! All traits related to the Open-Meteo file format.
use crate::core::c_defaults::{c_error_string, new_data_read, new_index_read};
use crate::core::data_types::OmDataType;
use crate::errors::OmFilesError;
use crate::reader::OmFileReader;
use crate::reader_async::OmFileReaderAsync;
use crate::variable::{OmOffsetSize, OmVariableContainer};
use ndarray::ArrayD;
use om_file_format_sys::{
    OmDecoder_t, OmError_t, om_decoder_decode_chunks, om_decoder_next_data_read,
    om_decoder_next_index_read, om_variable_get_children,
};
#[cfg(feature = "metadata-tree")]
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Range};
use std::os::raw::c_void;
use std::{mem, slice};

/// Trait for types that can be stored as arrays in OmFiles
pub trait OmFileArrayDataType {
    const DATA_TYPE_ARRAY: OmDataType;
}

/// Trait for types that can be stored as scalars in OmFiles
pub trait OmFileScalarDataType: Default {
    const DATA_TYPE_SCALAR: OmDataType;

    /// Creates a new instance from raw bytes
    ///
    /// This is the default implementation, which assumes that the bytes
    /// represent a valid value of Self and that alignment requirements are met.
    fn from_raw_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= mem::size_of::<Self>(),
            "Buffer too small to contain type of size {}",
            mem::size_of::<Self>()
        );

        // Safety: This assumes the bytes represent a valid value of Self
        // and that alignment requirements are met
        unsafe {
            let mut result = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut result as *mut Self as *mut u8,
                mem::size_of::<Self>(),
            );
            result
        }
    }

    /// Performs an operation with the raw bytes of this value
    ///
    /// This is the default implementation, which passes a slice of the bytes
    /// of self to the provided closure.
    /// For String and OmNone types, this method is overridden to provide the
    /// UTF-8 bytes of the string and an empty slice, respectively.
    fn with_raw_bytes<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8]) -> T,
    {
        // Safety: This creates a slice that references the bytes of self
        let bytes = unsafe {
            slice::from_raw_parts(self as *const Self as *const u8, mem::size_of::<Self>())
        };
        f(bytes)
    }
}

/// A trait for writing byte data synchronously to different storage backends.
pub trait OmFileWriterBackend {
    /// Write bytes at the current position to the backend.
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesError>;

    /// Synchronize the backend's data to disk.
    fn synchronize(&self) -> Result<(), OmFilesError>;
}

/// A trait for reading byte data synchronously from different storage backends.
pub trait OmFileReaderBackend: Send + Sync {
    /// The type of byte container returned by [`get_bytes`](Self::get_bytes).
    ///
    /// For zero-copy backends (like memory-mapped files), this is typically `&[u8]`.
    /// For I/O-based backends, this is typically `Vec<u8>`.
    type Bytes<'a>: Deref<Target = [u8]> + Send + Sync
    where
        Self: 'a;

    /// Returns the total size of the data source in bytes.
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

    /// Do an madvice to load data chunks from disk into page cache in the background
    fn decode_prefetch(&self, decoder: &OmDecoder_t) -> Result<(), OmFilesError> {
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
                    // Prefetch the data chunk
                    self.prefetch_data(data_read.offset as usize, data_read.count as usize);
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

/// Represents any variable within an OM file structure.
///
/// OM files contain hierarchical variable structures where each variable
/// can contain metadata, data, and child variables. This trait provides
/// access to common variable properties.
///
/// # Variable Types
///
/// Variables can be:
/// - **Scalar variables**: Single values (integers, floats, strings)
/// - **Array variables**: Multi-dimensional arrays with compression
/// - **Group variables**: Containers holding other only children but no data
pub trait OmFileVariable {
    /// Returns the data type of this variable.
    fn data_type(&self) -> OmDataType;
    /// Returns the variable's name, if it has one.
    fn name(&self) -> &str;
    /// Returns the number of direct child variables.
    fn number_of_children(&self) -> u32;
}

// Blanket implementation of OmFileVariable for types implementing OmFileVariableImpl
impl<T: OmFileVariableImpl> OmFileVariable for T {
    fn data_type(&self) -> OmDataType {
        unsafe {
            OmDataType::try_from(
                om_file_format_sys::om_variable_get_type(*self.variable().variable) as u8,
            )
            .expect("Invalid data type")
        }
    }

    fn name(&self) -> &str {
        unsafe {
            let mut length = 0u16;
            let name =
                om_file_format_sys::om_variable_get_name(*self.variable().variable, &mut length);
            if name.is_null() || length == 0 {
                return "";
            }
            let bytes = std::slice::from_raw_parts(name as *const u8, length as usize);
            str::from_utf8(bytes).unwrap_or_default()
        }
    }

    fn number_of_children(&self) -> u32 {
        unsafe { om_file_format_sys::om_variable_get_children_count(*self.variable().variable) }
    }
}

pub(crate) trait OmScalarVariableImpl: OmFileVariableImpl + OmFileVariable {
    /// Read a scalar value of the specified type
    fn read_scalar<T: OmFileScalarDataType>(&self) -> Option<T> {
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

/// A scalar variable in an OmFile.
pub trait OmScalarVariable {
    /// Read a scalar value of the specified type
    fn read_scalar<T: OmFileScalarDataType>(&self) -> Option<T>;
}

// Blanket implementation of OmScalarVariable
impl<T: OmScalarVariableImpl> OmScalarVariable for T {
    fn read_scalar<U: OmFileScalarDataType>(&self) -> Option<U> {
        OmScalarVariableImpl::read_scalar(self)
    }
}

pub(crate) trait OmArrayVariableImpl: OmFileVariableImpl {
    fn io_size_max(&self) -> u64;
    fn io_size_merge(&self) -> u64;
}

/// An array variable in an OmFile.
pub trait OmArrayVariable {
    /// Returns the compression type of the variable
    fn compression(&self) -> crate::core::compression::OmCompressionType;
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
    ) -> Result<crate::utils::wrapped_decoder::WrappedDecoder, OmFilesError>;
}

// Blanket implementation of OmArrayVariable for types implementing OmArrayVariableImpl
impl<T: OmArrayVariableImpl> OmArrayVariable for T {
    /// Returns the compression type of the variable
    fn compression(&self) -> crate::core::compression::OmCompressionType {
        unsafe {
            crate::core::compression::OmCompressionType::try_from(
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
            let count =
                om_file_format_sys::om_variable_get_dimensions_count(*self.variable().variable);
            let dims = om_file_format_sys::om_variable_get_dimensions(*self.variable().variable);
            std::slice::from_raw_parts(dims, count as usize)
        }
    }

    /// Returns the chunk dimensions of the variable
    fn get_chunk_dimensions(&self) -> &[u64] {
        unsafe {
            let count =
                om_file_format_sys::om_variable_get_dimensions_count(*self.variable().variable);
            let chunks = om_file_format_sys::om_variable_get_chunks(*self.variable().variable);
            std::slice::from_raw_parts(chunks, count as usize)
        }
    }

    /// Prepare common parameters for reading data
    fn prepare_read_parameters<U: OmFileArrayDataType>(
        &self,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<crate::utils::wrapped_decoder::WrappedDecoder, OmFilesError> {
        if U::DATA_TYPE_ARRAY != self.data_type() {
            return Err(OmFilesError::InvalidDataType);
        }
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
        let decoder = crate::utils::wrapped_decoder::WrappedDecoder::new(
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

    fn get_child_by_index(&self, index: u32) -> Option<OmFileReader<Backend>> {
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

    fn get_child_by_name(&self, name: &str) -> Option<OmFileReader<Backend>> {
        for i in 0..self.number_of_children() {
            let child = self.get_child_by_index(i);
            if let Some(child) = child {
                if child.name() == name {
                    return Some(child);
                }
            }
        }
        None
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

    #[cfg(feature = "metadata-tree")]
    /// Helper function that recursively collects variable metadata
    fn collect_variable_metadata(
        &self,
        mut current_path: Vec<String>,
        result: &mut HashMap<String, OmOffsetSize>,
    ) {
        // Add current variable's metadata if it has a name and offset_size
        // TODO: This requires for names to be unique
        let name = self.name();
        if let Some(offset_size) = &self.variable()._offset_size {
            current_path.push(format!("/{}", name));
            // Create hierarchical key
            let path_str = current_path.join("");

            result.insert(path_str, offset_size.clone());
        }

        // Process children
        let num_children = self.number_of_children();
        for i in 0..num_children {
            let child_path = current_path.clone();
            if let Some(child) = self.get_child_by_index(i) {
                child.collect_variable_metadata(child_path, result);
            }
        }
    }
}

/// Provides navigation capabilities for hierarchical OM file structures.
///
/// This trait allows traversing the variable tree, accessing child variables,
/// and collecting metadata about the entire structure. It's the main interface
/// for exploring OM file contents.
pub trait OmFileReadable<Backend: OmFileReaderBackend>: OmFileVariable {
    /// Returns a reader for the child variable at the specified index.
    ///
    /// Child indices are zero-based and must be less than [`number_of_children()`](OmFileVariable::number_of_children).
    fn get_child_by_index(&self, index: u32) -> Option<OmFileReader<Backend>>;

    /// Returns a reader for the child variable with the specified name.
    ///
    /// Child names are case-sensitive and must match exactly.
    fn get_child_by_name(&self, name: &str) -> Option<OmFileReader<Backend>>;
}

impl<T, Backend> OmFileReadable<Backend> for T
where
    T: OmFileReadableImpl<Backend>,
    Backend: OmFileReaderBackend,
{
    fn get_child_by_index(&self, index: u32) -> Option<OmFileReader<Backend>> {
        OmFileReadableImpl::get_child_by_index(self, index)
    }

    fn get_child_by_name(&self, name: &str) -> Option<OmFileReader<Backend>> {
        OmFileReadableImpl::get_child_by_name(self, name)
    }
}

#[cfg(feature = "metadata-tree")]
pub trait OmFileVariableMetadataTree<Backend: OmFileReaderBackend> {
    /// Collects metadata for all variables in the hierarchy.
    ///
    /// This method traverses the entire variable tree and returns a mapping
    /// from hierarchical variable paths to their storage locations. The paths
    /// use forward slashes as separators (e.g., "root/group1/variable2").
    ///
    /// # Performance Notes
    ///
    /// This operation requires traversing the entire variable tree, so it's
    /// recommended to ensure variable metadata is cached or stored efficiently
    /// in the backend.
    fn _get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize>;

    /// Creates a reader for a variable at a specific storage location.
    ///
    /// This is typically used internally when navigating the variable hierarchy,
    /// but can also be used to directly access variables when their storage
    /// locations are known.
    fn _init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError>;
}

#[cfg(feature = "metadata-tree")]
impl<T, Backend> OmFileVariableMetadataTree<Backend> for T
where
    T: OmFileReadableImpl<Backend>,
    Backend: OmFileReaderBackend,
{
    fn _get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize> {
        let mut result = HashMap::new();
        self.collect_variable_metadata(Vec::new(), &mut result);
        result
    }

    fn _init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        OmFileReadableImpl::init_child_from_offset_size(self, offset_size)
    }
}

pub(crate) trait OmFileAsyncReadableImpl<Backend: OmFileReaderBackendAsync>:
    OmFileVariableImpl + OmFileVariable
{
    fn new_with_variable(&self, variable: OmVariableContainer) -> OmFileReaderAsync<Backend>;
    fn backend(&self) -> &Backend;

    async fn get_child_by_index(&self, index: u32) -> Option<OmFileReaderAsync<Backend>> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !unsafe {
            om_variable_get_children(*self.variable().variable, index, 1, &mut offset, &mut size)
        } {
            return None;
        }

        let offset_size = OmOffsetSize::new(offset, size);
        self.init_child_from_offset_size(offset_size).await.ok()
    }

    async fn get_child_by_name(&self, name: &str) -> Option<OmFileReaderAsync<Backend>> {
        for i in 0..self.number_of_children() {
            let child = self.get_child_by_index(i).await;
            if let Some(child) = child {
                if child.name() == name {
                    return Some(child);
                }
            }
        }
        None
    }

    async fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReaderAsync<Backend>, OmFilesError> {
        let child_variable = self
            .backend()
            .get_bytes_async(offset_size.offset, offset_size.size)
            .await?
            .to_vec();

        Ok(self.new_with_variable(OmVariableContainer::new(child_variable, Some(offset_size))))
    }
}

/// Provides navigation capabilities for hierarchical OM file structures in asynchronous contexts.
///
/// This trait allows traversing the variable tree, accessing child variables,
/// and collecting metadata about the entire structure. It's the main interface
/// for exploring OM file contents in asynchronous environments.
pub trait OmFileAsyncReadable<Backend: OmFileReaderBackendAsync>: OmFileVariable {
    /// Returns a reader for the child variable at the specified index.
    ///
    /// Child indices are zero-based and must be less than [`number_of_children()`](OmFileVariable::number_of_children).
    fn get_child_by_index(
        &self,
        index: u32,
    ) -> impl Future<Output = Option<OmFileReaderAsync<Backend>>>;

    /// Returns a reader for the child variable with the specified name.
    ///
    /// Child names are case-sensitive and must match exactly.
    fn get_child_by_name(
        &self,
        name: &str,
    ) -> impl Future<Output = Option<OmFileReaderAsync<Backend>>>;
}

// Blanket implementation for any OmFileReaderBackendAsync
impl<T, Backend> OmFileAsyncReadable<Backend> for T
where
    T: OmFileAsyncReadableImpl<Backend>,
    Backend: OmFileReaderBackendAsync,
{
    async fn get_child_by_index(&self, index: u32) -> Option<OmFileReaderAsync<Backend>> {
        OmFileAsyncReadableImpl::get_child_by_index(self, index).await
    }

    async fn get_child_by_name(&self, name: &str) -> Option<OmFileReaderAsync<Backend>> {
        OmFileAsyncReadableImpl::get_child_by_name(self, name).await
    }
}
