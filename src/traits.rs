//! All traits related to the Open-Meteo file format.
use crate::core::data_types::OmDataType;
use crate::errors::OmFilesError;
use crate::reader::OmFileReader;
use crate::reader_async::OmFileReaderAsync;
use crate::variable::{OmOffsetSize, OmVariablePtr};
use om_file_format_sys::om_variable_get_children;
#[cfg(feature = "metadata-tree")]
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, Range};

// Accessible within the crate, but downstream crates cannot name or implement it.
// OmFileArrayDataType and OmFileScalarDataType are part of the public interface
// and therefore need to be pub traits!
pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Types supported as numeric arrays: fixed-width integers, `f32`, and `f64`.
///
/// This trait is sealed. Its implementations guarantee that the Rust element
/// representation matches the associated OM type: no padding, drop glue, or
/// invalid bit patterns. Typed array pointers retain the primitive's alignment.
/// Downstream crates cannot add implementations.
pub trait OmFileArrayDataType: sealed::Sealed {
    const DATA_TYPE_ARRAY: OmDataType;
}

/// Types supported as scalars: fixed-width integers, `f32`, `f64`, and `String`.
///
/// This trait is sealed; the crate also implements it for its internal group
/// marker. Downstream crates cannot add implementations.
pub trait OmFileScalarDataType: Default + sealed::Sealed {
    const DATA_TYPE_SCALAR: OmDataType;

    /// Decode a scalar from bytes without requiring input alignment.
    ///
    /// Numeric implementations read a little-endian prefix and ignore trailing
    /// bytes. Strings use lossy UTF-8 decoding.
    ///
    /// # Panics
    /// Numeric implementations panic if fewer than `size_of::<Self>()` bytes
    /// are supplied. The internal group marker requires an empty slice.
    fn from_raw_bytes(bytes: &[u8]) -> Self;

    /// Invoke a callback with the scalar's encoded bytes.
    ///
    /// Numeric bytes use little-endian byte order and storage aligned for the C
    /// writer's integer loads. Strings expose UTF-8 bytes; groups expose none.
    fn with_raw_bytes<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8]) -> T;
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
    fn get_bytes(&self, offset: u64, count: u64) -> Result<Self::Bytes<'_>, OmFilesError>;
}

/// A trait for reading byte data asynchronously from different storage backends.
pub trait OmFileReaderBackendAsync: Send + Sync {
    /// The owned byte container returned by [`get_bytes_async`](Self::get_bytes_async).
    type Bytes: Deref<Target = [u8]> + Send + Sync + 'static;

    /// Length in bytes
    fn count_async(&self) -> usize;

    fn get_bytes_async(
        &self,
        _offset: u64,
        _count: u64,
    ) -> impl Future<Output = Result<Self::Bytes, OmFilesError>> + Send;
}

pub(crate) trait OmFileVariableImpl {
    fn variable(&self) -> &OmVariablePtr;
    fn offset_size(&self) -> &OmOffsetSize;
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
/// - **Group variables**: Containers holding only children but no data
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
                om_file_format_sys::om_variable_get_type(self.variable().as_ptr()) as u8,
            )
            .expect("Invalid data type")
        }
    }

    fn name(&self) -> &str {
        unsafe {
            let mut length = 0u16;
            let name =
                om_file_format_sys::om_variable_get_name(self.variable().as_ptr(), &mut length);
            if name.is_null() || length == 0 {
                return "";
            }
            let bytes = std::slice::from_raw_parts(name as *const u8, length as usize);
            str::from_utf8(bytes).unwrap_or_default()
        }
    }

    fn number_of_children(&self) -> u32 {
        unsafe { om_file_format_sys::om_variable_get_children_count(self.variable().as_ptr()) }
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
                self.variable().as_ptr(),
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

    /// Prepare common parameters for reading data
    fn prepare_read_parameters<'a, U: OmFileArrayDataType>(
        &'a self,
        dim_read: &[Range<u64>],
        into_cube_offset: &'a [u64],
        into_cube_dimension: &'a [u64],
    ) -> Result<crate::utils::wrapped_decoder::WrappedDecoder<'a>, OmFilesError>
    where
        Self: Sized,
    {
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
            self.variable(),
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
}

// Blanket implementation of OmArrayVariable for types implementing OmArrayVariableImpl
impl<T: OmArrayVariableImpl> OmArrayVariable for T {
    /// Returns the compression type of the variable
    fn compression(&self) -> crate::core::compression::OmCompressionType {
        unsafe {
            crate::core::compression::OmCompressionType::try_from(
                om_file_format_sys::om_variable_get_compression(self.variable().as_ptr()) as u8,
            )
            .expect("Invalid compression type")
        }
    }

    /// Returns the scale factor of the variable
    fn scale_factor(&self) -> f32 {
        unsafe { om_file_format_sys::om_variable_get_scale_factor(self.variable().as_ptr()) }
    }

    /// Returns the add offset of the variable
    fn add_offset(&self) -> f32 {
        unsafe { om_file_format_sys::om_variable_get_add_offset(self.variable().as_ptr()) }
    }

    /// Returns the dimensions of the variable
    fn get_dimensions(&self) -> &[u64] {
        unsafe {
            let count =
                om_file_format_sys::om_variable_get_dimensions_count(self.variable().as_ptr());
            let dims = om_file_format_sys::om_variable_get_dimensions(self.variable().as_ptr());
            std::slice::from_raw_parts(dims, count as usize)
        }
    }

    /// Returns the chunk dimensions of the variable
    fn get_chunk_dimensions(&self) -> &[u64] {
        unsafe {
            let count =
                om_file_format_sys::om_variable_get_dimensions_count(self.variable().as_ptr());
            let chunks = om_file_format_sys::om_variable_get_chunks(self.variable().as_ptr());
            std::slice::from_raw_parts(chunks, count as usize)
        }
    }
}

pub(crate) trait OmFileReadableImpl<Backend: OmFileReaderBackend>:
    OmFileVariableImpl + OmFileVariable
{
    fn new_from_offset(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError>;

    fn get_child_by_index(&self, index: u32) -> Option<OmFileReader<Backend>> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !unsafe {
            om_variable_get_children(self.variable().as_ptr(), index, 1, &mut offset, &mut size)
        } {
            return None;
        }

        let offset_size = OmOffsetSize::new(offset, size);
        self.new_from_offset(offset_size).ok()
    }

    fn get_child_by_name(&self, name: &str) -> Option<OmFileReader<Backend>> {
        for i in 0..self.number_of_children() {
            let child = self.get_child_by_index(i);
            if let Some(child) = child
                && child.name() == name
            {
                return Some(child);
            }
        }
        None
    }

    #[cfg(feature = "metadata-tree")]
    fn collect_variable_metadata(
        &self,
        mut current_path: Vec<String>,
        result: &mut HashMap<String, OmOffsetSize>,
    ) {
        let name = self.name();

        // TODO: This requires for paths to be unique
        current_path.push(format!("/{}", name));
        // Create hierarchical key
        let path_str = current_path.join("");
        result.insert(path_str, self.offset_size().clone());

        let num_children = self.number_of_children();
        for i in 0..num_children {
            if let Some(child) = self.get_child_by_index(i) {
                child.collect_variable_metadata(current_path.clone(), result);
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
        OmFileReadableImpl::new_from_offset(self, offset_size)
    }
}

pub(crate) trait OmFileAsyncReadableImpl<Backend: OmFileReaderBackendAsync>:
    OmFileVariableImpl + OmFileVariable
{
    async fn new_from_offset(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReaderAsync<Backend>, OmFilesError>;

    async fn get_child_by_index(&self, index: u32) -> Option<OmFileReaderAsync<Backend>> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !unsafe {
            om_variable_get_children(self.variable().as_ptr(), index, 1, &mut offset, &mut size)
        } {
            return None;
        }

        let offset_size = OmOffsetSize::new(offset, size);
        self.new_from_offset(offset_size).await.ok()
    }

    async fn get_child_by_name(&self, name: &str) -> Option<OmFileReaderAsync<Backend>> {
        for i in 0..self.number_of_children() {
            let child = self.get_child_by_index(i).await;
            if let Some(child) = child
                && child.name() == name
            {
                return Some(child);
            }
        }
        None
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
