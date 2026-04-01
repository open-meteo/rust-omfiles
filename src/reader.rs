//! Sync reader related structs for OmFiles.

use crate::OmOffsetSize;
use crate::backends::mmapfile::{FileAccessMode, MmapFile};
use crate::errors::OmFilesError;
use crate::traits::OmFileArrayDataType;
use crate::traits::{
    OmArrayVariable, OmArrayVariableImpl, OmFileReadableImpl, OmFileReaderBackend, OmFileVariable,
    OmFileVariableImpl, OmScalarVariableImpl,
};
use crate::utils::reader_utils::process_trailer;
use crate::variable::OmVariablePtr;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{OmHeaderType_t, om_header_size, om_header_type, om_trailer_size};
use std::fs::File;
use std::ops::Range;
use std::os::raw::c_void;
use std::sync::Arc;

/// Represents any variable in an OmFile.
///
/// Allows traversing the file hierarchy and can be downcast to a scalar or array variable.
/// Therefore, the traits [`OmArrayVariable`], [`OmScalarVariable`](crate::traits::OmScalarVariable), and [`OmFileVariable`]
/// need to be implemented and in scope.
pub struct OmFileReader<Backend> {
    /// The backend that provides data via the get_bytes method
    pub backend: Arc<Backend>,
    /// Direct access to the C variable pointer + safety anchor
    variable: OmVariablePtr,
    /// Metadata location, can be used to re-enter the file hierarchy via the backend
    offset_size: OmOffsetSize,
}

impl<Backend> OmFileVariableImpl for OmFileReader<Backend> {
    fn variable(&self) -> &OmVariablePtr {
        &self.variable
    }
    fn offset_size(&self) -> &OmOffsetSize {
        &self.offset_size
    }
}

impl<Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileReader<Backend> {
    fn new_from_offset(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        let variable = create_variable_from_offset(&self.backend, &offset_size)?;
        Ok(Self {
            backend: self.backend.clone(),
            variable,
            offset_size,
        })
    }
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Arc<Backend>) -> Result<Self, OmFilesError> {
        // Read v3 files first, if this fails try legacy format
        let file_size = backend.count();
        let trailer_size = unsafe { om_trailer_size() };

        if file_size >= trailer_size {
            let trailer_data =
                backend.get_bytes((file_size - trailer_size) as u64, trailer_size as u64)?;
            match unsafe { process_trailer(&trailer_data) } {
                Ok(offset_size) => {
                    let variable = create_variable_from_offset(&backend, &offset_size)?;

                    return Ok(Self {
                        backend: backend.clone(),
                        variable,
                        offset_size,
                    });
                }
                Err(OmFilesError::NotAnOmFile) => {
                    // fall through to v2 format
                }
                Err(e) => return Err(e),
            }
        }

        // Fallback: Try v2 (legacy) format
        let header_size = unsafe { om_header_size() };
        if file_size < header_size {
            return Err(OmFilesError::FileTooSmall);
        }
        let header_data = backend.get_bytes(0, header_size as u64)?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };
        if header_type != OmHeaderType_t::OM_HEADER_LEGACY {
            return Err(OmFilesError::NotAnOmFile);
        }
        let header_vec: Vec<u8> = header_data.to_vec();

        Ok(Self {
            backend: backend.clone(),
            variable: OmVariablePtr::new(header_vec),
            offset_size: OmOffsetSize {
                offset: 0,
                size: header_size as u64,
            },
        })
    }

    pub fn expect_scalar<'a>(&'a self) -> Result<OmFileScalar<'a, Backend>, OmFilesError> {
        if !self.data_type().is_scalar() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileScalar::new(
            &self.backend,
            &self.variable,
            &self.offset_size,
        ))
    }

    pub fn expect_array<'a>(&'a self) -> Result<OmFileArray<'a, Backend>, OmFilesError> {
        self.expect_array_with_io_sizes(65536, 512)
    }

    pub fn expect_array_with_io_sizes<'a>(
        &'a self,
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<OmFileArray<'a, Backend>, OmFilesError> {
        if !self.data_type().is_array() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileArray {
            backend: &self.backend,
            variable: &self.variable,
            offset_size: &self.offset_size,
            io_size_max,
            io_size_merge,
        })
    }
}

/// Represents a scalar variable in an OmFile.
pub struct OmFileScalar<'a, Backend> {
    backend: &'a Arc<Backend>,
    variable: &'a OmVariablePtr,
    offset_size: &'a OmOffsetSize,
}

impl<'a, Backend> OmFileScalar<'a, Backend> {
    pub(crate) fn new(
        backend: &'a Arc<Backend>,
        variable: &'a OmVariablePtr,
        offset_size: &'a OmOffsetSize,
    ) -> Self {
        OmFileScalar {
            backend,
            variable,
            offset_size,
        }
    }
}

impl<'a, Backend> OmFileVariableImpl for OmFileScalar<'a, Backend> {
    fn variable(&self) -> &OmVariablePtr {
        self.variable
    }
    fn offset_size(&self) -> &OmOffsetSize {
        &self.offset_size
    }
}

impl<'a, Backend> OmScalarVariableImpl for OmFileScalar<'a, Backend> {}

impl<'a, Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileScalar<'a, Backend> {
    fn new_from_offset(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        let variable = create_variable_from_offset(&self.backend, &offset_size)?;
        Ok(OmFileReader {
            backend: self.backend.clone(),
            variable,
            offset_size,
        })
    }
}

/// Represents an array variable in an OmFile.
pub struct OmFileArray<'a, Backend> {
    /// The backend that provides data via the get_bytes method
    backend: &'a Arc<Backend>,
    /// The variable containing metadata and access methods
    variable: &'a OmVariablePtr,
    offset_size: &'a OmOffsetSize,

    io_size_max: u64,
    io_size_merge: u64,
}

impl<'a, Backend> OmFileVariableImpl for OmFileArray<'a, Backend> {
    fn variable(&self) -> &OmVariablePtr {
        self.variable
    }
    fn offset_size(&self) -> &OmOffsetSize {
        self.offset_size
    }
}

impl<'a, Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileArray<'a, Backend> {
    fn new_from_offset(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<OmFileReader<Backend>, OmFilesError> {
        let variable = create_variable_from_offset(&self.backend, &offset_size)?;
        Ok(OmFileReader {
            backend: self.backend.clone(),
            variable,
            offset_size,
        })
    }
}

impl<'a, Backend> OmArrayVariableImpl for OmFileArray<'a, Backend> {
    fn io_size_max(&self) -> u64 {
        self.io_size_max
    }

    fn io_size_merge(&self) -> u64 {
        self.io_size_merge
    }
}

impl<'a, Backend: OmFileReaderBackend> OmFileArray<'a, Backend> {
    /// Read a variable as an array of a dynamic data type.
    pub fn read_into<T: OmFileArrayDataType>(
        &self,
        into: &mut ArrayD<T>,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<(), OmFilesError> {
        let decoder =
            self.prepare_read_parameters::<T>(dim_read, into_cube_offset, into_cube_dimension)?;

        let mut chunk_buffer = Vec::<u8>::with_capacity(decoder.buffer_size() as usize);
        self.backend
            .decode(&decoder.decoder, into, chunk_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn read<T: OmFileArrayDataType + Clone + Zero>(
        &self,
        dim_read: &[Range<u64>],
    ) -> Result<ArrayD<T>, OmFilesError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into::<T>(&mut out, dim_read, &vec![0; dim_read.len()], &out_dims)?;

        Ok(out)
    }

    /// Retrieve the complete chunk lookup table (LUT) as a vector of i64 offsets
    pub fn get_complete_lut(&self) -> Result<Vec<u64>, OmFilesError> {
        // Get necessary info for decoder
        let dimensions = self.get_dimensions();
        let n_dims = dimensions.len();

        // Create full read ranges (read the entire variable)
        let read_offset = vec![0u64; n_dims];
        let read_count = dimensions.to_vec();
        let read_ranges = read_offset
            .iter()
            .zip(read_count.iter())
            .map(|(&offset, &count)| offset..(offset + count))
            .collect::<Vec<_>>();

        // Initialize the decoder
        let decoder =
            self.prepare_read_parameters::<f32>(&read_ranges, &read_offset, &read_count)?;

        // Calculate the number of chunks total
        let number_of_chunks = decoder.decoder.number_of_chunks;

        // Allocate space for the complete LUT
        // Size is number of chunks + 1 so it is possible to store the end address of each chunk
        // plus the offset to the first chunk
        let mut lut = vec![0u64; number_of_chunks as usize + 1];

        let mut index_read = crate::core::c_defaults::new_index_read(&decoder.decoder);

        // Loop through index ranges to get the complete LUT
        while unsafe {
            om_file_format_sys::om_decoder_next_index_read(&decoder.decoder, &mut index_read)
        } {
            // Calculate the range of chunks in this index read
            let start_chunk = index_read.indexRange.lowerBound;
            let end_chunk = index_read.indexRange.upperBound;
            let chunk_count = end_chunk - start_chunk;

            // Get the index data for this range
            let index_data = self
                .backend
                .get_bytes(index_read.offset, index_read.count)?;

            // Extract the partial LUT for this range
            let lut_ptr = lut[(start_chunk as usize)..].as_mut_ptr();
            let lut_out_size = (number_of_chunks + 1 - start_chunk) as u64;
            let error = unsafe {
                om_file_format_sys::om_decoder_get_partial_lut(
                    &decoder.decoder,
                    index_data.as_ptr() as *const c_void,
                    index_data.len() as u64,
                    lut_ptr,
                    lut_out_size,
                    start_chunk,
                    start_chunk, // index_range_lower_bound is same as start_chunk here
                    chunk_count + 1,
                )
            };

            if error != om_file_format_sys::OmError_t::ERROR_OK {
                return Err(OmFilesError::DecoderError(
                    crate::core::c_defaults::c_error_string(error),
                ));
            }
        }

        // For V1 format, adjust offsets to account for header and LUT
        if decoder.decoder.lut_chunk_length == 0 {
            let header_size = unsafe { om_file_format_sys::om_header_size() } as u64;
            let lut_size = number_of_chunks * std::mem::size_of::<u64>() as u64;
            let total_offset = header_size + lut_size;

            // Skip adjusting the first entry which might be 0 in V1 format
            for i in 1..(number_of_chunks) as usize {
                lut[i] += total_offset;
            }
        }

        Ok(lut)
    }
}

impl OmFileReader<MmapFile> {
    /// Convenience initializer to create an `OmFileReader` from a file path.
    pub fn from_file(file: &str) -> Result<Self, OmFilesError> {
        let file_handle = File::open(file).map_err(|e| OmFilesError::CannotOpenFile {
            filename: file.to_string(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        Self::from_file_handle(file_handle)
    }

    /// Convenience initializer to create an `OmFileReader` from an existing `FileHandle`.
    pub fn from_file_handle(file_handle: File) -> Result<Self, OmFilesError> {
        let mmap = MmapFile::new(file_handle, FileAccessMode::ReadOnly)
            .map_err(|e| OmFilesError::GenericError(format!("Failed to memory map file: {}", e)))?;
        Self::new(Arc::new(mmap))
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}

/// Utility function to create an `OmVariablePtr` from offset and size in the file.
fn create_variable_from_offset<Backend: OmFileReaderBackend>(
    backend: &Arc<Backend>,
    offset_size: &OmOffsetSize,
) -> Result<OmVariablePtr, OmFilesError> {
    let var_data = backend.get_bytes(offset_size.offset, offset_size.size)?;
    let var_vec: Vec<u8> = var_data.to_vec();
    Ok(OmVariablePtr::new(var_vec))
}
