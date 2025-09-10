//! Sync reader related structs for OmFiles.

use crate::backends::mmapfile::{FileAccessMode, MmapFile};
use crate::errors::OmFilesError;
use crate::traits::OmFileArrayDataType;
use crate::traits::{
    OmArrayVariable, OmArrayVariableImpl, OmFileReadableImpl, OmFileReaderBackend, OmFileVariable,
    OmFileVariableImpl, OmScalarVariableImpl,
};
use crate::utils::reader_utils::process_trailer;
use crate::variable::OmVariableContainer;
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
    /// The variable containing metadata and access methods
    variable: OmVariableContainer,
}

impl<Backend> OmFileVariableImpl for OmFileReader<Backend> {
    fn variable(&self) -> &OmVariableContainer {
        &self.variable
    }
}

impl<Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileReader<Backend> {
    fn new_with_variable(&self, variable: OmVariableContainer) -> OmFileReader<Backend> {
        Self {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Backend {
        &self.backend
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
                    let variable_data = backend
                        .get_bytes(offset_size.offset, offset_size.size)?
                        .to_vec();

                    return Ok(Self {
                        backend: backend.clone(),
                        variable: OmVariableContainer::new(variable_data, Some(offset_size)),
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

        Ok(Self {
            backend: backend.clone(),
            variable: OmVariableContainer::new(header_data.to_vec(), None),
        })
    }

    pub fn expect_scalar(&self) -> Result<OmFileScalar<Backend>, OmFilesError> {
        if !self.data_type().is_scalar() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileScalar::new(&self.backend, &self.variable))
    }

    pub fn expect_array(&self) -> Result<OmFileArray<Backend>, OmFilesError> {
        self.expect_array_with_io_sizes(65536, 512)
    }

    pub fn expect_array_with_io_sizes(
        &self,
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<OmFileArray<Backend>, OmFilesError> {
        if !self.data_type().is_array() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileArray {
            backend: &self.backend,
            variable: &self.variable,
            io_size_max,
            io_size_merge,
        })
    }
}

/// Represents a scalar variable in an OmFile.
pub struct OmFileScalar<'a, Backend> {
    backend: &'a Arc<Backend>,
    variable: &'a OmVariableContainer,
}

impl<'a, Backend> OmFileScalar<'a, Backend> {
    pub(crate) fn new(backend: &'a Arc<Backend>, variable: &'a OmVariableContainer) -> Self {
        OmFileScalar { backend, variable }
    }
}

impl<'a, Backend> OmFileVariableImpl for OmFileScalar<'a, Backend> {
    fn variable(&self) -> &OmVariableContainer {
        self.variable
    }
}

impl<'a, Backend> OmScalarVariableImpl for OmFileScalar<'a, Backend> {}

impl<'a, Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileScalar<'a, Backend> {
    fn new_with_variable(&self, variable: OmVariableContainer) -> OmFileReader<Backend> {
        OmFileReader {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Backend {
        &self.backend
    }
}

/// Represents an array variable in an OmFile.
pub struct OmFileArray<'a, Backend> {
    /// The backend that provides data via the get_bytes method
    backend: &'a Arc<Backend>,
    /// The variable containing metadata and access methods
    variable: &'a OmVariableContainer,

    io_size_max: u64,
    io_size_merge: u64,
}

impl<'a, Backend> OmFileVariableImpl for OmFileArray<'a, Backend> {
    fn variable(&self) -> &OmVariableContainer {
        self.variable
    }
}

impl<'a, Backend: OmFileReaderBackend> OmFileReadableImpl<Backend> for OmFileArray<'a, Backend> {
    fn new_with_variable(&self, variable: OmVariableContainer) -> OmFileReader<Backend> {
        OmFileReader {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Backend {
        self.backend
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
