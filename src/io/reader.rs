use crate::backends::mmapfile::{MmapFile, Mode};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::io::reader_utils::process_trailer;
use crate::io::variable::OmVariableContainer;
use crate::traits::{
    ArrayOmVariable, GenericOmVariable, OmFileReaderBackend, OmVariableReadable, ScalarOmVariable,
};
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{OmHeaderType_t, om_header_size, om_header_type, om_trailer_size};
use std::fs::File;
use std::ops::Range;
use std::os::raw::c_void;
use std::sync::Arc;

pub struct OmFileReader<Backend> {
    /// The backend that provides data via the get_bytes method
    pub backend: Arc<Backend>,
    /// The variable containing metadata and access methods
    pub variable: OmVariableContainer,
}

impl<Backend: OmFileReaderBackend> GenericOmVariable for OmFileReader<Backend> {
    fn variable(&self) -> &OmVariableContainer {
        &self.variable
    }
}

impl<Backend: OmFileReaderBackend> OmVariableReadable for OmFileReader<Backend> {
    type ChildType = OmFileReader<Backend>;
    type Backend = Backend;

    fn new_with_variable(&self, variable: OmVariableContainer) -> Self::ChildType {
        Self {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Self::Backend {
        &self.backend
    }
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
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
                Err(OmFilesRsError::NotAnOmFile) => {
                    // fall through to v2 format
                }
                Err(e) => return Err(e),
            }
        }

        // Fallback: Try v2 (legacy) format
        let header_size = unsafe { om_header_size() };
        if file_size < header_size {
            return Err(OmFilesRsError::FileTooSmall);
        }
        let header_data = backend.get_bytes(0, header_size as u64)?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };
        if header_type != OmHeaderType_t::OM_HEADER_LEGACY {
            return Err(OmFilesRsError::NotAnOmFile);
        }

        Ok(Self {
            backend: backend.clone(),
            variable: OmVariableContainer::new(header_data.to_vec(), None),
        })
    }

    pub fn expect_scalar(self) -> Result<OmFileReaderScalar<Backend>, OmFilesRsError> {
        if self.data_type().is_array() {
            return Err(OmFilesRsError::InvalidDataType);
        }
        Ok(OmFileReaderScalar {
            backend: self.backend,
            variable: self.variable,
        })
    }

    pub fn expect_array(self) -> Result<OmFileReaderArray<Backend>, OmFilesRsError> {
        self.expect_array_with_io_sizes(65536, 512)
    }

    pub fn expect_array_with_io_sizes(
        self,
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<OmFileReaderArray<Backend>, OmFilesRsError> {
        if !self.data_type().is_array() {
            return Err(OmFilesRsError::InvalidDataType);
        }
        Ok(OmFileReaderArray {
            backend: self.backend,
            variable: self.variable,
            io_size_max,
            io_size_merge,
        })
    }
}

pub struct OmFileReaderScalar<Backend> {
    backend: Arc<Backend>,
    variable: OmVariableContainer,
}

impl<Backend: OmFileReaderBackend> GenericOmVariable for OmFileReaderScalar<Backend> {
    fn variable(&self) -> &OmVariableContainer {
        &self.variable
    }
}

impl<Backend: OmFileReaderBackend> OmVariableReadable for OmFileReaderScalar<Backend> {
    type ChildType = OmFileReader<Backend>;
    type Backend = Backend;

    fn new_with_variable(&self, variable: OmVariableContainer) -> Self::ChildType {
        OmFileReader {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Self::Backend {
        &self.backend
    }
}

impl<Backend: OmFileReaderBackend> ScalarOmVariable for OmFileReaderScalar<Backend> {}

pub struct OmFileReaderArray<Backend> {
    /// The backend that provides data via the get_bytes method
    pub backend: Arc<Backend>,
    /// The variable containing metadata and access methods
    pub variable: OmVariableContainer,

    io_size_max: u64,
    io_size_merge: u64,
}

impl<Backend: OmFileReaderBackend> GenericOmVariable for OmFileReaderArray<Backend> {
    fn variable(&self) -> &OmVariableContainer {
        &self.variable
    }
}

impl<Backend: OmFileReaderBackend> OmVariableReadable for OmFileReaderArray<Backend> {
    type ChildType = OmFileReader<Backend>;
    type Backend = Backend;

    fn new_with_variable(&self, variable: OmVariableContainer) -> Self::ChildType {
        OmFileReader {
            backend: self.backend.clone(),
            variable,
        }
    }

    fn backend(&self) -> &Self::Backend {
        &self.backend
    }
}

impl<Backend: OmFileReaderBackend> ArrayOmVariable for OmFileReaderArray<Backend> {
    fn io_size_max(&self) -> u64 {
        self.io_size_max
    }

    fn io_size_merge(&self) -> u64 {
        self.io_size_merge
    }
}

impl<Backend: OmFileReaderBackend> OmFileReaderArray<Backend> {
    /// Read a variable as an array of a dynamic data type.
    pub fn read_into<T: OmFileArrayDataType>(
        &self,
        into: &mut ArrayD<T>,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<(), OmFilesRsError> {
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
    ) -> Result<ArrayD<T>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into::<T>(&mut out, dim_read, &vec![0; dim_read.len()], &out_dims)?;

        Ok(out)
    }
}

impl OmFileReader<MmapFile> {
    /// Convenience initializer to create an `OmFileReader` from a file path.
    pub fn from_file(file: &str) -> Result<Self, OmFilesRsError> {
        let file_handle = File::open(file).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: file.to_string(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        Self::from_file_handle(file_handle)
    }

    /// Convenience initializer to create an `OmFileReader` from an existing `FileHandle`.
    pub fn from_file_handle(file_handle: File) -> Result<Self, OmFilesRsError> {
        let mmap = MmapFile::new(file_handle, Mode::ReadOnly).map_err(|e| {
            OmFilesRsError::GenericError(format!("Failed to memory map file: {}", e))
        })?;
        Self::new(Arc::new(mmap))
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}
