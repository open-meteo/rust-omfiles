use crate::backend::backends::OmFileReaderBackend;
use crate::backend::mmapfile::{MmapFile, Mode};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::implement_variable_methods;
use crate::io::reader_utils::process_trailer;
use crate::io::tree_navigator::{MetadataCache, TreeNavigator, TreeNavigatorInternal};
use crate::io::variable::OmVariableContainer;
use crate::io::writer::OmOffsetSize;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{
    OmHeaderType_t, om_header_size, om_header_type, om_trailer_size, om_variable_get_children,
};
use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::os::raw::c_void;
use std::sync::Arc;

#[derive(Clone)]
pub struct OmFileReader<Backend> {
    /// The backend that provides data via the get_bytes method
    pub backend: Arc<Backend>,
    /// The variable containing metadata and access methods
    pub variable: OmVariableContainer,
    metadata_cache: MetadataCache<OmOffsetSize>,
}

// implement utility methods for OmFileReader
implement_variable_methods!(OmFileReader<Backend>);

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
        let header_size = unsafe { om_header_size() };
        if backend.count() < header_size {
            return Err(OmFilesRsError::FileTooSmall);
        }
        let header_data = backend.get_bytes_with_fallback(0, header_size as u64)?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };

        let (variable_data, offset_size) = {
            match header_type {
                OmHeaderType_t::OM_HEADER_LEGACY => (header_data.into_owned(), None),
                OmHeaderType_t::OM_HEADER_READ_TRAILER => {
                    let file_size = backend.count();
                    let trailer_size = unsafe { om_trailer_size() };
                    let trailer_data = backend.get_bytes_with_fallback(
                        (file_size - trailer_size) as u64,
                        trailer_size as u64,
                    )?;

                    let offset_size = unsafe { process_trailer(&trailer_data) }?;
                    let variable_data = backend
                        .get_bytes_with_fallback(offset_size.offset, offset_size.size)?
                        .into_owned();
                    (variable_data, Some(offset_size))
                }
                OmHeaderType_t::OM_HEADER_INVALID => {
                    return Err(OmFilesRsError::NotAnOmFile);
                }
            }
        };

        Ok(Self {
            backend,
            variable: OmVariableContainer::new(variable_data, offset_size),
            metadata_cache: MetadataCache::default(),
        })
    }

    /// Returns a HashMap mapping variable names to their offset and size
    /// This function needs to traverse the entire variable tree, therefore
    /// it is best to make sure that variable metadata is close to each other
    /// at the end of the file (before the trailer). The caller could then
    /// make sure that this part of the file is loaded/cached in memory
    pub fn get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize> {
        let mut result = HashMap::new();
        self.collect_variable_metadata(Vec::new(), &mut result);
        result
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
            if let Some(offset_size) = &self.variable.offset_size {
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

    pub fn get_child(&self, index: u32) -> Option<Self> {
        if let Some(offset_size) = self.get_child_metadata(index) {
            let child = self
                .init_child_from_offset_size(offset_size)
                .expect("Failed to init child");
            Some(child)
        } else {
            None
        }
    }

    pub fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<Self, OmFilesRsError> {
        let child_variable = self
            .backend
            .get_bytes_with_fallback(offset_size.offset, offset_size.size)?
            .into_owned();

        Ok(Self {
            backend: self.backend.clone(),
            variable: OmVariableContainer::new(child_variable, Some(offset_size)),
            metadata_cache: MetadataCache::default(),
        })
    }

    /// Read a variable as an array of a dynamic data type.
    pub fn read_into<T: OmFileArrayDataType>(
        &self,
        into: &mut ArrayD<T>,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<(), OmFilesRsError> {
        let decoder = self.prepare_read_parameters::<T>(
            dim_read,
            into_cube_offset,
            into_cube_dimension,
            io_size_max,
            io_size_merge,
        )?;

        // Allocate chunk buffer
        let mut chunk_buffer = Vec::<u8>::with_capacity(decoder.buffer_size() as usize);

        // Perform decoding
        self.backend
            .decode(&decoder.decoder, into, chunk_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn read<T: OmFileArrayDataType + Clone + Zero>(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<ArrayD<T>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into::<T>(
            &mut out,
            dim_read,
            &vec![0; dim_read.len()],
            &out_dims,
            io_size_max,
            io_size_merge,
        )?;

        Ok(out)
    }
}

impl<Backend: OmFileReaderBackend> TreeNavigator for OmFileReader<Backend> {}

impl<Backend: OmFileReaderBackend> TreeNavigatorInternal for OmFileReader<Backend> {
    type ChildMetadata = OmOffsetSize;
    type Cache = MetadataCache<OmOffsetSize>;

    fn get_name(&self) -> Option<String> {
        self.get_name()
    }

    fn number_of_children(&self) -> u32 {
        self.number_of_children()
    }

    fn get_child_metadata(&self, index: u32) -> Option<Self::ChildMetadata> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if unsafe {
            om_variable_get_children(*self.variable.variable, index, 1, &mut offset, &mut size)
        } {
            Some(OmOffsetSize::new(offset, size))
        } else {
            None
        }
    }

    fn create_child_from_metadata(&self, metadata: &Self::ChildMetadata) -> Option<Self> {
        self.init_child_from_offset_size(metadata.clone()).ok()
    }

    fn cache(&self) -> &Self::Cache {
        &self.metadata_cache
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
        // TODO: Error handling
        let mmap = MmapFile::new(file_handle, Mode::ReadOnly).unwrap();
        Self::new(Arc::new(mmap))
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}
