//! Writer-related structs for OmFile format.

use crate::OmDataType;
use crate::core::c_defaults::{c_error_string, create_uninit_encoder};
use crate::core::compression::OmCompressionType;
use crate::core::data_types::OmNone;
use crate::errors::OmFilesError;
use crate::traits::OmFileWriterBackend;
use crate::traits::{OmFileArrayDataType, OmFileScalarDataType};
use crate::utils::buffered_writer::OmBufferedWriter;
use crate::variable::OmOffsetSize;
use ndarray::ArrayViewD;
use om_file_format_sys::{
    OmEncoder_t, OmError_t, om_encoder_chunk_buffer_size, om_encoder_compress_chunk,
    om_encoder_compress_lut, om_encoder_compressed_chunk_buffer_size, om_encoder_count_chunks,
    om_encoder_count_chunks_in_array, om_encoder_init, om_encoder_lut_buffer_size, om_header_write,
    om_header_write_size, om_trailer_size, om_trailer_write, om_variable_write_numeric_array,
    om_variable_write_numeric_array_size, om_variable_write_scalar, om_variable_write_scalar_size,
};
use std::borrow::BorrowMut;
use std::marker::PhantomData;
use std::os::raw::c_void;

/// Writer for OmFile format.
pub struct OmFileWriter<Backend: OmFileWriterBackend> {
    buffer: OmBufferedWriter<Backend>,
}

impl<Backend: OmFileWriterBackend> OmFileWriter<Backend> {
    pub fn new(backend: Backend, initial_capacity: u64) -> Self {
        Self {
            buffer: OmBufferedWriter::new(backend, initial_capacity as usize),
        }
    }

    fn write_header_if_required(&mut self) -> Result<(), OmFilesError> {
        if self.buffer.total_bytes_written > 0 {
            return Ok(());
        }
        let size = unsafe { om_header_write_size() };
        self.buffer.reallocate(size as usize)?;
        unsafe {
            om_header_write(self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void);
        }
        self.buffer.increment_write_position(size as usize);
        Ok(())
    }

    pub fn write_scalar<T: OmFileScalarDataType>(
        &mut self,
        value: T,
        name: &str,
        children: &[OmOffsetSize],
    ) -> Result<OmOffsetSize, OmFilesError> {
        self.write_header_if_required()?;

        assert!(name.len() <= u16::MAX as usize);
        assert!(children.len() <= u32::MAX as usize);

        let type_scalar = T::DATA_TYPE_SCALAR.to_c();
        let children_offsets: Vec<u64> = children.iter().map(|c| c.offset).collect();
        let children_sizes: Vec<u64> = children.iter().map(|c| c.size).collect();
        // Align to 8 bytes before writing
        self.buffer.align_to_8_bytes()?;
        let offset = self.buffer.total_bytes_written as u64;

        let size = value.with_raw_bytes(|bytes| -> Result<usize, OmFilesError> {
            let size = unsafe {
                om_variable_write_scalar_size(
                    name.len() as u16,
                    children.len() as u32,
                    type_scalar,
                    bytes.len() as u64,
                )
            };

            self.buffer.reallocate(size)?;

            unsafe {
                om_variable_write_scalar(
                    self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                    name.len() as u16,
                    children.len() as u32,
                    children_offsets.as_ptr(),
                    children_sizes.as_ptr(),
                    name.as_ptr() as *const ::std::os::raw::c_char,
                    type_scalar,
                    bytes.as_ptr() as *const c_void,
                    bytes.len(),
                )
            };

            self.buffer.increment_write_position(size);

            Ok(size)
        })?;

        Ok(OmOffsetSize::new(offset, size as u64))
    }

    pub fn write_none(
        &mut self,
        name: &str,
        children: &[OmOffsetSize],
    ) -> Result<OmOffsetSize, OmFilesError> {
        // Use write_scalar with OmNone
        self.write_scalar(OmNone::default(), name, children)
    }

    pub fn prepare_array<'a, T: OmFileArrayDataType>(
        &'a mut self,
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: OmCompressionType,
        scale_factor: f32,
        add_offset: f32,
    ) -> Result<OmFileWriterArray<'a, T, Backend>, OmFilesError> {
        let _ = &self.write_header_if_required()?;

        let array_writer = OmFileWriterArray::new(
            dimensions,
            chunk_dimensions,
            compression,
            T::DATA_TYPE_ARRAY,
            scale_factor,
            add_offset,
            self.buffer.borrow_mut(),
        )?;

        Ok(array_writer)
    }

    pub fn write_array(
        &mut self,
        array: OmFileWriterArrayFinalized,
        name: &str,
        children: &[OmOffsetSize],
    ) -> Result<OmOffsetSize, OmFilesError> {
        self.write_header_if_required()?;

        debug_assert!(name.len() <= u16::MAX as usize);
        debug_assert_eq!(array.dimensions.len(), array.chunks.len());

        let size = unsafe {
            om_variable_write_numeric_array_size(
                name.len() as u16,
                children.len() as u32,
                array.dimensions.len() as u64,
            )
        };
        self.buffer.align_to_8_bytes()?;

        let offset = self.buffer.total_bytes_written as u64;

        self.buffer.reallocate(size)?;

        let children_offsets: Vec<u64> = children.iter().map(|c| c.offset).collect();
        let children_sizes: Vec<u64> = children.iter().map(|c| c.size).collect();
        unsafe {
            om_variable_write_numeric_array(
                self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                name.len() as u16,
                children.len() as u32,
                children_offsets.as_ptr(),
                children_sizes.as_ptr(),
                name.as_ptr() as *const ::std::os::raw::c_char,
                array.data_type.to_c(),
                array.compression.to_c(),
                array.scale_factor,
                array.add_offset,
                array.dimensions.len() as u64,
                array.dimensions.as_ptr(),
                array.chunks.as_ptr(),
                array.lut_size,
                array.lut_offset,
            )
        };

        self.buffer.increment_write_position(size);
        Ok(OmOffsetSize::new(offset, size as u64))
    }

    pub fn write_trailer(&mut self, root_variable: OmOffsetSize) -> Result<(), OmFilesError> {
        self.write_header_if_required()?;
        self.buffer.align_to_8_bytes()?;

        let size = unsafe { om_trailer_size() };
        self.buffer.reallocate(size)?;
        unsafe {
            om_trailer_write(
                self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                root_variable.offset,
                root_variable.size,
            );
        }
        self.buffer.increment_write_position(size);

        self.buffer.write_to_file()
    }
}

/// The OmFileWriterArray allows streaming large nd-arrays to an OmFile.
///
/// After writing the complete array to the file, you need to call the [`finalize()`](Self::finalize)
/// method to write the lookup table.
/// The [`OmFileWriterArrayFinalized`] then needs to be referenced in the final OmFile
/// as root variable or as child variable of another variable.
pub struct OmFileWriterArray<'a, OmType: OmFileArrayDataType, Backend: OmFileWriterBackend> {
    look_up_table: Vec<u64>,
    encoder: OmEncoder_t,
    chunk_index: u64,
    scale_factor: f32,
    add_offset: f32,
    compression: OmCompressionType,
    data_type: PhantomData<OmType>,
    dimensions: Vec<u64>,
    chunks: Vec<u64>,
    compressed_chunk_buffer_size: u64,
    chunk_buffer: Vec<u8>,
    buffer: &'a mut OmBufferedWriter<Backend>,
}

impl<'a, OmType: OmFileArrayDataType, Backend: OmFileWriterBackend>
    OmFileWriterArray<'a, OmType, Backend>
{
    pub fn new(
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: OmCompressionType,
        data_type: OmDataType,
        scale_factor: f32,
        add_offset: f32,
        buffer: &'a mut OmBufferedWriter<Backend>,
    ) -> Result<Self, OmFilesError> {
        if data_type != OmType::DATA_TYPE_ARRAY {
            return Err(OmFilesError::InvalidDataType);
        }
        if dimensions.len() != chunk_dimensions.len() {
            return Err(OmFilesError::MismatchingCubeDimensionLength);
        }

        let chunks = chunk_dimensions;

        let mut encoder = unsafe { create_uninit_encoder() };
        let error = unsafe {
            om_encoder_init(
                &mut encoder,
                scale_factor,
                add_offset,
                compression.to_c(),
                data_type.to_c(),
                dimensions.as_ptr(),
                chunks.as_ptr(),
                dimensions.len() as u64,
            )
        };
        if error != OmError_t::ERROR_OK {
            return Err(OmFilesError::FileWriterError {
                errno: error as i32,
                error: c_error_string(error),
            });
        }

        let n_chunks = unsafe { om_encoder_count_chunks(&encoder) } as usize;
        let compressed_chunk_buffer_size =
            unsafe { om_encoder_compressed_chunk_buffer_size(&encoder) };
        let chunk_buffer_size = unsafe { om_encoder_chunk_buffer_size(&encoder) } as usize;

        let chunk_buffer = vec![0u8; chunk_buffer_size];
        let look_up_table = vec![0u64; n_chunks + 1];

        Ok(Self {
            look_up_table,
            encoder,
            chunk_index: 0,
            scale_factor,
            add_offset,
            compression,
            data_type: PhantomData,
            dimensions,
            chunks,
            compressed_chunk_buffer_size,
            chunk_buffer,
            buffer,
        })
    }

    /// Writes an ndarray to the file.
    pub fn write_data(
        &mut self,
        array: ArrayViewD<OmType>,
        array_offset: Option<&[u64]>,
        array_count: Option<&[u64]>,
    ) -> Result<(), OmFilesError> {
        let array_dimensions = array
            .shape()
            .iter()
            .map(|&x| x as u64)
            .collect::<Vec<u64>>();
        let array = array.as_slice().ok_or(OmFilesError::ArrayNotContiguous)?;
        self.write_data_flat(array, Some(&array_dimensions), array_offset, array_count)
    }

    /// Compresses data and writes it to file.
    fn write_data_flat(
        &mut self,
        array: &[OmType],
        array_dimensions: Option<&[u64]>,
        array_offset: Option<&[u64]>,
        array_count: Option<&[u64]>,
    ) -> Result<(), OmFilesError> {
        let array_dimensions = array_dimensions.unwrap_or(&self.dimensions);
        let default_offset = vec![0; array_dimensions.len()];
        let array_offset = array_offset.unwrap_or(default_offset.as_slice());
        let array_count = array_count.unwrap_or(array_dimensions);

        if array_count.len() != self.dimensions.len() {
            return Err(OmFilesError::ChunkHasWrongNumberOfElements);
        }
        for (array_dim, max_dim) in array_count.iter().zip(self.dimensions.iter()) {
            if array_dim > max_dim {
                return Err(OmFilesError::ChunkHasWrongNumberOfElements);
            }
        }

        let array_size: u64 = array_dimensions.iter().product::<u64>();
        if array.len() as u64 != array_size {
            return Err(OmFilesError::ChunkHasWrongNumberOfElements);
        }
        for (dim, (offset, count)) in array_dimensions
            .iter()
            .zip(array_offset.iter().zip(array_count.iter()))
        {
            if offset + count > *dim {
                return Err(OmFilesError::OffsetAndCountExceedDimension {
                    offset: *offset,
                    count: *count,
                    dimension: *dim,
                });
            }
        }

        self.buffer
            .reallocate(self.compressed_chunk_buffer_size as usize * 4)?;

        let number_of_chunks_in_array =
            unsafe { om_encoder_count_chunks_in_array(&mut self.encoder, array_count.as_ptr()) };

        if self.chunk_index == 0 {
            self.look_up_table[self.chunk_index as usize] = self.buffer.total_bytes_written as u64;
        }

        // This loop could be parallelized. However, the order of chunks must
        // remain the same in the LUT and final output buffer.
        // For multithreading, we would need multiple buffers that need to be
        // copied into the final buffer in the correct order after compression.
        for chunk_offset in 0..number_of_chunks_in_array {
            self.buffer
                .reallocate(self.compressed_chunk_buffer_size as usize)?;

            let bytes_written = unsafe {
                om_encoder_compress_chunk(
                    &mut self.encoder,
                    array.as_ptr() as *const c_void,
                    array_dimensions.as_ptr(),
                    array_offset.as_ptr(),
                    array_count.as_ptr(),
                    self.chunk_index,
                    chunk_offset,
                    self.buffer.buffer_at_write_position().as_mut_ptr(),
                    self.chunk_buffer.as_mut_ptr(),
                )
            };

            self.buffer.increment_write_position(bytes_written as usize);

            self.look_up_table[(self.chunk_index + 1) as usize] =
                self.buffer.total_bytes_written as u64;
            self.chunk_index += 1;
        }

        Ok(())
    }

    /// Compress the lookup table and write it to the output buffer.
    fn write_lut(&mut self) -> u64 {
        let buffer_size = unsafe {
            om_encoder_lut_buffer_size(self.look_up_table.as_ptr(), self.look_up_table.len() as u64)
        };

        self.buffer
            .reallocate(buffer_size as usize)
            .expect("Failed to reallocate buffer");

        let compressed_lut_size = unsafe {
            om_encoder_compress_lut(
                self.look_up_table.as_ptr(),
                self.look_up_table.len() as u64,
                self.buffer.buffer_at_write_position().as_mut_ptr(),
                buffer_size,
            )
        };

        self.buffer
            .increment_write_position(compressed_lut_size as usize);
        compressed_lut_size
    }

    /// Finalize the array and return the finalized struct.
    pub fn finalize(mut self) -> OmFileWriterArrayFinalized {
        let lut_offset = self.buffer.total_bytes_written as u64;
        let lut_size = self.write_lut();

        OmFileWriterArrayFinalized {
            scale_factor: self.scale_factor,
            add_offset: self.add_offset,
            compression: self.compression,
            data_type: OmType::DATA_TYPE_ARRAY,
            dimensions: self.dimensions.clone(),
            chunks: self.chunks.clone(),
            lut_size,
            lut_offset,
        }
    }
}

/// An array variable that has already been written to the file.
///
/// The [`OmFileWriterArrayFinalized`] struct contains information about the array variable,
/// by which it can later be identified in the file.
pub struct OmFileWriterArrayFinalized {
    scale_factor: f32,
    add_offset: f32,
    compression: OmCompressionType,
    data_type: OmDataType,
    dimensions: Vec<u64>,
    chunks: Vec<u64>,
    lut_size: u64,
    lut_offset: u64,
}

#[cfg(test)]
mod tests {
    use std::{ptr, slice, sync::Arc};

    use om_file_format_sys::{
        om_variable_get_children_count, om_variable_get_scalar, om_variable_get_type,
        om_variable_init,
    };

    use crate::{
        backends::memory::InMemoryBackend,
        reader::OmFileReader,
        traits::{OmFileReadable, OmFileVariable, OmScalarVariable},
    };

    use super::*;

    #[test]
    fn test_variable() {
        let name = "name";

        // Calculate size needed for scalar variable
        let size_scalar = unsafe {
            om_variable_write_scalar_size(name.len() as u16, 0, OmDataType::Int8.to_c(), 0)
        };

        assert_eq!(size_scalar, 13);

        // Create buffer for the variable
        let mut data = vec![255u8; size_scalar];
        let value: u8 = 177;

        // Write the scalar variable
        unsafe {
            om_variable_write_scalar(
                data.as_mut_ptr() as *mut std::os::raw::c_void,
                name.len() as u16,
                0,
                ptr::null(),
                ptr::null(),
                name.as_ptr() as *const ::std::os::raw::c_char,
                OmDataType::Int8.to_c(),
                &value as *const u8 as *const std::os::raw::c_void,
                0,
            );
        }

        assert_eq!(data, [1, 4, 4, 0, 0, 0, 0, 0, 177, 110, 97, 109, 101]);

        // Initialize a variable from the data
        let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

        // Verify the variable type and children count
        unsafe {
            assert_eq!(om_variable_get_type(om_variable), OmDataType::Int8.to_c());
            assert_eq!(om_variable_get_children_count(om_variable), 0);
        }

        // Get the scalar value
        let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
        let mut size: u64 = 0;

        let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

        // Verify successful retrieval and the value
        assert_eq!(error, OmError_t::ERROR_OK);
        assert!(!ptr.is_null());

        let result_value = unsafe { *(ptr as *const u8) };
        assert_eq!(result_value, value);
    }

    #[test]
    fn test_variable_string() {
        let name = "name";
        let value = "Hello, World!";

        // Calculate size for string scalar
        let size_scalar = unsafe {
            om_variable_write_scalar_size(
                name.len() as u16,
                0,
                OmDataType::String.to_c(),
                value.len() as u64,
            )
        };

        assert_eq!(size_scalar, 33);

        // Create buffer for the variable
        let mut data = vec![255u8; size_scalar];

        // Write the string scalar
        unsafe {
            om_variable_write_scalar(
                data.as_mut_ptr() as *mut std::os::raw::c_void,
                name.len() as u16,
                0,
                ptr::null(),
                ptr::null(),
                name.as_ptr() as *const ::std::os::raw::c_char,
                OmDataType::String.to_c(),
                value.as_ptr() as *const std::os::raw::c_void,
                value.len(),
            );
        }

        // Verify the written data
        let expected = [
            11, // OmDataType_t: 11 = DATA_TYPE_STRING
            4,  // OmCompression_t: 4 = COMPRESSION_NONE
            4, 0, // Size of name
            0, 0, 0, 0, // Children count
            13, 0, 0, 0, 0, 0, 0, 0, // stringSize
            72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33, // "Hello, World!"
            110, 97, 109, 101, // "name"
        ];

        assert_eq!(data, expected);

        // Initialize a variable from the data
        let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

        // Verify the variable type and children count
        unsafe {
            assert_eq!(om_variable_get_type(om_variable), OmDataType::String.to_c());
            assert_eq!(om_variable_get_children_count(om_variable), 0);
        }

        // Get the scalar value
        let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
        let mut size: u64 = 0;

        let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

        // Verify successful retrieval and the value
        assert_eq!(error, OmError_t::ERROR_OK);
        assert!(!ptr.is_null());

        // Convert the raw bytes back to a string
        let string_bytes = unsafe { slice::from_raw_parts(ptr as *const u8, size as usize) };
        let result_string = std::str::from_utf8(string_bytes).unwrap();

        assert_eq!(result_string, value);
    }

    #[test]
    fn test_variable_none() {
        let name = "name";

        // Calculate size for None type scalar
        let size_scalar = unsafe {
            om_variable_write_scalar_size(name.len() as u16, 0, OmDataType::None.to_c(), 0)
        };

        assert_eq!(size_scalar, 12); // 8 (header) + 4 (name length) + 0 (no value)

        // Create buffer for the variable
        let mut data = vec![255u8; size_scalar];

        // Write the non-existing value -> This is essentially creating a Group
        unsafe {
            om_variable_write_scalar(
                data.as_mut_ptr() as *mut std::os::raw::c_void,
                name.len() as u16,
                0,
                ptr::null(),
                ptr::null(),
                name.as_ptr() as *const ::std::os::raw::c_char,
                OmDataType::None.to_c(),
                ptr::null(),
                0,
            );
        }

        // Verify the written data
        assert_eq!(data, [0, 4, 4, 0, 0, 0, 0, 0, 110, 97, 109, 101]);

        // Initialize a variable from the data
        let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

        // Verify the variable type and children count
        unsafe {
            assert_eq!(om_variable_get_type(om_variable), OmDataType::None.to_c());
            assert_eq!(om_variable_get_children_count(om_variable), 0);
        }

        // Try to get scalar value from None type (should fail)
        let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
        let mut size: u64 = 0;

        let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

        // Verify that retrieval fails with the expected error
        assert_eq!(error, OmError_t::ERROR_INVALID_DATA_TYPE);
    }

    #[test]
    fn test_none_variable_as_group() -> Result<(), Box<dyn std::error::Error>> {
        let mut in_memory_backend = InMemoryBackend::new(vec![]);
        let mut file_writer = OmFileWriter::new(in_memory_backend.borrow_mut(), 8);

        // Write a regular variable
        let int_var = file_writer.write_scalar(42i32, "attribute", &[])?;
        // Write a None type to indicate some type of group
        let group_var = file_writer.write_none("group", &[int_var])?;

        file_writer.write_trailer(group_var)?;
        drop(file_writer);

        // Read the file
        let read = OmFileReader::new(Arc::new(in_memory_backend))?;

        // Verify the group variable
        assert_eq!(read.name(), "group");
        assert_eq!(read.data_type(), OmDataType::None);

        // Get the child variable, which is an attribute
        let child = read.get_child(0).unwrap();
        assert_eq!(child.name(), "attribute");
        assert_eq!(child.data_type(), OmDataType::Int32);
        assert_eq!(child.expect_scalar()?.read_scalar::<i32>().unwrap(), 42);

        Ok(())
    }
}
