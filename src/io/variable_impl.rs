/// Macro that implements all variable reader methods on the provided type
#[macro_export]
macro_rules! implement_variable_methods {
    ($type:ident < $generic:ident >) => {
        impl<$generic> $type<$generic> {
            /// Returns the data type of the variable
            pub fn data_type(&self) -> $crate::core::data_types::DataType {
                unsafe {
                    $crate::core::data_types::DataType::try_from(
                        om_file_format_sys::om_variable_get_type(*self.variable.variable) as u8,
                    )
                    .expect("Invalid data type")
                }
            }

            /// Returns the compression type of the variable
            pub fn compression(&self) -> $crate::core::compression::CompressionType {
                unsafe {
                    $crate::core::compression::CompressionType::try_from(
                        om_file_format_sys::om_variable_get_compression(*self.variable.variable)
                            as u8,
                    )
                    .expect("Invalid compression type")
                }
            }

            /// Returns the scale factor of the variable
            pub fn scale_factor(&self) -> f32 {
                unsafe { om_file_format_sys::om_variable_get_scale_factor(*self.variable.variable) }
            }

            /// Returns the add offset of the variable
            pub fn add_offset(&self) -> f32 {
                unsafe { om_file_format_sys::om_variable_get_add_offset(*self.variable.variable) }
            }

            /// Returns the dimensions of the variable
            pub fn get_dimensions(&self) -> &[u64] {
                unsafe {
                    let dims =
                        om_file_format_sys::om_variable_get_dimensions(*self.variable.variable);
                    std::slice::from_raw_parts(dims.values, dims.count as usize)
                }
            }

            /// Returns the chunk dimensions of the variable
            pub fn get_chunk_dimensions(&self) -> &[u64] {
                unsafe {
                    let chunks =
                        om_file_format_sys::om_variable_get_chunks(*self.variable.variable);
                    std::slice::from_raw_parts(chunks.values, chunks.count as usize)
                }
            }

            /// Returns the name of the variable, if available
            pub fn get_name(&self) -> Option<String> {
                unsafe {
                    let name = om_file_format_sys::om_variable_get_name(*self.variable.variable);
                    if name.size == 0 {
                        return None;
                    }
                    let bytes =
                        std::slice::from_raw_parts(name.value as *const u8, name.size as usize);
                    String::from_utf8(bytes.to_vec()).ok()
                }
            }

            /// Returns the number of children of the variable
            pub fn number_of_children(&self) -> u32 {
                unsafe {
                    om_file_format_sys::om_variable_get_children_count(*self.variable.variable)
                }
            }

            /// Read a scalar value of the specified type
            pub fn read_scalar<T: crate::core::data_types::OmFileScalarDataType>(
                &self,
            ) -> Option<T> {
                if T::DATA_TYPE_SCALAR != self.data_type() {
                    return None;
                }

                let mut ptr: *mut std::os::raw::c_void = std::ptr::null_mut();
                let mut size: u64 = 0;

                let error = unsafe {
                    om_file_format_sys::om_variable_get_scalar(
                        *self.variable.variable,
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

            /// Prepare common parameters for reading data
            fn prepare_read_parameters<T: OmFileArrayDataType>(
                &self,
                dim_read: &[Range<u64>],
                into_cube_offset: &[u64],
                into_cube_dimension: &[u64],
                io_size_max: Option<u64>,
                io_size_merge: Option<u64>,
            ) -> Result<crate::io::wrapped_decoder::WrappedDecoder, OmFilesRsError> {
                let io_size_max = io_size_max.unwrap_or(65536);
                let io_size_merge = io_size_merge.unwrap_or(512);

                // Verify data type
                if T::DATA_TYPE_ARRAY != self.data_type() {
                    return Err(OmFilesRsError::InvalidDataType);
                }

                let n_dimensions_read = dim_read.len();
                let n_dims = self.get_dimensions().len();

                // Validate dimension counts
                if n_dims != n_dimensions_read
                    || n_dimensions_read != into_cube_offset.len()
                    || n_dimensions_read != into_cube_dimension.len()
                {
                    return Err(OmFilesRsError::MismatchingCubeDimensionLength);
                }

                // Prepare read parameters
                let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
                let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

                // Initialize decoder
                let decoder = crate::io::wrapped_decoder::WrappedDecoder::new(
                    self.variable.variable,
                    n_dimensions_read as u64,
                    read_offset,
                    read_count,
                    into_cube_offset,
                    into_cube_dimension,
                    io_size_merge,
                    io_size_max,
                )?;

                Ok(decoder)
            }
        }
    };
}
