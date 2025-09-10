//! Async reader related structs for OmFiles.

use crate::errors::OmFilesError;
use crate::reader::OmFileScalar;
use crate::traits::OmFileArrayDataType;
use crate::traits::{
    OmArrayVariable, OmArrayVariableImpl, OmFileReaderBackendAsync, OmFileVariable,
    OmFileVariableImpl,
};
use crate::utils::reader_utils::process_trailer;
use crate::variable::OmVariableContainer;
use async_executor::{Executor, Task};
use async_lock::Semaphore;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{
    OmHeaderType_t, OmRange_t, om_header_size, om_header_type, om_trailer_size,
};
use std::ffi::c_void;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

/// Global executor for handling asynchronous tasks
static EXECUTOR: OnceLock<Executor> = OnceLock::new();
fn get_executor() -> &'static Executor<'static> {
    EXECUTOR.get_or_init(|| Executor::new())
}

/// Represents any variable in an OmFile and allows access to it via an async backend.
pub struct OmFileReaderAsync<Backend> {
    /// The backend that provides asynchronous data access
    backend: Arc<Backend>,
    /// Container for variable metadata and raw data
    variable: OmVariableContainer,
}

impl<Backend: OmFileReaderBackendAsync> OmFileVariableImpl for OmFileReaderAsync<Backend> {
    fn variable(&self) -> &OmVariableContainer {
        &self.variable
    }
}

impl<Backend: OmFileReaderBackendAsync + Send + Sync + 'static> OmFileReaderAsync<Backend> {
    /// Creates a new asynchronous reader for an Open-Meteo file.
    ///
    /// This method tries to initialize from the file trailer and falls back to the legacy format if necessary.
    ///
    /// # Parameters
    /// - `backend`: An asynchronous backend that provides access to the file data
    ///
    /// # Returns
    /// - `Result<Self, OmFilesError>`: A new reader instance or an error
    ///
    /// # Errors
    /// - `OmFilesError::FileTooSmall`: If the file is smaller than the required header size
    /// - `OmFilesError::NotAnOmFile`: If the file doesn't have a valid Open-Meteo format
    pub async fn new(backend: Arc<Backend>) -> Result<Self, OmFilesError> {
        let file_size = backend.count_async();
        let trailer_size = unsafe { om_trailer_size() };

        // Try v3 (trailer-based) format first
        if file_size >= trailer_size {
            let trailer_data = backend
                .get_bytes_async((file_size - trailer_size) as u64, trailer_size as u64)
                .await?;
            match unsafe { process_trailer(&trailer_data) } {
                Ok(offset_size) => {
                    let variable_data = backend
                        .get_bytes_async(offset_size.offset, offset_size.size)
                        .await?
                        .to_vec();
                    return Ok(Self {
                        backend,
                        variable: OmVariableContainer::new(variable_data, Some(offset_size)),
                    });
                }
                Err(OmFilesError::NotAnOmFile) => {
                    // fall through to legacy check
                }
                Err(e) => return Err(e),
            }
        }

        // Fallback: Try v2 (legacy) format
        let header_size = unsafe { om_header_size() };
        if file_size < header_size {
            return Err(OmFilesError::FileTooSmall);
        }
        let header_data = backend.get_bytes_async(0, header_size as u64).await?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };
        if header_type != OmHeaderType_t::OM_HEADER_LEGACY {
            return Err(OmFilesError::NotAnOmFile);
        }

        Ok(Self {
            backend,
            variable: OmVariableContainer::new(header_data.to_vec(), None),
        })
    }

    pub fn expect_scalar(&self) -> Result<OmFileScalar<Backend>, OmFilesError> {
        if !self.data_type().is_scalar() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileScalar::new(&self.backend, &self.variable))
    }

    pub fn expect_array(&self) -> Result<OmFileAsyncArray<Backend>, OmFilesError> {
        self.expect_array_with_io_sizes(65536, 512)
    }

    pub fn expect_array_with_io_sizes(
        &self,
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<OmFileAsyncArray<Backend>, OmFilesError> {
        if !self.data_type().is_array() {
            return Err(OmFilesError::InvalidDataType);
        }
        Ok(OmFileAsyncArray {
            backend: &self.backend,
            variable: &self.variable,
            semaphore: Arc::new(Semaphore::new(16)),
            io_size_max,
            io_size_merge,
        })
    }
}

/// Represents an array variable in an OmFile and allows access to it via an async backend.
pub struct OmFileAsyncArray<'a, Backend> {
    /// The backend that provides asynchronous data access
    backend: &'a Arc<Backend>,
    /// Container for variable metadata and raw data
    variable: &'a OmVariableContainer,
    /// Maximum number of concurrent data fetching operations
    semaphore: Arc<Semaphore>,

    io_size_max: u64,
    io_size_merge: u64,
}

impl<'a, Backend: OmFileReaderBackendAsync> OmFileVariableImpl for OmFileAsyncArray<'a, Backend> {
    fn variable(&self) -> &OmVariableContainer {
        self.variable
    }
}

impl<'a, Backend: OmFileReaderBackendAsync> OmArrayVariableImpl for OmFileAsyncArray<'a, Backend> {
    fn io_size_max(&self) -> u64 {
        self.io_size_max
    }

    fn io_size_merge(&self) -> u64 {
        self.io_size_merge
    }
}

impl<'a, Backend: OmFileReaderBackendAsync + Send + Sync + 'static> OmFileAsyncArray<'a, Backend> {
    /// Sets the maximum number of concurrent fetch operations.
    /// # Parameters
    /// - `max_concurrency`: The maximum number of concurrent operations (must be > 0)
    pub fn set_max_concurrency(&mut self, max_concurrency: NonZeroUsize) {
        self.semaphore = Arc::new(Semaphore::new(max_concurrency.get()));
    }

    /// Reads a multi-dimensional array from the file asynchronously.
    ///
    /// This method optimizes I/O by fetching data chunks concurrently, making it
    /// especially efficient for remote or high-latency storage systems.
    ///
    /// # Type Parameters
    /// - `T`: The data type to read into (e.g., f32, i16)
    ///
    /// # Parameters
    /// - `dim_read`: Specifies which region to read as [start..end] ranges for each dimension
    /// - `io_size_max`: Optional maximum size of I/O operations in bytes (default: 65536)
    /// - `io_size_merge`: Optional threshold for merging small I/O operations (default: 512)
    ///
    /// # Returns
    /// - `Result<ArrayD<T>, OmFilesError>`: The read data as a multi-dimensional array
    pub async fn read<T: OmFileArrayDataType + Clone + Zero + Send + Sync + 'static>(
        &self,
        dim_read: &[Range<u64>],
    ) -> Result<ArrayD<T>, OmFilesError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into::<T>(&mut out, dim_read, &vec![0; dim_read.len()], &out_dims)
            .await?;

        Ok(out)
    }

    /// Reads data into an existing array asynchronously.
    ///
    /// This advanced method allows reading data into a specific region of an existing array,
    /// which is useful for tiled processing of large datasets or partial updates.
    ///
    /// # Type Parameters
    /// - `T`: The data type to read (must match the array's data type)
    ///
    /// # Parameters
    /// - `into`: Target array to read the data into
    /// - `dim_read`: Regions to read from the file as [start..end] ranges
    /// - `into_cube_offset`: Start position in the target array for each dimension
    /// - `into_cube_dimension`: Size of the region to fill in the target array
    /// - `io_size_max`: Optional maximum size of I/O operations (default: 65536)
    /// - `io_size_merge`: Optional threshold for merging small I/O operations (default: 512)
    ///
    /// # Performance Notes
    /// - Data is fetched concurrently but decoded sequentially
    /// - The `max_concurrency` setting controls the parallelism level
    /// - For large files with many small chunks, increasing `io_size_merge` may improve performance
    pub async fn read_into<T: OmFileArrayDataType + Send + Sync + 'static>(
        &self,
        into: &mut ArrayD<T>,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
    ) -> Result<(), OmFilesError> {
        let decoder =
            self.prepare_read_parameters::<T>(dim_read, into_cube_offset, into_cube_dimension)?;

        // Process all index blocks
        let mut index_read = decoder.new_index_read();
        while decoder.next_index_read(&mut index_read) {
            // Acquire permit, limiting concurrency
            let _permit = self.semaphore.acquire().await;
            // Fetch index data in a blocking task
            let index_data = self
                .backend
                .get_bytes_async(index_read.offset, index_read.count)
                .await?;
            drop(_permit);

            // Create a collection to store single chunks to process
            let mut chunk_infos = Vec::new();
            // Collect tasks from the callback without spawning them
            decoder.process_data_reads(
                &index_read,
                &index_data,
                |offset, count, chunk_index| {
                    // Collect task parameters for later processing
                    chunk_infos.push((offset, count, chunk_index));
                    Ok(())
                },
            )?;

            let mut task_handles: Vec<Task<Result<(Vec<u8>, OmRange_t), OmFilesError>>> =
                Vec::with_capacity(chunk_infos.len());

            // Spawn a task for each chunk info
            for (offset, count, chunk_index) in chunk_infos {
                let backend = self.backend.clone();
                let semaphore_clone = self.semaphore.clone();

                let task = get_executor().spawn(async move {
                    // Acquire permit limiting concurrency
                    let permit = semaphore_clone.acquire_arc().await;

                    // Fetch data and attach chunk index
                    let data = backend.get_bytes_async(offset, count).await?;
                    let result = Ok((data, chunk_index));

                    // Release permit
                    drop(permit);

                    result
                });
                task_handles.push(task);
            }

            // Run the executor to process all tasks
            let mut chunk_data: Vec<(Vec<u8>, OmRange_t)> = Vec::with_capacity(task_handles.len());
            get_executor()
                .run(async {
                    for handle in task_handles {
                        match handle.await {
                            Ok(result) => chunk_data.push(result),
                            Err(e) => return Err(OmFilesError::TaskError(e.to_string())),
                        }
                    }
                    Ok::<_, OmFilesError>(())
                })
                .await?;

            // Decode all chunks sequentially.
            // This could also potentially be parallelized using a thread pool.
            let mut chunk_buffer = vec![0u8; decoder.buffer_size()];
            // Get access to the output array
            // SAFETY: The decoder is supposed to write into disjoint slices
            // of the output array, so this is not racy!
            let output_bytes = unsafe {
                let output_slice = into
                    .as_slice_mut()
                    .ok_or(OmFilesError::ArrayNotContiguous)?;

                std::slice::from_raw_parts_mut(
                    output_slice.as_mut_ptr() as *mut u8,
                    output_slice.len() * std::mem::size_of::<T>(),
                )
            };
            let results: Vec<Result<(), OmFilesError>> = chunk_data
                .into_iter()
                .map(|(data_bytes, chunk_index)| {
                    decoder.decode_chunk(chunk_index, &data_bytes, output_bytes, &mut chunk_buffer)
                })
                .collect();

            // Check for errors
            for result in results {
                if let Err(e) = result {
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
