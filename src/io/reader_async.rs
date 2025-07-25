//! Asynchronous reader implementation for the Open-Meteo file format.
//!
//! This module provides functionality to read Open-Meteo files asynchronously,
//! which is particularly useful for I/O-bound operations like:
//! - Reading large meteorological datasets over the network
//! - Processing high-resolution climate data with concurrent fetching

use crate::backend::backends::OmFileReaderBackendAsync;
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::implement_variable_methods;
use crate::io::reader_utils::process_trailer;
use crate::io::variable::OmVariableContainer;
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

/// Asynchronous reader for Open-Meteo file format.
///
/// `OmFileReaderAsync` provides optimized access to multi-dimensional weather and climate data
/// using asynchronous I/O operations. It supports concurrent fetching of data chunks to minimize
/// I/O latency while maintaining memory efficiency.
///
/// # Features
/// - Concurrent data fetching with configurable parallelism
/// - Compatible with various backend implementations for different storage types
///
/// # Type Parameters
/// - `Backend`: The storage backend that implements `OmFileReaderBackendAsync`
pub struct OmFileReaderAsync<Backend> {
    /// The backend that provides asynchronous data access
    pub backend: Arc<Backend>,
    /// Container for variable metadata and raw data
    variable: OmVariableContainer,
    /// Maximum number of concurrent data fetching operations
    semaphore: Arc<Semaphore>,
}

// implement utility methods for OmFileReaderAsync
implement_variable_methods!(OmFileReaderAsync<Backend>);

impl<Backend: OmFileReaderBackendAsync + Send + Sync + 'static> OmFileReaderAsync<Backend> {
    /// Creates a new asynchronous reader for an Open-Meteo file.
    ///
    /// This method reads the file header and necessary metadata to initialize the reader.
    /// It handles both legacy format and trailer-based formats automatically.
    ///
    /// # Parameters
    /// - `backend`: An asynchronous backend that provides access to the file data
    ///
    /// # Returns
    /// - `Result<Self, OmFilesRsError>`: A new reader instance or an error
    ///
    /// # Errors
    /// - `OmFilesRsError::FileTooSmall`: If the file is smaller than the required header size
    /// - `OmFilesRsError::NotAnOmFile`: If the file doesn't have a valid Open-Meteo format
    pub async fn new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
        let header_size = unsafe { om_header_size() };
        if backend.count_async() < header_size {
            return Err(OmFilesRsError::FileTooSmall);
        }
        let header_data = backend.get_bytes_async(0, header_size as u64).await?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };

        let (variable_data, offset_size) = {
            match header_type {
                OmHeaderType_t::OM_HEADER_LEGACY => (header_data, None),
                OmHeaderType_t::OM_HEADER_READ_TRAILER => unsafe {
                    let file_size = backend.count_async();
                    let trailer_size = om_trailer_size();
                    let trailer_data = backend
                        .get_bytes_async((file_size - trailer_size) as u64, trailer_size as u64)
                        .await?;

                    let offset_size = process_trailer(&trailer_data)?;
                    let variable_data = backend
                        .get_bytes_async(offset_size.offset, offset_size.size)
                        .await?;
                    (variable_data, Some(offset_size))
                },
                OmHeaderType_t::OM_HEADER_INVALID => {
                    return Err(OmFilesRsError::NotAnOmFile);
                }
            }
        };

        Ok(Self {
            backend,
            variable: OmVariableContainer::new(variable_data, offset_size),
            semaphore: Arc::new(Semaphore::new(16)),
        })
    }

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
    /// - `Result<ArrayD<T>, OmFilesRsError>`: The read data as a multi-dimensional array
    pub async fn read<T: OmFileArrayDataType + Clone + Zero + Send + Sync + 'static>(
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
        )
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

            let mut task_handles: Vec<Task<Result<(Vec<u8>, OmRange_t), OmFilesRsError>>> =
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
                            Err(e) => return Err(OmFilesRsError::TaskError(e.to_string())),
                        }
                    }
                    Ok::<_, OmFilesRsError>(())
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
                    .ok_or(OmFilesRsError::ArrayNotContiguous)?;

                std::slice::from_raw_parts_mut(
                    output_slice.as_mut_ptr() as *mut u8,
                    output_slice.len() * std::mem::size_of::<T>(),
                )
            };
            let results: Vec<Result<(), OmFilesRsError>> = chunk_data
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
