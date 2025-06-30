use crate::{
    backend::io_uring::IoUringBackend, errors::OmFilesRsError, io::reader_async::OmFileReaderAsync,
};
use std::sync::Arc;

impl OmFileReaderAsync<IoUringBackend> {
    /// Creates a new asynchronous reader for an Open-Meteo file using io_uring.
    ///
    /// This method provides a convenient way to create an asynchronous reader using
    /// the high-performance io_uring interface on Linux systems.
    ///
    /// # Parameters
    /// - `file_path`: Path to the Open-Meteo file
    /// - `queue_depth`: Optional queue depth for io_uring operations (default: 32)
    /// - `max_concurrency`: Optional maximum number of concurrent operations (default: 16)
    ///
    /// # Returns
    /// - `Result<Self, OmFilesRsError>`: A new reader instance or an error
    ///
    /// # Errors
    /// - `OmFilesRsError::CannotOpenFile`: If the file cannot be opened
    /// - `OmFilesRsError::FileTooSmall`: If the file is smaller than required
    /// - `OmFilesRsError::NotAnOmFile`: If the file format is invalid
    ///
    /// # Example
    /// ```no_run
    /// use omfiles_rs::io::reader_async::OmFileReaderAsync;
    /// use std::num::NonZeroUsize;
    ///
    /// async fn example() {
    ///     let reader = OmFileReaderAsync::from_file("data.om", None, None)
    ///         .await
    ///         .expect("Failed to open file");
    ///
    ///     // Read data from file
    ///     let data = reader.read::<f32>(&[0..10, 0..100], None, None)
    ///         .await
    ///         .expect("Failed to read data");
    /// }
    /// ```
    pub async fn from_file(
        file_path: &str,
        queue_depth: Option<u32>,
        max_concurrency: Option<std::num::NonZeroUsize>,
    ) -> Result<Self, OmFilesRsError> {
        println!("Creating io_uring reader from file: {}", file_path);

        // Create io_uring backend
        let backend = IoUringBackend::from_path(file_path, queue_depth)?;
        let backend = Arc::new(backend);

        // Create the reader
        let mut reader = Self::new(backend).await?;

        // Set concurrency if provided
        if let Some(max_concurrency) = max_concurrency {
            reader.set_max_concurrency(max_concurrency);
        }

        Ok(reader)
    }
}
