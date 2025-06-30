use crate::backend::backends::OmFileReaderBackendAsync;
use crate::errors::OmFilesRsError;
use flume::{Receiver, Sender};
use io_uring::{opcode, types, IoUring};
use oneshot;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct IoUringBackend {
    size: usize,
    operation_tx: Sender<IoRequest>,
    _io_thread: std::thread::JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

struct IoRequest {
    offset: u64,
    size: u64,
    response: oneshot::Sender<Result<Vec<u8>, OmFilesRsError>>,
}

impl Drop for IoUringBackend {
    fn drop(&mut self) {
        // Signal the IO thread to shut down
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl IoUringBackend {
    pub fn new(file: File, queue_depth: Option<u32>) -> Result<Self, OmFilesRsError> {
        let queue_depth = queue_depth.unwrap_or(32);

        // Get file size
        let size = file
            .metadata()
            .map_err(|e| OmFilesRsError::FileWriterError {
                errno: e.raw_os_error().unwrap_or(0),
                error: format!("Failed to get file metadata: {}", e),
            })?
            .len() as usize;

        // Set up the shutdown flag
        let shutdown = Arc::new(AtomicBool::new(false));

        // Create the channel for sending operations
        let (operation_tx, operation_rx) = flume::bounded(1000);

        // Create a clone of the file for the io thread
        let io_file = file
            .try_clone()
            .map_err(|e| OmFilesRsError::FileWriterError {
                errno: e.raw_os_error().unwrap_or(0),
                error: format!("Failed to clone file: {}", e),
            })?;
        let io_shutdown = shutdown.clone();

        // Start the io thread
        let io_thread = std::thread::spawn(move || {
            if let Err(e) = io_thread_main(io_file, queue_depth, operation_rx, io_shutdown) {
                eprintln!("IO thread error: {:?}", e);
            }
        });

        Ok(Self {
            size,
            operation_tx,
            _io_thread: io_thread,
            shutdown,
        })
    }

    pub fn from_path(path: &str, queue_depth: Option<u32>) -> Result<Self, OmFilesRsError> {
        let file = File::open(path).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: path.to_string(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;

        Self::new(file, queue_depth)
    }
}

impl OmFileReaderBackendAsync for IoUringBackend {
    fn count_async(&self) -> usize {
        self.size
    }

    async fn get_bytes_async(&self, offset: u64, count: u64) -> Result<Vec<u8>, OmFilesRsError> {
        // Create a oneshot channel for the response
        let (response_tx, response_rx) = oneshot::channel();

        // Create the request
        let request = IoRequest {
            offset,
            size: count,
            response: response_tx,
        };

        // Send the request to the io thread
        self.operation_tx
            .send(request)
            .map_err(|_| OmFilesRsError::FileWriterError {
                errno: 0,
                error: "IO thread disconnected".into(),
            })?;

        // Wait for the response
        response_rx
            .recv()
            .map_err(|_| OmFilesRsError::FileWriterError {
                errno: 0,
                error: "Response channel closed".into(),
            })?
    }
}

fn io_thread_main(
    file: File,
    queue_depth: u32,
    operation_rx: Receiver<IoRequest>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), OmFilesRsError> {
    // Create the io_uring instance
    let mut ring = IoUring::new(queue_depth).map_err(|e| OmFilesRsError::FileWriterError {
        errno: e.raw_os_error().unwrap_or(0),
        error: format!("Failed to create io_uring: {}", e),
    })?;

    // // Try to register the file - optimization, non-critical if it fails
    // let file_registered = ring.registrar().register_files(&[file.as_raw_fd()]).is_ok();
    // if !file_registered {
    //     eprintln!("Warning: Failed to register file with io_uring. Performance might be slightly reduced.");
    // }

    // Buffer pool (simple version for demonstration)
    // A more sophisticated pool (e.g., sharded, size-classed) could be better.
    let mut buffer_pool: Vec<Vec<u8>> = Vec::with_capacity(queue_depth as usize);

    // Map to store pending operation ID -> (response_tx, buffer)
    let mut pending_ops: std::collections::HashMap<
        u64,
        (oneshot::Sender<Result<Vec<u8>, OmFilesRsError>>, Vec<u8>),
    > = std::collections::HashMap::with_capacity(queue_depth as usize);
    let mut next_op_id: u64 = 1; // Use unique IDs for user_data

    // Main loop: batch submissions and process completions
    while !shutdown.load(Ordering::Relaxed) {
        let mut submitted_ops_in_batch = 0;
        let mut made_progress = false;

        // --- Phase 1: Collect and Submit Requests ---
        // Try to fill the submission queue or process a reasonable batch size
        let ring_capacity = ring.submission().capacity();
        let batch_capacity = ring_capacity - ring.submission().len();
        let max_batch_size = std::cmp::min(batch_capacity, queue_depth as usize / 2); // Example batch limit

        for _ in 0..max_batch_size {
            match operation_rx.try_recv() {
                Ok(request) => {
                    made_progress = true; // We received a request
                    let op_id = next_op_id;
                    next_op_id += 1;

                    // Get or allocate buffer
                    let buffer_size = request.size as usize;
                    let mut buffer = buffer_pool
                        .pop()
                        .unwrap_or_else(|| Vec::with_capacity(buffer_size));
                    if buffer.capacity() < buffer_size {
                        buffer.reserve_exact(buffer_size - buffer.capacity());
                    }
                    // SAFETY: We ensure capacity above and will write into it via io_uring
                    unsafe {
                        buffer.set_len(buffer_size);
                    }

                    // Prepare the read operation
                    let read_e = opcode::Read::new(
                        types::Fd(file.as_raw_fd()),
                        buffer.as_mut_ptr(),
                        buffer.len() as u32,
                    )
                    .offset(request.offset)
                    .build()
                    .user_data(op_id); // Use unique ID

                    // Store pending operation details BEFORE pushing to SQ
                    pending_ops.insert(op_id, (request.response, buffer));

                    // Push to submission queue (unsafe block needed)
                    // SAFETY: We ensure the SQ has capacity before this loop.
                    // `pending_ops` holds the buffer and sender, ensuring they live long enough.
                    unsafe {
                        match ring.submission().push(&read_e) {
                            Ok(_) => submitted_ops_in_batch += 1,
                            Err(e) => {
                                // Failed to push (likely SQ full despite check, handle gracefully)
                                eprintln!(
                                    "Error pushing to submission queue (should be rare): {}",
                                    e
                                );
                                // Retrieve the op we just failed to submit
                                let (response, failed_buffer) = pending_ops.remove(&op_id).unwrap();
                                let _ = response.send(Err(OmFilesRsError::FileWriterError {
                                    errno: 0, // Consider mapping the error code if possible
                                    error: "Failed to push to submission queue".into(),
                                }));
                                buffer_pool.push(failed_buffer); // Return buffer
                                break; // Stop trying to submit more in this batch
                            }
                        }
                    }
                }
                Err(flume::TryRecvError::Empty) => {
                    // No more requests waiting in the channel for now
                    break;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    // Sender disconnected, initiate shutdown
                    shutdown.store(true, Ordering::Relaxed);
                    break; // Exit outer loop soon
                }
            }
        }

        // --- Phase 2: Submit Operations (if any) ---
        if submitted_ops_in_batch > 0 {
            // Submit all queued operations to the kernel.
            // Use submit() which doesn't block waiting for completions.
            match ring.submit() {
                Ok(submitted_count) => {
                    if submitted_count < submitted_ops_in_batch {
                        // This might happen under heavy load or specific kernel versions.
                        // The unsubmitted ops are still in the SQ ring buffer.
                        eprintln!(
                            "Warning: Submitted fewer ops ({}) than pushed ({})",
                            submitted_count, submitted_ops_in_batch
                        );
                    }
                }
                Err(e) => {
                    // This is a more serious error, potentially affecting multiple ops.
                    // We might need to fail pending ops associated with this submission attempt.
                    // For simplicity now, just log it. A robust implementation would need
                    // to carefully track which ops were part of the failed submit.
                    eprintln!("Critical error during ring.submit(): {}", e);
                    // Consider how to handle pending_ops related to this failed submit
                }
            }
        }

        // --- Phase 3: Process Completions ---
        // Check completion queue regardless of submission
        let cq_drained = ring.completion().is_empty(); // Check if empty *before* iterating
        for cqe in ring.completion() {
            made_progress = true; // We processed a completion
            let op_id = cqe.user_data();

            if let Some((response_tx, mut buffer)) = pending_ops.remove(&op_id) {
                let bytes_read_or_err = cqe.result();

                if bytes_read_or_err < 0 {
                    // Error occurred
                    let error = std::io::Error::from_raw_os_error(-bytes_read_or_err);
                    let _ = response_tx.send(Err(OmFilesRsError::FileWriterError {
                        errno: error.raw_os_error().unwrap_or(0),
                        error: format!("io_uring read error: {}", error),
                    }));
                    // Return buffer to pool on error
                    // SAFETY: Clear buffer before reuse in case of partial read on error
                    unsafe {
                        buffer.set_len(0);
                    }
                    buffer_pool.push(buffer);
                } else {
                    // Success
                    let bytes_read = bytes_read_or_err as usize;
                    // SAFETY: io_uring guarantees buffer is filled up to bytes_read
                    unsafe {
                        buffer.set_len(bytes_read);
                    }

                    // Send the successful result (buffer is moved)
                    let _ = response_tx.send(Ok(buffer));
                    // Buffer is NOT returned to pool here, it's owned by the receiver now
                }
            } else {
                // Spurious completion or op_id mismatch - should be rare
                eprintln!(
                    "Warning: Received completion for unknown or already completed op_id: {}",
                    op_id
                );
            }
        }

        // --- Phase 4: Wait Strategy ---
        if !made_progress && !shutdown.load(Ordering::Relaxed) {
            // No requests received and no completions processed.
            // Wait efficiently for new events (requests or completions).
            // Use submit_and_wait(0) if possible, otherwise fallback to timeout.
            // submit_and_wait(0) waits for completions without submitting anything new.
            if ring.submission().is_empty() {
                // Only wait if SQ is empty, otherwise submit might be needed first
                match ring.submit_and_wait(0) {
                    Ok(_) => {} // Waited successfully, loop will check completions again
                    Err(e) if e.raw_os_error() == Some(4032) => {
                        // EBUSY might mean SQPOLL thread is active, just yield
                        std::thread::yield_now();
                    }
                    Err(e) => {
                        // Other wait error
                        eprintln!("Error during submit_and_wait(0): {}", e);
                        std::thread::sleep(Duration::from_millis(1)); // Fallback sleep
                    }
                }
            } else {
                // SQ not empty, try submitting first in next loop iteration
                std::thread::yield_now();
            }
        } else if cq_drained && !ring.completion().is_empty() {
            // If CQ *was* empty but now isn't after processing, yield to allow
            // potentially woken tasks to run before we loop again.
            std::thread::yield_now();
        }
    } // End while !shutdown

    // --- Shutdown Phase ---
    // Handle any remaining pending operations (e.g., send error)
    eprintln!(
        "IO thread shutting down. {} operations pending.",
        pending_ops.len()
    );
    for (_op_id, (response_tx, mut buffer)) in pending_ops.drain() {
        let _ = response_tx.send(Err(OmFilesRsError::FileWriterError {
            errno: 0,
            error: "IO operation cancelled due to shutdown".into(),
        }));
        // Return buffer to pool
        unsafe {
            buffer.set_len(0);
        }
        buffer_pool.push(buffer);
    }

    Ok(())
}
