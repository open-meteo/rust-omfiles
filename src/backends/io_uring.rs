//! Linux `io_uring` reader backend.

use crate::{
    errors::OmFilesError, traits::OmFileReaderBackendAsync, utils::byte_range::checked_byte_range,
};
use flume::{Receiver, Sender};
use io_uring::{IoUring, opcode, types};
use std::{collections::HashMap, fs::File, ops::Deref, os::fd::AsRawFd, thread::JoinHandle};

const DEFAULT_QUEUE_DEPTH: u32 = 32;
const REQUEST_CHANNEL_CAPACITY_FACTOR: usize = 4;

/// Bytes returned by [`IoUringBackend`].
///
/// The underlying allocation is returned to the backend's buffer pool when
/// this value is dropped. If the backend has already shut down, the allocation
/// is simply freed.
pub struct IoUringBytes {
    buffer: Option<Vec<u8>>,
    recycle_tx: Sender<Vec<u8>>,
}

impl IoUringBytes {
    fn new(buffer: Vec<u8>, recycle_tx: Sender<Vec<u8>>) -> Self {
        Self {
            buffer: Some(buffer),
            recycle_tx,
        }
    }
}

impl Deref for IoUringBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer
            .as_deref()
            .expect("io_uring byte buffer is present until drop")
    }
}

impl Drop for IoUringBytes {
    fn drop(&mut self) {
        if let Some(mut buffer) = self.buffer.take() {
            buffer.clear();
            let _ = self.recycle_tx.send(buffer);
        }
    }
}

/// Asynchronous file reader backed by a dedicated `io_uring` worker.
pub struct IoUringBackend {
    size: usize,
    operation_tx: Option<Sender<IoRequest>>,
    recycle_tx: Sender<Vec<u8>>,
    io_thread: Option<JoinHandle<()>>,
}

struct IoRequest {
    offset: u64,
    size: usize,
    response: oneshot::Sender<Result<IoUringBytes, OmFilesError>>,
}

struct PendingOperation {
    response: oneshot::Sender<Result<IoUringBytes, OmFilesError>>,
    buffer: Vec<u8>,
    expected_size: usize,
}

impl Drop for IoUringBackend {
    fn drop(&mut self) {
        // Closing the request channel tells the worker to drain all requests
        // it has already accepted and then shut down.
        self.operation_tx.take();

        if let Some(io_thread) = self.io_thread.take()
            && io_thread.join().is_err()
        {
            // Drop cannot report an error. The worker avoids panicking, so
            // reaching this means an unexpected panic occurred.
            eprintln!("io_uring worker panicked during shutdown");
        }
    }
}

impl IoUringBackend {
    /// Create a backend from an open file.
    pub fn new(file: File, queue_depth: Option<u32>) -> Result<Self, OmFilesError> {
        let queue_depth = queue_depth.unwrap_or(DEFAULT_QUEUE_DEPTH);
        if queue_depth == 0 {
            return Err(reader_error(
                0,
                "io_uring queue depth must be greater than zero",
            ));
        }

        let size_u64 = file.metadata().map_err(map_reader_io_error)?.len();
        let size = usize::try_from(size_u64)
            .map_err(|_| reader_error(0, format!("file size {size_u64} does not fit in usize")))?;

        // Construct the ring before starting the worker so setup failures are
        // returned directly to the caller.
        let mut ring = IoUring::new(queue_depth).map_err(map_reader_io_error)?;
        let actual_queue_depth = ring.submission().capacity();
        let channel_capacity = actual_queue_depth
            .saturating_mul(REQUEST_CHANNEL_CAPACITY_FACTOR)
            .max(1);
        let (operation_tx, operation_rx) = flume::bounded(channel_capacity);
        let (recycle_tx, recycle_rx) = flume::unbounded();
        let worker_recycle_tx = recycle_tx.clone();

        let io_thread = std::thread::Builder::new()
            .name("omfiles-io-uring".to_string())
            .spawn(move || {
                io_thread_main(file, ring, operation_rx, recycle_rx, worker_recycle_tx);
            })
            .map_err(map_reader_io_error)?;

        Ok(Self {
            size,
            operation_tx: Some(operation_tx),
            recycle_tx,
            io_thread: Some(io_thread),
        })
    }

    /// Open a file and create an `io_uring` backend for it.
    pub fn from_path(
        path: impl AsRef<std::path::Path>,
        queue_depth: Option<u32>,
    ) -> Result<Self, OmFilesError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| OmFilesError::CannotOpenFile {
            filename: path.display().to_string(),
            errno: error.raw_os_error().unwrap_or(0),
            error: error.to_string(),
        })?;

        Self::new(file, queue_depth)
    }
}

impl OmFileReaderBackendAsync for IoUringBackend {
    type Bytes = IoUringBytes;

    fn count_async(&self) -> usize {
        self.size
    }

    async fn get_bytes_async(&self, offset: u64, count: u64) -> Result<Self::Bytes, OmFilesError> {
        let range = checked_byte_range(offset, count, self.size)?;
        let size = range.len();
        u32::try_from(size).map_err(|_| OmFilesError::InvalidBackendRead {
            offset,
            count,
            size: self.size,
        })?;

        if size == 0 {
            return Ok(IoUringBytes::new(Vec::new(), self.recycle_tx.clone()));
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = IoRequest {
            offset,
            size,
            response: response_tx,
        };

        self.operation_tx
            .as_ref()
            .ok_or_else(|| reader_error(0, "io_uring worker is shutting down"))?
            .send_async(request)
            .await
            .map_err(|_| reader_error(0, "io_uring worker disconnected"))?;

        response_rx
            .await
            .map_err(|_| reader_error(0, "io_uring response channel closed"))?
    }
}

fn io_thread_main(
    file: File,
    mut ring: IoUring,
    operation_rx: Receiver<IoRequest>,
    recycle_rx: Receiver<Vec<u8>>,
    recycle_tx: Sender<Vec<u8>>,
) {
    let max_in_flight = ring.submission().capacity();
    let mut buffer_pool = Vec::with_capacity(max_in_flight);
    let mut pending = HashMap::<u64, PendingOperation>::with_capacity(max_in_flight);
    let mut next_operation_id = 1_u64;
    let mut request_channel_closed = false;

    loop {
        drain_recycled_buffers(&recycle_rx, &mut buffer_pool);

        if pending.is_empty() && !request_channel_closed {
            match operation_rx.recv() {
                Ok(request) => {
                    if let Err(request) = enqueue_request(
                        &file,
                        &mut ring,
                        &mut pending,
                        &mut buffer_pool,
                        &mut next_operation_id,
                        request,
                    ) {
                        fail_request(request, 0, "io_uring submission queue is full");
                    }
                }
                Err(_) => request_channel_closed = true,
            }
        }

        while pending.len() < max_in_flight && !request_channel_closed {
            match operation_rx.try_recv() {
                Ok(request) => {
                    if let Err(request) = enqueue_request(
                        &file,
                        &mut ring,
                        &mut pending,
                        &mut buffer_pool,
                        &mut next_operation_id,
                        request,
                    ) {
                        fail_request(request, 0, "io_uring submission queue is full");
                        break;
                    }
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    request_channel_closed = true;
                }
            }
        }

        if request_channel_closed && pending.is_empty() {
            // All accepted requests have completed. No SQE still references a
            // userspace buffer, so it is safe to destroy the pool and ring.
            break;
        }

        if !pending.is_empty() {
            if let Err(error) = ring.submit_and_wait(1) {
                let errno = error.raw_os_error().unwrap_or(0);
                let message = format!("io_uring submission failed: {error}");

                // Destroy the ring before freeing any pending buffer. Ring
                // teardown cancels/drains operations that still reference
                // those buffers.
                drop(ring);
                fail_all_pending(&mut pending, errno, &message);
                fail_queued_requests(&operation_rx, errno, &message);
                return;
            }

            process_completions(&mut ring, &mut pending, &mut buffer_pool, &recycle_tx);
        }
    }
}

fn enqueue_request(
    file: &File,
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingOperation>,
    buffer_pool: &mut Vec<Vec<u8>>,
    next_operation_id: &mut u64,
    request: IoRequest,
) -> Result<(), IoRequest> {
    let operation_id = next_free_operation_id(pending, next_operation_id);
    let mut buffer = take_buffer(buffer_pool, request.size);
    let read = opcode::Read::new(
        types::Fd(file.as_raw_fd()),
        buffer.as_mut_ptr(),
        request.size as u32,
    )
    .offset(request.offset)
    .build()
    .user_data(operation_id);

    // The buffer has length zero, but has at least `request.size` bytes of
    // capacity. It stays in `pending` at the same stable allocation until the
    // matching CQE arrives.
    if unsafe { ring.submission().push(&read) }.is_err() {
        buffer_pool.push(buffer);
        return Err(request);
    }

    pending.insert(
        operation_id,
        PendingOperation {
            response: request.response,
            buffer,
            expected_size: request.size,
        },
    );
    Ok(())
}

fn process_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, PendingOperation>,
    buffer_pool: &mut Vec<Vec<u8>>,
    recycle_tx: &Sender<Vec<u8>>,
) {
    for completion in ring.completion() {
        let Some(mut operation) = pending.remove(&completion.user_data()) else {
            continue;
        };

        let result = completion.result();
        if result < 0 {
            let error = std::io::Error::from_raw_os_error(-result);
            let _ = operation.response.send(Err(map_reader_io_error(error)));
            buffer_pool.push(operation.buffer);
            continue;
        }

        let bytes_read = result as usize;
        if bytes_read != operation.expected_size {
            let error = reader_error(
                0,
                format!(
                    "short io_uring read: expected {} bytes, received {bytes_read}",
                    operation.expected_size
                ),
            );
            let _ = operation.response.send(Err(error));
            buffer_pool.push(operation.buffer);
            continue;
        }

        // SAFETY: The CQE reports that exactly `bytes_read` bytes were written
        // to a buffer with at least that much capacity.
        unsafe {
            operation.buffer.set_len(bytes_read);
        }
        let bytes = IoUringBytes::new(operation.buffer, recycle_tx.clone());
        if let Err(send_error) = operation.response.send(Ok(bytes)) {
            // Dropping the unsent response recycles its buffer.
            drop(send_error.into_inner());
        }
    }
}

fn take_buffer(buffer_pool: &mut Vec<Vec<u8>>, size: usize) -> Vec<u8> {
    let mut buffer = buffer_pool.pop().unwrap_or_default();
    buffer.clear();
    if buffer.capacity() < size {
        // `reserve_exact` takes an amount additional to the current length,
        // not additional to the current capacity. The buffer length is zero.
        buffer.reserve_exact(size);
    }
    buffer
}

fn drain_recycled_buffers(recycle_rx: &Receiver<Vec<u8>>, buffer_pool: &mut Vec<Vec<u8>>) {
    while let Ok(mut buffer) = recycle_rx.try_recv() {
        buffer.clear();
        buffer_pool.push(buffer);
    }
}

fn next_free_operation_id(
    pending: &HashMap<u64, PendingOperation>,
    next_operation_id: &mut u64,
) -> u64 {
    loop {
        let candidate = *next_operation_id;
        *next_operation_id = next_operation_id.wrapping_add(1);
        if candidate != 0 && !pending.contains_key(&candidate) {
            return candidate;
        }
    }
}

fn fail_request(request: IoRequest, errno: i32, message: &str) {
    let _ = request
        .response
        .send(Err(reader_error(errno, message.to_string())));
}

fn fail_all_pending(pending: &mut HashMap<u64, PendingOperation>, errno: i32, message: &str) {
    for (_, operation) in pending.drain() {
        let _ = operation
            .response
            .send(Err(reader_error(errno, message.to_string())));
    }
}

fn fail_queued_requests(operation_rx: &Receiver<IoRequest>, errno: i32, message: &str) {
    while let Ok(request) = operation_rx.try_recv() {
        fail_request(request, errno, message);
    }
}

fn map_reader_io_error(error: std::io::Error) -> OmFilesError {
    reader_error(error.raw_os_error().unwrap_or(0), error.to_string())
}

fn reader_error(errno: i32, error: impl Into<String>) -> OmFilesError {
    OmFilesError::FileReaderError {
        errno,
        error: error.into(),
    }
}
