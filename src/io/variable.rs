use crate::io::writer::OmOffsetSize;
use om_file_format_sys::{OmVariable_t, om_variable_init};
use std::ops::Deref;
use std::os::raw::c_void;
use std::sync::Arc;

/// A wrapper around the raw C pointer OmVariable_t
/// marked as Send + Sync.
///
/// # Safety
///
/// This relies on the assumption that the underlying C library functions
/// used for reading metadata via this pointer (`om_variable_get_*`) are
/// thread-safe when called concurrently on the same immutable variable data.
/// The pointer itself points into the `variable_data` Vec owned by the
/// `OmFileReader`, ensuring its validity for the lifetime of the reader instance.
#[derive(Clone, Copy, Debug)]
pub struct OmVariablePtr(pub(crate) *const OmVariable_t);

/// SAFETY: See safety note above. We assert that read-only access via this pointer
/// is safe to perform concurrently from multiple threads, provided the underlying
/// `variable_data` remains valid and unchanged, which is guaranteed by `OmFileReader`'s ownership.
unsafe impl Send for OmVariablePtr {}
unsafe impl Sync for OmVariablePtr {}

impl Deref for OmVariablePtr {
    type Target = *const OmVariable_t;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Core struct to handle variable data and metadata
#[derive(Clone)]
pub struct OmVariableContainer {
    /// Holds the raw data backing the variable
    pub data: Arc<[u8]>,
    /// Offset and size information for the variable
    pub offset_size: Option<OmOffsetSize>,
    /// Opaque pointer to the variable defined by header/trailer
    pub(crate) variable: OmVariablePtr,
}

impl OmVariableContainer {
    /// Create a new variable from raw data
    pub fn new(data: Vec<u8>, offset_size: Option<OmOffsetSize>) -> Self {
        let data: Arc<[u8]> = data.into();
        let variable_ptr = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

        Self {
            data,
            offset_size,
            variable: OmVariablePtr(variable_ptr),
        }
    }
}
