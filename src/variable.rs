use om_file_format_sys::{OmVariable_t, om_variable_init};
use std::ops::Deref;
use std::os::raw::c_void;

/// A type indicating the offset and size of a variable in an OmFile.
#[derive(Debug, Clone, PartialEq)]
pub struct OmOffsetSize {
    pub offset: u64,
    pub size: u64,
}

impl OmOffsetSize {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

/// A wrapper ensuring the C pointer remains valid by holding the owner of the data.
#[derive(Clone, Debug)]
pub(crate) struct OmVariablePtr {
    /// The raw pointer to the C struct.
    pub(crate) ptr: *const OmVariable_t,
    /// Keeps the memory alive for this variable alive.
    _marker: Vec<u8>,
}

// Safety: We assert that the C library functions are thread-safe for read-only access.
// By holding `_marker`, we ensure the memory backing `ptr` is not deallocated.
unsafe impl Send for OmVariablePtr {}
unsafe impl Sync for OmVariablePtr {}

impl Deref for OmVariablePtr {
    type Target = *const OmVariable_t;
    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl OmVariablePtr {
    /// Initialize a new variable pointer from an Arc slice.
    pub(crate) fn new(data: Vec<u8>) -> Self {
        let ptr = unsafe { om_variable_init(data.as_ptr() as *const c_void) };
        Self { ptr, _marker: data }
    }
}
