use crate::errors::OmFilesRsError;
use crate::io::writer::OmOffsetSize;
use om_file_format_sys::om_trailer_read;
use std::os::raw::c_void;

/// Process trailer data to extract OmOffsetSize of root variable
pub unsafe fn process_trailer(trailer_data: &[u8]) -> Result<OmOffsetSize, OmFilesRsError> {
    let mut offset = 0u64;
    let mut size = 0u64;
    if unsafe {
        !om_trailer_read(
            trailer_data.as_ptr() as *const c_void,
            &mut offset,
            &mut size,
        )
    } {
        return Err(OmFilesRsError::NotAnOmFile);
    }

    Ok(OmOffsetSize::new(offset, size))
}
