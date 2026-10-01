use crate::errors::OmFilesError;
use crate::variable::OmOffsetSize;
use om_file_format_sys::om_trailer_read;
use std::ops::Range;
use std::os::raw::c_void;

pub(crate) fn read_counts(ranges: &[Range<u64>]) -> Result<Vec<u64>, OmFilesError> {
    ranges
        .iter()
        .map(|range| {
            range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| OmFilesError::InvalidReadRange {
                    range: range.clone(),
                })
        })
        .collect()
}

/// Process trailer data to extract OmOffsetSize of root variable
pub unsafe fn process_trailer(trailer_data: &[u8]) -> Result<OmOffsetSize, OmFilesError> {
    let mut offset = 0u64;
    let mut size = 0u64;
    if unsafe {
        !om_trailer_read(
            trailer_data.as_ptr() as *const c_void,
            &mut offset,
            &mut size,
        )
    } {
        return Err(OmFilesError::NotAnOmFile);
    }

    Ok(OmOffsetSize::new(offset, size))
}
