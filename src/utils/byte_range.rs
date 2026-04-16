use crate::errors::OmFilesError;
use std::ops::Range;

/// Convert a byte `offset` and `count` into a checked `usize` range within `size`.
///
/// This is intended for backend implementations that need to safely slice a byte
/// buffer without risking integer conversion overflow, addition overflow, or
/// out-of-bounds access.
pub(crate) fn checked_byte_range(
    offset: u64,
    count: u64,
    size: usize,
) -> Result<Range<usize>, OmFilesError> {
    let invalid_read = || OmFilesError::InvalidBackendRead {
        offset,
        count,
        size,
    };

    let start = usize::try_from(offset).map_err(|_| invalid_read())?;
    let len = usize::try_from(count).map_err(|_| invalid_read())?;
    let end = start.checked_add(len).ok_or_else(invalid_read)?;

    if end > size {
        return Err(invalid_read());
    }

    Ok(start..end)
}
