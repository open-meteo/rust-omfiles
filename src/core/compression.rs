use crate::errors::OmFilesError;
use om_file_format_sys::OmCompression_t;

/// Compression types supported in OmFiles.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OmCompressionType {
    /// Lossy compression using 2D delta coding and scale-factor.
    /// Only supports float and scales to 16-bit signed integer.
    PforDelta2dInt16 = 0,
    /// Lossless float/double compression using 2D xor coding.
    FpxXor2d = 1,
    /// PFor integer compression.
    /// f32 values are scaled to u32, f64 are scaled to u64.
    PforDelta2d = 2,
    /// Similar to `PforDelta2dInt16` but applies `log10(1+x)` before.
    PforDelta2dInt16Logarithmic = 3,
    None = 4,
}

impl OmCompressionType {
    pub(crate) fn to_c(&self) -> OmCompression_t {
        unsafe { std::mem::transmute(*self as u32) }
    }
}

impl TryFrom<u8> for OmCompressionType {
    type Error = OmFilesError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OmCompressionType::PforDelta2dInt16),
            1 => Ok(OmCompressionType::FpxXor2d),
            2 => Ok(OmCompressionType::PforDelta2d),
            3 => Ok(OmCompressionType::PforDelta2dInt16Logarithmic),
            4 => Ok(OmCompressionType::None),
            _ => Err(OmFilesError::InvalidCompressionType),
        }
    }
}
