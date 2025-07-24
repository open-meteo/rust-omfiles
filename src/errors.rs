use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum OmFilesRsError {
    #[error("Cannot open file '{filename}': errno {errno}, error: {error}")]
    CannotOpenFile {
        filename: String,
        errno: i32,
        error: String,
    },
    #[error("File writer error: errno {errno}, error: {error}")]
    FileWriterError { errno: i32, error: String },
    #[error("Chunk has wrong number of elements")]
    ChunkHasWrongNumberOfElements,
    #[error(
        "Offset and count exceed dimension: offset {offset}, count {count}, dimension {dimension}"
    )]
    OffsetAndCountExceedDimension {
        offset: u64,
        count: u64,
        dimension: u64,
    },
    #[error("Dimension out of bounds: range {range:?}, allowed {allowed}")]
    DimensionOutOfBounds {
        range: std::ops::Range<usize>,
        allowed: usize,
    },
    #[error("Chunk dimension is smaller than overall dimension")]
    ChunkDimensionIsSmallerThanOverallDim,
    #[error("Dimension must be larger than 0")]
    DimensionMustBeLargerThan0,
    #[error("Mismatching cube dimension length")]
    MismatchingCubeDimensionLength,
    #[error("File exists already: {filename}")]
    FileExistsAlready { filename: String },
    #[error("Invalid compression type")]
    InvalidCompressionType,
    #[error("Invalid data type")]
    InvalidDataType,
    #[error("Decoder error {0}")]
    DecoderError(String),
    #[error("Not an OM file")]
    NotAnOmFile,
    #[error("File too small")]
    FileTooSmall,
    #[error("Not implemented: {0}")]
    NotImplementedError(String),
    #[error("Array not contiguous")]
    ArrayNotContiguous,
    #[error("Task Error: {0}")]
    TaskError(String),
}
