//! omfiles: A Rust library for working with Open-Meteo OM files
//!
//! This library provides functionality for reading and writing OM file format.
//!

pub mod reader;
pub mod reader_async;
pub mod traits;
mod variable;
pub mod writer;
pub mod backends {
    mod file;
    pub mod memory;
    pub mod mmapfile;
}
mod core {
    pub mod c_defaults;
    pub mod compression;
    pub mod data_types;
}
pub(crate) mod utils {
    pub mod buffered_writer;
    pub mod math;
    pub mod reader_utils;
    pub mod wrapped_decoder;
}
mod errors;

pub use core::compression::OmCompressionType;
pub use core::data_types::OmDataType;
pub use variable::OmOffsetSize;

pub use errors::OmFilesError;
