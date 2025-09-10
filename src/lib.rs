//! omfiles: A Rust library for working with Open-Meteo OM files
//!
//! This library provides functionality for reading and writing OM file format.
//!

pub mod reader;
pub mod reader_async;
pub mod traits;
mod variable;
pub use variable::OmOffsetSize;
pub mod writer;

mod core {
    pub(crate) mod c_defaults;
    pub mod compression;
    pub mod data_types;
}
pub use core::compression::OmCompressionType;
pub use core::data_types::OmDataType;

pub mod backends {
    pub mod file;
    pub mod memory;
    pub mod mmapfile;
}

mod errors;
pub use errors::OmFilesError;

pub(crate) mod utils {
    pub mod buffered_writer;
    pub mod math;
    pub mod reader_utils;
    pub mod wrapped_decoder;
}
