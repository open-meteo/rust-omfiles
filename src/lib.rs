//! omfiles is a Rust library for working with the Open-Meteo file format.
//!
//! This library provides functionality for reading and writing OM file format.
//!
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod reader;
pub mod reader_async;
pub mod traits;
mod variable;
pub mod writer;
pub(crate) mod backends {
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
    pub mod reader_utils;
    pub mod wrapped_decoder;
}
mod errors;

pub use backends::memory::InMemoryBackend;
pub use backends::mmapfile::{FileAccessMode, MmapFile};
pub use core::compression::OmCompressionType;
pub use core::data_types::OmDataType;
#[cfg(feature = "metadata-tree")]
pub use variable::OmOffsetSize;

pub use errors::OmFilesError;
pub use om_file_format_sys as _om_file_format_sys;
