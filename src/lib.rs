//! omfiles: A Rust library for working with Open-Meteo OM files
//!
//! This library provides functionality for reading and writing OM file format.
//!

pub mod traits;
pub mod io {
    pub(crate) mod buffered_writer;
    pub mod reader;
    pub mod reader_async;
    pub(crate) mod reader_utils;
    pub(crate) mod variable;
    pub(crate) mod wrapped_decoder;
    pub mod writer;
}

pub mod core {
    pub(crate) mod c_defaults;
    pub mod compression;
    pub mod data_types;
}

pub mod backends {
    pub mod file;
    pub mod memory;
    pub mod mmapfile;
}

pub mod errors;

mod utils;
