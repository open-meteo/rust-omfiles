//! Trait implementations for `std::fs::File` types.
//!
//! This module provides `OmFileWriterBackend` implementations for both
//! `File` and `&File`, enabling direct use of standard library file types
//! as OM file backends.

use std::{fs::File, io::Write};

use crate::{errors::OmFilesRsError, traits::OmFileWriterBackend};

impl OmFileWriterBackend for &File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all().map_err(|e| map_io_error(e))?;
        Ok(())
    }
}

impl OmFileWriterBackend for File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all().map_err(|e| map_io_error(e))?;
        Ok(())
    }
}

fn map_io_error(e: std::io::Error) -> OmFilesRsError {
    OmFilesRsError::FileWriterError {
        errno: e.raw_os_error().unwrap_or(0),
        error: e.to_string(),
    }
}
