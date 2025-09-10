//! Reader backend implementation based on memory-mapped files.

#[cfg(unix)]
use memmap2::{Advice, UncheckedAdvice};
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::File;

use crate::{
    errors::OmFilesError,
    traits::{OmFileReaderBackend, OmFileReaderBackendAsync},
};

/// File access mode
pub enum FileAccessMode {
    ReadOnly,
    ReadWrite,
}

/// Represents a memory-mapped file and implements the [`OmFileReaderBackend`](`OmFileReaderBackend`) trait.
///
/// The memory-mapped file can be mapped with read-only and read-write modes, but you should normally
/// only use the read-only mode!
pub struct MmapFile {
    data: MmapType,
    file: File,
}

/// Specifies how the memory-mapped file should be accessed and whether it is mutable
pub enum MmapType {
    ReadOnly(Mmap),
    ReadWrite(MmapMut),
}

impl MmapType {
    #[cfg(unix)]
    fn advise_range(&self, advice: Advice, offset: usize, len: usize) -> std::io::Result<()> {
        match self {
            MmapType::ReadOnly(mmap) => mmap.advise_range(advice, offset, len),
            MmapType::ReadWrite(mmap_mut) => mmap_mut.advise_range(advice, offset, len),
        }
    }

    #[cfg(unix)]
    fn unchecked_advise_range(
        &self,
        advice: UncheckedAdvice,
        offset: usize,
        len: usize,
    ) -> std::io::Result<()> {
        match self {
            MmapType::ReadOnly(mmap) => unsafe { mmap.unchecked_advise_range(advice, offset, len) },
            MmapType::ReadWrite(mmap_mut) => unsafe {
                mmap_mut.unchecked_advise_range(advice, offset, len)
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            MmapType::ReadOnly(mmap) => mmap.len(),
            MmapType::ReadWrite(mmap_mut) => mmap_mut.len(),
        }
    }
}

enum MAdvice {
    WillNeed,
    _DontNeed,
}

impl MAdvice {
    #[cfg(unix)]
    fn advice(&self, mmap: &MmapType, offset: usize, len: usize) -> std::io::Result<()> {
        match self {
            MAdvice::WillNeed => mmap.advise_range(Advice::WillNeed, offset, len),
            MAdvice::_DontNeed => {
                mmap.unchecked_advise_range(UncheckedAdvice::DontNeed, offset, len)
            }
        }
    }

    #[cfg(not(unix))]
    fn advice(&self, _mmap: &MmapType, _offset: usize, _len: usize) -> std::io::Result<()> {
        Ok(()) // No-op on non-Unix systems
    }
}

impl MmapFile {
    /// Mmap the entire filehandle
    pub fn new(file: File, mode: FileAccessMode) -> Result<Self, std::io::Error> {
        let data = match mode {
            FileAccessMode::ReadOnly => {
                MmapType::ReadOnly(unsafe { MmapOptions::new().map(&file)? })
            }
            FileAccessMode::ReadWrite => {
                MmapType::ReadWrite(unsafe { MmapOptions::new().map_mut(&file)? })
            }
        };
        Ok(MmapFile { data, file })
    }

    /// Check if the file was deleted on the file system. Linux keeps the file alive as long as some processes have it open.
    pub(crate) fn was_deleted(&self) -> bool {
        // Try to stat the file to see if it still exists
        match self.file.metadata() {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(_) => false, // Conservatively return false for other errors
        }
    }

    /// Tell the OS to prefetch the required memory pages. Subsequent calls to read data should be faster
    fn prefetch_data_advice(&self, offset: usize, count: usize, advice: MAdvice) {
        let page_size = 4096;
        let page_start = offset / page_size * page_size;
        let page_end = (offset + count + page_size - 1) / page_size * page_size;
        let length = page_end - page_start;
        // Note: length can be greater than data size, due to page cache alignment
        // precondition(length <= data.count, "Prefetch read exceeds length. Length=\(length) data count=\(data.count)")

        // Log any errors but continue execution
        advice
            .advice(&self.data, offset, length)
            .map_err(|e| {
                eprintln!("Failed to set memory advice: {}", e);
                ()
            })
            .unwrap_or(())
    }
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        // The Mmap type will automatically unmap the memory when it is dropped
    }
}

impl OmFileReaderBackend for MmapFile {
    type Bytes<'a> = &'a [u8];

    fn count(&self) -> usize {
        self.data.len()
    }

    fn prefetch_data(&self, offset: usize, count: usize) {
        self.prefetch_data_advice(offset, count, MAdvice::WillNeed);
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<Self::Bytes<'_>, OmFilesError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        match self.data {
            MmapType::ReadOnly(ref mmap) => Ok(&mmap[index_range]),
            MmapType::ReadWrite(ref mmap_mut) => Ok(&mmap_mut[index_range]),
        }
    }
}

impl OmFileReaderBackendAsync for MmapFile {
    fn count_async(&self) -> usize {
        self.data.len()
    }

    async fn get_bytes_async(&self, offset: u64, count: u64) -> Result<Vec<u8>, OmFilesError> {
        let data = self.get_bytes(offset, count);
        Ok(data?.to_vec())
    }
}
