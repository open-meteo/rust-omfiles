use core::slice;
use std::mem;

use om_file_format_sys::OmDataType_t;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum DataType {
    None = 0,
    Int8 = 1,
    Uint8 = 2,
    Int16 = 3,
    Uint16 = 4,
    Int32 = 5,
    Uint32 = 6,
    Int64 = 7,
    Uint64 = 8,
    Float = 9,
    Double = 10,
    String = 11,
    Int8Array = 12,
    Uint8Array = 13,
    Int16Array = 14,
    Uint16Array = 15,
    Int32Array = 16,
    Uint32Array = 17,
    Int64Array = 18,
    Uint64Array = 19,
    FloatArray = 20,
    DoubleArray = 21,
    StringArray = 22,
}

impl DataType {
    pub fn to_c(&self) -> OmDataType_t {
        unsafe { std::mem::transmute(*self as u32) }
    }

    pub fn is_array(&self) -> bool {
        match self {
            DataType::Int8Array
            | DataType::Uint8Array
            | DataType::Int16Array
            | DataType::Uint16Array
            | DataType::Int32Array
            | DataType::Uint32Array
            | DataType::Int64Array
            | DataType::Uint64Array
            | DataType::FloatArray
            | DataType::DoubleArray
            | DataType::StringArray => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for DataType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DataType::None),
            1 => Ok(DataType::Int8),
            2 => Ok(DataType::Uint8),
            3 => Ok(DataType::Int16),
            4 => Ok(DataType::Uint16),
            5 => Ok(DataType::Int32),
            6 => Ok(DataType::Uint32),
            7 => Ok(DataType::Int64),
            8 => Ok(DataType::Uint64),
            9 => Ok(DataType::Float),
            10 => Ok(DataType::Double),
            11 => Ok(DataType::String),
            12 => Ok(DataType::Int8Array),
            13 => Ok(DataType::Uint8Array),
            14 => Ok(DataType::Int16Array),
            15 => Ok(DataType::Uint16Array),
            16 => Ok(DataType::Int32Array),
            17 => Ok(DataType::Uint32Array),
            18 => Ok(DataType::Int64Array),
            19 => Ok(DataType::Uint64Array),
            20 => Ok(DataType::FloatArray),
            21 => Ok(DataType::DoubleArray),
            22 => Ok(DataType::StringArray),
            _ => Err("Invalid data type value"),
        }
    }
}

/// Trait for types that can be stored as arrays in OmFiles
pub trait OmFileArrayDataType {
    const DATA_TYPE_ARRAY: DataType;
}

/// Trait for types that can be stored as scalars in OmFiles
pub trait OmFileScalarDataType: Default {
    const DATA_TYPE_SCALAR: DataType;

    /// Creates a new instance from raw bytes
    ///
    /// This is the default implementation, which assumes that the bytes
    /// represent a valid value of Self and that alignment requirements are met.
    fn from_raw_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= mem::size_of::<Self>(),
            "Buffer too small to contain type of size {}",
            mem::size_of::<Self>()
        );

        // Safety: This assumes the bytes represent a valid value of Self
        // and that alignment requirements are met
        unsafe {
            let mut result = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut result as *mut Self as *mut u8,
                mem::size_of::<Self>(),
            );
            result
        }
    }

    /// Performs an operation with the raw bytes of this value
    ///
    /// This is the default implementation, which passes a slice of the bytes
    /// of self to the provided closure.
    /// For String and OmNone types, this method is overridden to provide the
    /// UTF-8 bytes of the string and an empty slice, respectively.
    fn with_raw_bytes<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8]) -> T,
    {
        // Safety: This creates a slice that references the bytes of self
        let bytes = unsafe {
            slice::from_raw_parts(self as *const Self as *const u8, mem::size_of::<Self>())
        };
        f(bytes)
    }
}

// Implement both traits for all supported numeric types
impl OmFileArrayDataType for i8 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int8Array;
}
impl OmFileScalarDataType for i8 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int8;
}

impl OmFileArrayDataType for u8 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint8Array;
}
impl OmFileScalarDataType for u8 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint8;
}

impl OmFileArrayDataType for i16 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int16Array;
}
impl OmFileScalarDataType for i16 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int16;
}

impl OmFileArrayDataType for u16 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint16Array;
}
impl OmFileScalarDataType for u16 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint16;
}

impl OmFileArrayDataType for i32 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int32Array;
}
impl OmFileScalarDataType for i32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int32;
}

impl OmFileArrayDataType for u32 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint32Array;
}
impl OmFileScalarDataType for u32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint32;
}

impl OmFileArrayDataType for i64 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int64Array;
}
impl OmFileScalarDataType for i64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int64;
}

impl OmFileArrayDataType for u64 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint64Array;
}
impl OmFileScalarDataType for u64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint64;
}

impl OmFileArrayDataType for f32 {
    const DATA_TYPE_ARRAY: DataType = DataType::FloatArray;
}
impl OmFileScalarDataType for f32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Float;
}

impl OmFileArrayDataType for f64 {
    const DATA_TYPE_ARRAY: DataType = DataType::DoubleArray;
}
impl OmFileScalarDataType for f64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Double;
}

impl OmFileScalarDataType for String {
    const DATA_TYPE_SCALAR: DataType = DataType::String;

    /// Create a new String from raw bytes
    fn from_raw_bytes(bytes: &[u8]) -> Self {
        // Attempt to create a UTF-8 string from the bytes
        // If bytes are not valid UTF-8, replace invalid sequences
        String::from_utf8_lossy(bytes).into_owned()
    }

    /// Perform an operation with the raw bytes of this value
    /// This will always operate on the contiguous UTF-8 bytes of the string
    fn with_raw_bytes<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8]) -> T,
    {
        // Use the UTF-8 bytes of the string
        f(self.as_bytes())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OmNone();

impl OmFileScalarDataType for OmNone {
    const DATA_TYPE_SCALAR: DataType = DataType::None;

    fn from_raw_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() == 0, "OmNone should not have any bytes");
        // None type doesn't contain any data, so just return the default value
        OmNone()
    }

    fn with_raw_bytes<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&[u8]) -> T,
    {
        // None type doesn't have any bytes, so pass an empty slice
        f(&[])
    }
}
