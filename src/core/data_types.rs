use om_file_format_sys::OmDataType_t;

use crate::traits::{OmFileArrayDataType, OmFileScalarDataType};

/// Data types supported in OmFiles.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OmDataType {
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

impl OmDataType {
    pub(crate) fn to_c(&self) -> OmDataType_t {
        unsafe { std::mem::transmute(*self as u32) }
    }

    /// Check if the data type is an array type.
    pub fn is_array(&self) -> bool {
        match self {
            OmDataType::Int8Array
            | OmDataType::Uint8Array
            | OmDataType::Int16Array
            | OmDataType::Uint16Array
            | OmDataType::Int32Array
            | OmDataType::Uint32Array
            | OmDataType::Int64Array
            | OmDataType::Uint64Array
            | OmDataType::FloatArray
            | OmDataType::DoubleArray => true,
            _ => false,
        }
    }

    /// Check if the data type is a scalar type.
    pub fn is_scalar(&self) -> bool {
        match self {
            OmDataType::Int8
            | OmDataType::Uint8
            | OmDataType::Int16
            | OmDataType::Uint16
            | OmDataType::Int32
            | OmDataType::Uint32
            | OmDataType::Int64
            | OmDataType::Uint64
            | OmDataType::Float
            | OmDataType::Double
            | OmDataType::String => true,
            _ => false,
        }
    }

    /// Check if the data type is a group.
    pub fn is_group(&self) -> bool {
        match self {
            OmDataType::None => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for OmDataType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OmDataType::None),
            1 => Ok(OmDataType::Int8),
            2 => Ok(OmDataType::Uint8),
            3 => Ok(OmDataType::Int16),
            4 => Ok(OmDataType::Uint16),
            5 => Ok(OmDataType::Int32),
            6 => Ok(OmDataType::Uint32),
            7 => Ok(OmDataType::Int64),
            8 => Ok(OmDataType::Uint64),
            9 => Ok(OmDataType::Float),
            10 => Ok(OmDataType::Double),
            11 => Ok(OmDataType::String),
            12 => Ok(OmDataType::Int8Array),
            13 => Ok(OmDataType::Uint8Array),
            14 => Ok(OmDataType::Int16Array),
            15 => Ok(OmDataType::Uint16Array),
            16 => Ok(OmDataType::Int32Array),
            17 => Ok(OmDataType::Uint32Array),
            18 => Ok(OmDataType::Int64Array),
            19 => Ok(OmDataType::Uint64Array),
            20 => Ok(OmDataType::FloatArray),
            21 => Ok(OmDataType::DoubleArray),
            22 => Ok(OmDataType::StringArray),
            _ => Err("Invalid data type value"),
        }
    }
}

// Implement both traits for all supported numeric types
impl OmFileArrayDataType for i8 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Int8Array;
}
impl OmFileScalarDataType for i8 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Int8;
}

impl OmFileArrayDataType for u8 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Uint8Array;
}
impl OmFileScalarDataType for u8 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Uint8;
}

impl OmFileArrayDataType for i16 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Int16Array;
}
impl OmFileScalarDataType for i16 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Int16;
}

impl OmFileArrayDataType for u16 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Uint16Array;
}
impl OmFileScalarDataType for u16 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Uint16;
}

impl OmFileArrayDataType for i32 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Int32Array;
}
impl OmFileScalarDataType for i32 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Int32;
}

impl OmFileArrayDataType for u32 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Uint32Array;
}
impl OmFileScalarDataType for u32 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Uint32;
}

impl OmFileArrayDataType for i64 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Int64Array;
}
impl OmFileScalarDataType for i64 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Int64;
}

impl OmFileArrayDataType for u64 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::Uint64Array;
}
impl OmFileScalarDataType for u64 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Uint64;
}

impl OmFileArrayDataType for f32 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::FloatArray;
}
impl OmFileScalarDataType for f32 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Float;
}

impl OmFileArrayDataType for f64 {
    const DATA_TYPE_ARRAY: OmDataType = OmDataType::DoubleArray;
}
impl OmFileScalarDataType for f64 {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::Double;
}

impl OmFileScalarDataType for String {
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::String;

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
    const DATA_TYPE_SCALAR: OmDataType = OmDataType::None;

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
