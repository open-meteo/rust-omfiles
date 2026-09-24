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

// The C scalar writer loads numeric values through int16_t/int32_t/int64_t
// pointers. A plain byte array does not guarantee the required alignment.
#[repr(C, align(8))]
struct ScalarBytes<const N: usize>([u8; N]);

// Fixed-width primitives have no padding or invalid bit patterns. Keep their
// sizes and OM mappings together; scalar bytes use little-endian encoding.
macro_rules! impl_numeric_data_type {
    ($ty:ty, $scalar:ident, $array:ident, $width:literal) => {
        const _: () = {
            assert!(std::mem::size_of::<$ty>() == $width);
            assert!(std::mem::align_of::<ScalarBytes<$width>>() >= std::mem::align_of::<$ty>());
        };

        impl crate::traits::sealed::Sealed for $ty {}

        impl OmFileArrayDataType for $ty {
            const DATA_TYPE_ARRAY: OmDataType = OmDataType::$array;
        }

        impl OmFileScalarDataType for $ty {
            const DATA_TYPE_SCALAR: OmDataType = OmDataType::$scalar;

            fn from_raw_bytes(bytes: &[u8]) -> Self {
                let mut value = [0; $width];
                value.copy_from_slice(&bytes[..$width]);
                Self::from_le_bytes(value)
            }

            fn with_raw_bytes<T, F>(&self, f: F) -> T
            where
                F: FnOnce(&[u8]) -> T,
            {
                let bytes = ScalarBytes(self.to_le_bytes());
                f(&bytes.0)
            }
        }
    };
}

impl_numeric_data_type!(i8, Int8, Int8Array, 1);
impl_numeric_data_type!(u8, Uint8, Uint8Array, 1);
impl_numeric_data_type!(i16, Int16, Int16Array, 2);
impl_numeric_data_type!(u16, Uint16, Uint16Array, 2);
impl_numeric_data_type!(i32, Int32, Int32Array, 4);
impl_numeric_data_type!(u32, Uint32, Uint32Array, 4);
impl_numeric_data_type!(i64, Int64, Int64Array, 8);
impl_numeric_data_type!(u64, Uint64, Uint64Array, 8);
impl_numeric_data_type!(f32, Float, FloatArray, 4);
impl_numeric_data_type!(f64, Double, DoubleArray, 8);

impl crate::traits::sealed::Sealed for String {}
impl crate::traits::sealed::Sealed for OmNone {}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_scalars_use_little_endian_bytes() {
        let bytes = [0x78, 0x56, 0x34, 0x12];
        assert_eq!(u32::from_raw_bytes(&bytes), 0x1234_5678);
        0x1234_5678u32.with_raw_bytes(|encoded| assert_eq!(encoded, bytes));

        let bytes = [0x00, 0x00, 0xc0, 0x3f];
        assert_eq!(f32::from_raw_bytes(&bytes), 1.5);
        1.5f32.with_raw_bytes(|encoded| assert_eq!(encoded, bytes));
    }

    #[test]
    fn scalar_read_accepts_unaligned_input_and_ignores_trailing_bytes() {
        // The prefix places the value one byte past an aligned address.
        let storage = ScalarBytes([0xff, 8, 7, 6, 5, 4, 3, 2, 1, 0xee]);
        assert_eq!(u64::from_raw_bytes(&storage.0[1..]), 0x0102_0304_0506_0708);
    }

    #[test]
    fn scalar_write_provides_aligned_storage() {
        // The widest integer load used by the C scalar writer requires this
        // alignment. Float scalars use the same staging buffer.
        42u64.with_raw_bytes(|bytes| {
            assert_eq!(bytes.len(), 8);
            assert_eq!(bytes.as_ptr().align_offset(std::mem::align_of::<i64>()), 0);
        });
        1.5f64.with_raw_bytes(|bytes| {
            assert_eq!(bytes.len(), 8);
            assert_eq!(bytes.as_ptr().align_offset(std::mem::align_of::<i64>()), 0);
        });
    }

    #[test]
    #[should_panic]
    fn scalar_read_rejects_short_input() {
        u64::from_raw_bytes(&[0; 7]);
    }
}
