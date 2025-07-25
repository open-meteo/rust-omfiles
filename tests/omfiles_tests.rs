use macro_rules_attribute::apply;
use ndarray::{Array2, ArrayD, ArrayViewD, s};
use om_file_format_sys::{
    OmError_t, fpxdec32, fpxenc32, om_variable_get_children_count, om_variable_get_scalar,
    om_variable_get_type, om_variable_init, om_variable_write_scalar,
    om_variable_write_scalar_size,
};
use omfiles::{
    backend::{
        backends::{InMemoryBackend, OmFileReaderBackend},
        mmapfile::{MmapFile, Mode},
    },
    core::{compression::CompressionType, data_types::DataType},
    errors::OmFilesRsError,
    io::{
        reader::OmFileReader,
        reader_async::OmFileReaderAsync,
        writer::{OmFileWriter, OmOffsetSize},
    },
};
use smol_macros::test;
use std::{
    borrow::BorrowMut,
    collections::HashMap,
    f32::{self},
    ffi::c_void,
    fs::{self, File},
    ptr, slice,
    sync::Arc,
};

mod test_utils;
use test_utils::remove_file_if_exists;

#[test]
fn turbo_pfor_roundtrip() {
    let data: Vec<f32> = vec![10.0, 22.0, 23.0, 24.0];
    let length = data.len();

    // create buffers for compression and decompression!
    let compressed_buffer = vec![0; 10];
    let compressed = compressed_buffer.as_slice();
    let decompress_buffer = vec![0.0; 10];
    let decompressed = decompress_buffer.as_slice();

    // compress data
    let compressed_size = unsafe {
        fpxenc32(
            data.as_ptr() as *mut u32,
            length,
            compressed.as_ptr() as *mut u8,
            0,
        )
    };
    if compressed_size >= compressed.len() {
        panic!("Compress Buffer too small");
    }

    // decompress data
    let decompressed_size = unsafe {
        fpxdec32(
            compressed.as_ptr() as *mut u8,
            length,
            decompressed.as_ptr() as *mut u32,
            0,
        )
    };
    if decompressed_size >= decompressed.len() {
        panic!("Decompress Buffer too small");
    }

    // this should be equal (we check it in the reader)
    // here we have a problem if length is only 1 and the exponent of the
    // float is greater than 0 (e.g. the value is greater than 10)
    // NOTE: This fails with 4 != 5 in the original turbo-pfor code
    assert_eq!(decompressed_size, compressed_size);
    assert_eq!(data[..length], decompressed[..length]);
}

#[test]
fn test_variable() {
    let name = "name";

    // Calculate size needed for scalar variable
    let size_scalar =
        unsafe { om_variable_write_scalar_size(name.len() as u16, 0, DataType::Int8.to_c(), 0) };

    assert_eq!(size_scalar, 13);

    // Create buffer for the variable
    let mut data = vec![255u8; size_scalar];
    let value: u8 = 177;

    // Write the scalar variable
    unsafe {
        om_variable_write_scalar(
            data.as_mut_ptr() as *mut std::os::raw::c_void,
            name.len() as u16,
            0,
            ptr::null(),
            ptr::null(),
            name.as_ptr() as *const ::std::os::raw::c_char,
            DataType::Int8.to_c(),
            &value as *const u8 as *const std::os::raw::c_void,
            0,
        );
    }

    assert_eq!(data, [1, 4, 4, 0, 0, 0, 0, 0, 177, 110, 97, 109, 101]);

    // Initialize a variable from the data
    let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

    // Verify the variable type and children count
    unsafe {
        assert_eq!(om_variable_get_type(om_variable), DataType::Int8.to_c());
        assert_eq!(om_variable_get_children_count(om_variable), 0);
    }

    // Get the scalar value
    let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
    let mut size: u64 = 0;

    let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

    // Verify successful retrieval and the value
    assert_eq!(error, OmError_t::ERROR_OK);
    assert!(!ptr.is_null());

    let result_value = unsafe { *(ptr as *const u8) };
    assert_eq!(result_value, value);
}

#[test]
fn test_variable_string() {
    let name = "name";
    let value = "Hello, World!";

    // Calculate size for string scalar
    let size_scalar = unsafe {
        om_variable_write_scalar_size(
            name.len() as u16,
            0,
            DataType::String.to_c(),
            value.len() as u64,
        )
    };

    assert_eq!(size_scalar, 33);

    // Create buffer for the variable
    let mut data = vec![255u8; size_scalar];

    // Write the string scalar
    unsafe {
        om_variable_write_scalar(
            data.as_mut_ptr() as *mut std::os::raw::c_void,
            name.len() as u16,
            0,
            ptr::null(),
            ptr::null(),
            name.as_ptr() as *const ::std::os::raw::c_char,
            DataType::String.to_c(),
            value.as_ptr() as *const std::os::raw::c_void,
            value.len(),
        );
    }

    // Verify the written data
    let expected = [
        11, // OmDataType_t: 11 = DATA_TYPE_STRING
        4,  // OmCompression_t: 4 = COMPRESSION_NONE
        4, 0, // Size of name
        0, 0, 0, 0, // Children count
        13, 0, 0, 0, 0, 0, 0, 0, // stringSize
        72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33, // "Hello, World!"
        110, 97, 109, 101, // "name"
    ];

    assert_eq!(data, expected);

    // Initialize a variable from the data
    let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

    // Verify the variable type and children count
    unsafe {
        assert_eq!(om_variable_get_type(om_variable), DataType::String.to_c());
        assert_eq!(om_variable_get_children_count(om_variable), 0);
    }

    // Get the scalar value
    let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
    let mut size: u64 = 0;

    let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

    // Verify successful retrieval and the value
    assert_eq!(error, OmError_t::ERROR_OK);
    assert!(!ptr.is_null());

    // Convert the raw bytes back to a string
    let string_bytes = unsafe { slice::from_raw_parts(ptr as *const u8, size as usize) };
    let result_string = std::str::from_utf8(string_bytes).unwrap();

    assert_eq!(result_string, value);
}

#[test]
fn test_variable_none() {
    let name = "name";

    // Calculate size for None type scalar
    let size_scalar =
        unsafe { om_variable_write_scalar_size(name.len() as u16, 0, DataType::None.to_c(), 0) };

    assert_eq!(size_scalar, 12); // 8 (header) + 4 (name length) + 0 (no value)

    // Create buffer for the variable
    let mut data = vec![255u8; size_scalar];

    // Write the non-existing value -> This is essentially creating a Group
    unsafe {
        om_variable_write_scalar(
            data.as_mut_ptr() as *mut std::os::raw::c_void,
            name.len() as u16,
            0,
            ptr::null(),
            ptr::null(),
            name.as_ptr() as *const ::std::os::raw::c_char,
            DataType::None.to_c(),
            ptr::null(),
            0,
        );
    }

    // Verify the written data
    assert_eq!(data, [0, 4, 4, 0, 0, 0, 0, 0, 110, 97, 109, 101]);

    // Initialize a variable from the data
    let om_variable = unsafe { om_variable_init(data.as_ptr() as *const c_void) };

    // Verify the variable type and children count
    unsafe {
        assert_eq!(om_variable_get_type(om_variable), DataType::None.to_c());
        assert_eq!(om_variable_get_children_count(om_variable), 0);
    }

    // Try to get scalar value from None type (should fail)
    let mut ptr: *mut std::os::raw::c_void = ptr::null_mut();
    let mut size: u64 = 0;

    let error = unsafe { om_variable_get_scalar(om_variable, &mut ptr, &mut size) };

    // Verify that retrieval fails with the expected error
    assert_eq!(error, OmError_t::ERROR_INVALID_DATA_TYPE);
}

#[test]
fn test_none_variable_as_group() -> Result<(), Box<dyn std::error::Error>> {
    let mut in_memory_backend = InMemoryBackend::new(vec![]);
    let mut file_writer = OmFileWriter::new(in_memory_backend.borrow_mut(), 8);

    // Write a regular variable
    let int_var = file_writer.write_scalar(42i32, "attribute", &[])?;
    // Write a None type to indicate some type of group
    let group_var = file_writer.write_none("group", &[int_var])?;

    file_writer.write_trailer(group_var)?;
    drop(file_writer);

    // Read the file
    let read = OmFileReader::new(Arc::new(in_memory_backend))?;

    // Verify the group variable
    assert_eq!(read.get_name().unwrap(), "group");
    assert_eq!(read.data_type(), DataType::None);

    // Get the child variable, which is an attribute
    let child = read.get_child(0).unwrap();
    assert_eq!(child.get_name().unwrap(), "attribute");
    assert_eq!(child.data_type(), DataType::Int32);
    assert_eq!(child.read_scalar::<i32>().unwrap(), 42);

    Ok(())
}

#[test]
fn test_in_memory_int_compression() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<f32> = vec![
        0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0, 66.0,
        17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    let shape: Vec<u64> = vec![1, data.len() as u64];
    let chunks: Vec<u64> = vec![1, 10];
    let data = ArrayD::from_shape_vec(copy_vec_u64_to_vec_usize(&shape), data).unwrap();

    let must_equal = data.clone();
    let mut in_memory_backend = InMemoryBackend::new(vec![]);
    let mut file_writer = OmFileWriter::new(in_memory_backend.borrow_mut(), 8);

    let mut writer = file_writer
        .prepare_array::<f32>(shape, chunks, CompressionType::PforDelta2dInt16, 1.0, 0.0)
        .expect("Could not prepare writer");

    writer.write_data(data.view(), None, None)?;
    let variable_meta = writer.finalize();
    let variable = file_writer.write_array(variable_meta, "data", &[])?;
    file_writer.write_trailer(variable)?;
    drop(file_writer); // drop file_writer to release mutable borrow

    assert_eq!(in_memory_backend.count(), 136);
    let read = OmFileReader::new(Arc::new(in_memory_backend))?;
    let uncompressed = read.read::<f32>(&[0u64..1, 0..data.len() as u64], None, None)?;

    assert_eq!(&must_equal, &uncompressed);

    Ok(())
}

#[test]
fn test_in_memory_f32_compression() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<f32> = vec![
        0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0, 66.0,
        17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    let shape: Vec<u64> = vec![1, data.len() as u64];
    let chunks: Vec<u64> = vec![1, 10];
    let data = ArrayD::from_shape_vec(copy_vec_u64_to_vec_usize(&shape), data).unwrap();

    let must_equal = data.clone();
    let mut in_memory_backend = InMemoryBackend::new(vec![]);
    let mut file_writer = OmFileWriter::new(in_memory_backend.borrow_mut(), 8);

    let mut writer = file_writer
        .prepare_array::<f32>(shape, chunks, CompressionType::FpxXor2d, 1.0, 0.0)
        .expect("Could not prepare writer");

    writer.write_data(data.view(), None, None)?;
    let variable_meta = writer.finalize();
    let variable = file_writer.write_array(variable_meta, "data", &[])?;
    file_writer.write_trailer(variable)?;
    drop(file_writer); // drop file_writer to release mutable borrow

    assert_eq!(in_memory_backend.count(), 160);
    let read = OmFileReader::new(Arc::new(in_memory_backend))?;
    let uncompressed = read.read::<f32>(&[0u64..1, 0..data.len() as u64], None, None)?;

    assert_eq!(&must_equal, &uncompressed);

    Ok(())
}

#[test]
fn test_write_more_data_than_expected() -> Result<(), Box<dyn std::error::Error>> {
    let mut in_memory_backend = InMemoryBackend::new(vec![]);
    let mut file_writer = OmFileWriter::new(in_memory_backend.borrow_mut(), 8);
    let mut writer = file_writer.prepare_array::<f32>(
        vec![5, 5],
        vec![2, 2],
        CompressionType::PforDelta2dInt16,
        1.0,
        0.0,
    )?;

    // Try to write more data than the dimensions allow
    let too_much_data: Vec<f32> = (0..30).map(|x| x as f32).collect();
    let too_much_data = ArrayD::from_shape_vec(vec![5, 6], too_much_data).unwrap();
    let result = writer.write_data(too_much_data.view(), None, None);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err, OmFilesRsError::ChunkHasWrongNumberOfElements);

    Ok(())
}

#[test]
fn test_write_large() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_write_large.om";
    remove_file_if_exists(file);

    // Set up the writer with the specified dimensions and chunk dimensions
    let dims = vec![100, 100, 10];
    let chunk_dimensions = vec![2, 2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    let data: Vec<f32> = (0..100000).map(|x| (x % 10000) as f32).collect();
    let data = ArrayD::from_shape_vec(copy_vec_u64_to_vec_usize(&dims), data)?;

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");

        writer.write_data(data.view(), None, None)?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader::new(Arc::new(read_backend))?;

        let a1 = read.read::<f32>(&[50..51, 20..21, 1..2], None, None)?;
        assert_eq!(a1.as_slice().unwrap(), &vec![201.0]);

        let a = read.read::<f32>(&[0..100, 0..100, 0..10], None, None)?;
        assert_eq!(a.len(), data.len());
        let range = s![0..100, 0..1, 0..1];
        assert_eq!(a.slice(range), data.slice(range));
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_write_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_write_chunks.om";
    remove_file_if_exists(file);

    // Set up the writer with the specified dimensions and chunk dimensions
    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");
        fn dyn_array2d<T>(shape: [usize; 2], data: Vec<T>) -> ArrayD<T> {
            Array2::from_shape_vec(shape, data).unwrap().into_dyn()
        }

        // Directly feed individual chunks
        writer.write_data(
            dyn_array2d([2, 2], vec![0.0, 1.0, 5.0, 6.0]).view(),
            None,
            None,
        )?;
        writer.write_data(
            dyn_array2d([2, 2], vec![2.0, 3.0, 7.0, 8.0]).view(),
            None,
            None,
        )?;
        writer.write_data(dyn_array2d([2, 1], vec![4.0, 9.0]).view(), None, None)?;
        writer.write_data(
            dyn_array2d([2, 2], vec![10.0, 11.0, 15.0, 16.0]).view(),
            None,
            None,
        )?;
        writer.write_data(
            dyn_array2d([2, 2], vec![12.0, 13.0, 17.0, 18.0]).view(),
            None,
            None,
        )?;
        writer.write_data(dyn_array2d([2, 1], vec![14.0, 19.0]).view(), None, None)?;
        writer.write_data(dyn_array2d([1, 2], vec![20.0, 21.0]).view(), None, None)?;
        writer.write_data(dyn_array2d([1, 2], vec![22.0, 23.0]).view(), None, None)?;
        writer.write_data(dyn_array2d([1, 1], vec![24.0]).view(), None, None)?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // test reading
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

        let backend = Arc::new(read_backend);

        let read = OmFileReader::new(backend.clone())?;

        let a = read.read::<f32>(&[0..5, 0..5], None, None)?;
        let expected = ArrayD::from_shape_vec(
            vec![5, 5],
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
            ],
        )
        .unwrap();

        assert_eq!(a, expected);

        // check the actual bytes of the file
        let count = backend.count() as u64;
        assert_eq!(count, 144);

        // let bytes = backend.get_bytes(0, count)?;
        // // difference on x86 and ARM cause by the underlying compression
        // assert_eq!(
        //     bytes,
        // &[
        //     79, 77, 3, 0, 4, 130, 0, 2, 3, 34, 0, 4, 194, 2, 10, 4, 178, 0, 12, 4, 242, 0, 14, 197,
        //     17, 20, 194, 2, 22, 194, 2, 24, 3, 3, 228, 200, 109, 1, 0, 0, 20, 0, 4, 0, 0, 0, 0, 0,
        //     6, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63,
        //     0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2,
        //     0, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 79, 77, 3, 0, 0, 0, 0, 0, 40, 0, 0,
        //     0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0
        // ]
        // );
        // assert_eq!(
        //     bytes,
        //     &[
        //         79, 77, 3, 0, 4, 130, 64, 2, 3, 34, 16, 4, 194, 2, 10, 4, 178, 64, 12, 4, 242, 64, 14,
        //         197, 17, 20, 194, 2, 22, 194, 2, 24, 3, 3, 228, 200, 109, 1, 0, 0, 20, 0, 4, 0, 0, 0,
        //         0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        //         128, 63, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
        //         0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 79, 77, 3, 0, 0, 0, 0, 0,
        //         40, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0
        //     ]
        // );
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_offset_write() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_offset_write.om";
    remove_file_if_exists(file);

    // Set up the writer with the specified dimensions and chunk dimensions
    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    // Deliberately add NaN on all positions that should not be written to the file.
    // Only the inner 5x5 array is written.
    let data = vec![
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        0.0,
        1.0,
        2.0,
        3.0,
        4.0,
        f32::NAN,
        f32::NAN,
        5.0,
        6.0,
        7.0,
        8.0,
        9.0,
        f32::NAN,
        f32::NAN,
        10.0,
        11.0,
        12.0,
        13.0,
        14.0,
        f32::NAN,
        f32::NAN,
        15.0,
        16.0,
        17.0,
        18.0,
        19.0,
        f32::NAN,
        f32::NAN,
        20.0,
        21.0,
        22.0,
        23.0,
        24.0,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
    ];

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");

        // Write data with array dimensions [7,7] and reading from [1..6, 1..6]
        let data = ArrayD::from_shape_vec(vec![7, 7], data).unwrap();
        writer.write_data(data.view(), Some(&[1, 1]), Some(&[5, 5]))?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // Read the file
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader::new(Arc::new(read_backend))?;

        // Read the data
        let a = read.read::<f32>(&[0..5, 0..5], None, None)?;

        // Expected data
        let expected = ArrayD::from_shape_vec(
            vec![5, 5],
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
            ],
        )
        .unwrap();

        assert_eq!(a, expected);
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_write_3d() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_write_3d.om";
    remove_file_if_exists(file);

    let dims = vec![3, 3, 3];
    let chunk_dimensions = vec![2, 2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    let data = ArrayD::from_shape_vec(
        copy_vec_u64_to_vec_usize(&dims),
        vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0,
        ],
    )
    .unwrap();

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");

        writer.write_data(data.view(), None, None)?;

        let variable_meta = writer.finalize();
        let int32_attribute = file_writer.write_scalar(12323154i32, "int32", &[])?;
        let double_attribute = file_writer.write_scalar(12323154f64, "double", &[])?;
        let variable =
            file_writer.write_array(variable_meta, "data", &[int32_attribute, double_attribute])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // Read the file
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let backend = Arc::new(read_backend);
        let read = OmFileReader::new(backend.clone())?;

        assert_eq!(read.number_of_children(), 2);

        let child = read.get_child(0).unwrap();
        assert_eq!(child.read_scalar::<i32>().unwrap(), 12323154i32);
        assert_eq!(child.get_name().unwrap(), "int32");

        let child2 = read.get_child(1).unwrap();
        assert_eq!(child2.read_scalar::<f64>().unwrap(), 12323154f64);
        assert_eq!(child2.get_name().unwrap(), "double");

        assert!(read.get_child(2).is_none());

        let a = read.read::<f32>(&[0..3, 0..3, 0..3], None, None)?;
        assert_eq!(a, data);

        // Single index checks
        for x in 0..dims[0] {
            for y in 0..dims[1] {
                for z in 0..dims[2] {
                    let value = read.read::<f32>(&[x..x + 1, y..y + 1, z..z + 1], None, None)?;
                    let expected =
                        ArrayD::from_shape_vec(vec![1, 1, 1], vec![(x * 9 + y * 3 + z) as f32])
                            .unwrap();
                    assert_eq!(value, expected);
                }
            }
        }

        let count = backend.count();
        assert_eq!(count, 240);
        let bytes = backend.get_bytes(0, count as u64)?;
        assert_eq!(&bytes[0..3], &[79, 77, 3]);
        assert_eq!(&bytes[3..8], &[0, 3, 34, 140, 2]);
        // difference on x86 and ARM cause by the underlying compression
        assert!(&bytes[8..12] == &[2, 3, 114, 1] || &bytes[8..12] == &[2, 3, 114, 141]);
        assert!(&bytes[12..16] == &[6, 3, 34, 0] || &bytes[12..16] == &[6, 3, 34, 140]);

        assert_eq!(&bytes[16..19], &[8, 194, 2]);
        assert_eq!(&bytes[19..23], &[18, 5, 226, 3]);
        assert_eq!(&bytes[23..26], &[20, 198, 33]);
        assert_eq!(&bytes[26..29], &[24, 194, 2]);
        assert_eq!(&bytes[29..30], &[26]);
        assert_eq!(&bytes[30..35], &[3, 3, 37, 199, 45]);
        assert_eq!(&bytes[35..40], &[0, 0, 0, 0, 0]);
        assert_eq!(
            &bytes[40..57],
            &[5, 4, 5, 0, 0, 0, 0, 0, 82, 9, 188, 0, 105, 110, 116, 51, 50]
        );
        assert_eq!(
            &bytes[65..87],
            &[
                4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 64, 42, 129, 103, 65, 100, 111, 117, 98, 108, 101, 0
            ]
        );
        assert_eq!(
            &bytes[88..212],
            &[
                20, 0, 4, 0, 2, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0,
                0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
                3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
                0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97
            ]
        );
        assert_eq!(
            &bytes[216..240],
            &[
                79, 77, 3, 0, 0, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 124, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_hierarchical_variables() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_hierarchical.om";
    remove_file_if_exists(file);

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);

        // Create a parent array
        let parent_dims = vec![3, 3];
        let parent_chunks = vec![2, 2];
        let parent_data = ArrayD::from_shape_vec(
            copy_vec_u64_to_vec_usize(&parent_dims),
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        )
        .unwrap();

        // Create sub-child array first (will be child of child1)
        let subchild_dims = vec![4, 500];
        let subchild_chunks = vec![2, 2];
        let subchild_data = ArrayD::from_shape_vec(
            copy_vec_u64_to_vec_usize(&subchild_dims),
            vec![(30..2030).map(|x| x as f32).collect::<Vec<f32>>()].concat(),
        )
        .unwrap();

        let mut subchild_writer = file_writer.prepare_array::<f32>(
            subchild_dims.clone(),
            subchild_chunks.clone(),
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )?;
        subchild_writer.write_data(subchild_data.view(), None, None)?;
        let subchild_meta = subchild_writer.finalize();

        // Create child arrays
        let child_dims = vec![2, 2];
        let child_chunks = vec![2, 2];
        let child1_data = ArrayD::from_shape_vec(
            copy_vec_u64_to_vec_usize(&child_dims),
            vec![10.0, 11.0, 12.0, 13.0],
        )
        .unwrap();
        let child2_data = ArrayD::from_shape_vec(
            copy_vec_u64_to_vec_usize(&child_dims),
            vec![20.0, 21.0, 22.0, 23.0],
        )
        .unwrap();

        // Write child arrays (child1 with subchild)
        let mut child1_writer = file_writer.prepare_array::<f32>(
            child_dims.clone(),
            child_chunks.clone(),
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )?;
        child1_writer.write_data(child1_data.view(), None, None)?;
        let child1_meta = child1_writer.finalize();

        let mut child2_writer = file_writer.prepare_array::<f32>(
            child_dims.clone(),
            child_chunks.clone(),
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )?;
        child2_writer.write_data(child2_data.view(), None, None)?;
        let child2_meta = child2_writer.finalize();

        // Write parent array with children
        let mut parent_writer = file_writer.prepare_array::<f32>(
            parent_dims,
            parent_chunks,
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )?;
        parent_writer.write_data(parent_data.view(), None, None)?;
        let parent_meta = parent_writer.finalize();

        // Write meta and attribute information just before the trailer
        let int32_attribute = file_writer.write_scalar(12323154i32, "int32", &[])?;
        let double_attribute = file_writer.write_scalar(12323154f64, "double", &[])?;
        let string_attribute = file_writer.write_scalar("hello".to_string(), "string", &[])?;
        let subchild_var = file_writer.write_array(subchild_meta, "subchild", &[])?;
        let child1_var = file_writer.write_array(child1_meta, "child1", &[subchild_var])?;
        let child2_var = file_writer.write_array(child2_meta, "child2", &[])?;
        let parent_var = file_writer.write_array(
            parent_meta,
            "parent",
            &[
                child1_var,
                child2_var,
                int32_attribute,
                double_attribute,
                string_attribute,
            ],
        )?;

        file_writer.write_trailer(parent_var)?;
    }

    {
        // Verify the hierarchical structure
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let reader = OmFileReader::new(Arc::new(read_backend))?;

        let all_children_meta = reader.get_flat_variable_metadata();
        let expected_metadata = [
            ("parent/int32", OmOffsetSize::new(3920, 17)),
            ("parent/double", OmOffsetSize::new(3944, 22)),
            ("parent/string", OmOffsetSize::new(3968, 27)),
            ("parent/child1/subchild", OmOffsetSize::new(4000, 80)),
            ("parent/child1", OmOffsetSize::new(4080, 94)),
            ("parent/child2", OmOffsetSize::new(4176, 78)),
            ("parent", OmOffsetSize::new(4256, 158)),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect::<HashMap<String, OmOffsetSize>>();

        assert_eq!(all_children_meta, expected_metadata);

        // Check parent data
        let parent = reader.read::<f32>(&[0..3, 0..3], None, None)?;
        let expected_parent = ArrayD::from_shape_vec(
            vec![3, 3],
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        )
        .unwrap();
        assert_eq!(parent, expected_parent);

        // Check number of children at root level
        assert_eq!(reader.number_of_children(), 5);

        // Check child1 data and its subchild
        let child1 = reader.get_child(0).unwrap();
        assert_eq!(child1.get_name().unwrap(), "child1");
        let child1_data = child1.read::<f32>(&[0..2, 0..2], None, None)?;
        let expected_child1 =
            ArrayD::from_shape_vec(vec![2, 2], vec![10.0, 11.0, 12.0, 13.0]).unwrap();
        assert_eq!(child1_data, expected_child1);

        // Check child1's subchild
        assert_eq!(child1.number_of_children(), 1);
        let subchild = child1.get_child(0).unwrap();
        assert_eq!(subchild.get_name().unwrap(), "subchild");
        let subchild_data = subchild.read::<f32>(&[0..4, 0..500], None, None)?;
        let expected_subchild = ArrayD::from_shape_vec(
            vec![4, 500],
            vec![(30..2030).map(|x| x as f32).collect::<Vec<f32>>()].concat(),
        )
        .unwrap();
        assert_eq!(subchild_data, expected_subchild);

        // Check child2 data (no children)
        let child2 = reader.get_child(1).unwrap();
        assert_eq!(child2.get_name().unwrap(), "child2");
        assert_eq!(child2.number_of_children(), 0);
        let child2_data = child2.read::<f32>(&[0..2, 0..2], None, None)?;
        let expected_child2 =
            ArrayD::from_shape_vec(vec![2, 2], vec![20.0, 21.0, 22.0, 23.0]).unwrap();
        assert_eq!(child2_data, expected_child2);

        // Check attributes
        let int32 = reader.get_child(2).unwrap();
        assert_eq!(int32.get_name().unwrap(), "int32");
        assert_eq!(int32.read_scalar::<i32>().unwrap(), 12323154i32);

        let double = reader.get_child(3).unwrap();
        assert_eq!(double.get_name().unwrap(), "double");
        assert_eq!(double.read_scalar::<f64>().unwrap(), 12323154f64);

        let string = reader.get_child(4).unwrap();
        assert_eq!(string.get_name().unwrap(), "string");
        assert_eq!(string.read_scalar::<String>().unwrap(), "hello");
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_write_v3() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_write_v3.om";
    remove_file_if_exists(file);

    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    let data = ArrayD::from_shape_fn(copy_vec_u64_to_vec_usize(&dims), |x| {
        (x[0] * 5 + x[1]) as f32
    });

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");

        writer.write_data(data.view(), None, None)?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // Open file for reading
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let backend = Arc::new(read_backend);
        let read = OmFileReader::new(backend.clone())?;

        // Rest of test remains the same but using read.read::<f32>() instead of read_var.read()
        let a = read.read::<f32>(&[0..5, 0..5], None, None)?;
        let expected = data.clone();
        assert_eq!(a, expected);

        // Single index checks
        for x in 0..5 {
            for y in 0..5 {
                let value = read.read::<f32>(&[x..x + 1, y..y + 1], None, None)?;
                let expected =
                    ArrayD::from_shape_vec(vec![1, 1], vec![(x * 5 + y) as f32]).unwrap();
                assert_eq!(value, expected);
            }
        }

        // Read into existing array with offset
        for x in 0..5 {
            for y in 0..5 {
                let mut r = ArrayD::from_elem(vec![3, 3], f32::NAN);
                read.read_into(
                    &mut r,
                    &[x..x + 1, y..y + 1],
                    &[1, 1],
                    &[3, 3],
                    Some(0),
                    Some(0),
                )?;
                let expected = ArrayD::from_shape_vec(
                    vec![3, 3],
                    vec![
                        f32::NAN,
                        f32::NAN,
                        f32::NAN,
                        f32::NAN,
                        (x * 5 + y) as f32,
                        f32::NAN,
                        f32::NAN,
                        f32::NAN,
                        f32::NAN,
                    ],
                )
                .unwrap();
                nd_assert_eq_with_nan(&r, &expected);
            }
        }

        // Rest of checks with read.read::<f32>()
        // 2x in fast dimension
        for x in 0..5 {
            for y in 0..4 {
                let value = read.read::<f32>(&[x..x + 1, y..y + 2], None, None)?;
                let expected = ArrayD::from_shape_vec(
                    vec![1, 2],
                    vec![(x * 5 + y) as f32, (x * 5 + y + 1) as f32],
                )
                .unwrap();
                assert_eq!(value, expected);
            }
        }

        // 2x in slow dimension
        for x in 0..4 {
            for y in 0..5 {
                let value = read.read::<f32>(&[x..x + 2, y..y + 1], None, None)?;
                let expected = ArrayD::from_shape_vec(
                    vec![2, 1],
                    vec![(x * 5 + y) as f32, ((x + 1) * 5 + y) as f32],
                )
                .unwrap();
                assert_eq!(value, expected);
            }
        }

        // 2x2 regions
        for x in 0..4 {
            for y in 0..4 {
                let value = read.read::<f32>(&[x..x + 2, y..y + 2], None, None)?;
                let expected = ArrayD::from_shape_vec(
                    vec![2, 2],
                    vec![
                        (x * 5 + y) as f32,
                        (x * 5 + y + 1) as f32,
                        ((x + 1) * 5 + y) as f32,
                        ((x + 1) * 5 + y + 1) as f32,
                    ],
                )
                .unwrap();
                assert_eq!(value, expected);
            }
        }

        // 3x3 regions
        for x in 0..3 {
            for y in 0..3 {
                let value = read.read::<f32>(&[x..x + 3, y..y + 3], None, None)?;
                let expected = ArrayD::from_shape_vec(
                    vec![3, 3],
                    vec![
                        (x * 5 + y) as f32,
                        (x * 5 + y + 1) as f32,
                        (x * 5 + y + 2) as f32,
                        ((x + 1) * 5 + y) as f32,
                        ((x + 1) * 5 + y + 1) as f32,
                        ((x + 1) * 5 + y + 2) as f32,
                        ((x + 2) * 5 + y) as f32,
                        ((x + 2) * 5 + y + 1) as f32,
                        ((x + 2) * 5 + y + 2) as f32,
                    ],
                )
                .unwrap();
                assert_eq!(value, expected);
            }
        }

        // 1x5 regions
        for x in 0..5 {
            let value = read.read::<f32>(&[x..x + 1, 0..5], None, None)?;
            let expected = ArrayD::from_shape_vec(
                vec![1, 5],
                vec![
                    (x * 5) as f32,
                    (x * 5 + 1) as f32,
                    (x * 5 + 2) as f32,
                    (x * 5 + 3) as f32,
                    (x * 5 + 4) as f32,
                ],
            )
            .unwrap();
            assert_eq!(value, expected);
        }

        // 5x1 regions
        for x in 0..5 {
            let value = read.read::<f32>(&[0..5, x..x + 1], None, None)?;
            let expected = ArrayD::from_shape_vec(
                vec![5, 1],
                vec![
                    (x) as f32,
                    (x + 5) as f32,
                    (x + 10) as f32,
                    (x + 15) as f32,
                    (x + 20) as f32,
                ],
            )
            .unwrap();
            assert_eq!(value, expected);
        }

        let count = backend.count();
        let bytes = backend.get_bytes(0, count as u64)?;
        assert_eq!(
            &bytes,
            &[
                79, 77, 3, 0, 4, 130, 0, 2, 3, 34, 0, 4, 194, 2, 10, 4, 178, 0, 12, 4, 242, 0, 14,
                197, 17, 20, 194, 2, 22, 194, 2, 24, 3, 3, 228, 200, 109, 1, 0, 0, 20, 0, 4, 0, 0,
                0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 128, 63, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
                0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 79, 77, 3, 0,
                0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_write_v3_max_io_limit() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_write_v3_max_io_limit.om";
    remove_file_if_exists(file);

    // Define dimensions and writer parameters
    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;
    // Define the data to write
    let data = ArrayD::from_shape_vec(
        copy_vec_u64_to_vec_usize(&dims),
        vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
        ],
    )
    .unwrap();

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer
            .prepare_array::<f32>(
                dims.clone(),
                chunk_dimensions,
                compression,
                scale_factor,
                add_offset,
            )
            .expect("Could not prepare writer");

        writer.write_data(data.view(), None, None)?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // Open the file for reading
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        // Initialize the reader using the open_file method
        let read = OmFileReader::new(Arc::new(read_backend))?;

        // Read with io_size_max: 0, io_size_merge: 0
        let a = read.read::<f32>(&[0..5, 0..5], Some(0), Some(0))?;
        let expected = data.clone();
        assert_eq!(a, expected);

        // Single index checks
        for x in 0..dims[0] {
            for y in 0..dims[1] {
                let value = read.read::<f32>(&[x..x + 1, y..y + 1], Some(0), Some(0))?;
                assert_eq!(*value.first().unwrap(), (x * 5 + y) as f32);
            }
        }
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_nan() -> Result<(), Box<dyn std::error::Error>> {
    let file = "test_nan.om";
    remove_file_if_exists(file);

    let shape: Vec<u64> = vec![5, 5];
    let chunks: Vec<u64> = vec![5, 5];
    let data = ArrayD::from_elem(copy_vec_u64_to_vec_usize(&shape), f32::NAN);

    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer.prepare_array::<f32>(
            shape,
            chunks,
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )?;

        writer.write_data(data.view(), None, None)?;
        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    {
        // Read the data back
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let reader = OmFileReader::new(Arc::new(read_backend))?;

        // Assert that all values in the specified range are NaN
        let values = reader.read::<f32>(&[1..2, 1..2], None, None)?;
        assert!(values.iter().all(|x| x.is_nan()));
    }

    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_opening_legacy_file() {
    // For legacy files, we need a file with the correct legacy header format
    let file = "legacy_om_file.om";

    // Create a minimal legacy OM file
    // The header needs to make om_header_type return OM_HEADER_LEGACY
    // Note: This is a simplified test that depends on the header detection logic
    {
        let mut data = vec![0u8; 40]; // Minimal header size
        data[0] = 79; // 'O'
        data[1] = 77; // 'M'
        data[2] = 2; // version 2 is legacy!

        fs::write(file, data).unwrap();
    }

    // Try to open the legacy file and check properties of the reader
    let result = OmFileReader::<MmapFile>::from_file(file);
    assert!(result.is_ok());
    let reader = result.unwrap();
    println!("Compression Type: {:?}", reader.compression());
    assert_eq!(reader.compression(), CompressionType::PforDelta2dInt16);
    assert_eq!(reader.get_dimensions(), [0u64, 0u64]);
    assert_eq!(reader.get_chunk_dimensions(), [0u64, 0u64]);
    assert_eq!(reader.get_name(), None);

    // Clean up
    remove_file_if_exists(file);
}

#[apply(test!)]
async fn test_read_async() -> Result<(), Box<dyn std::error::Error>> {
    // Setup: Create a test file with multi-dimensional data
    let file = "test_read_async.om";
    remove_file_if_exists(file);

    let dims = vec![10, 10, 10]; // 3D data
    let chunk_dimensions = vec![4, 4, 4];
    let compression = CompressionType::PforDelta2dInt16;
    let scale_factor = 1.0;
    let add_offset = 0.0;

    // Generate test data
    let data = ArrayD::from_shape_vec(
        copy_vec_u64_to_vec_usize(&dims),
        (0..1000).map(|i| i as f32).collect(),
    )
    .unwrap();

    // Write the test file
    {
        let file_handle = File::create(file)?;
        let mut file_writer = OmFileWriter::new(&file_handle, 8);
        let mut writer = file_writer.prepare_array::<f32>(
            dims.clone(),
            chunk_dimensions,
            compression,
            scale_factor,
            add_offset,
        )?;

        writer.write_data(data.view(), None, None)?;

        let variable_meta = writer.finalize();
        let variable = file_writer.write_array(variable_meta, "data", &[])?;
        file_writer.write_trailer(variable)?;
    }

    // Test async read functionality
    {
        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let async_reader = OmFileReaderAsync::new(Arc::new(read_backend)).await?;

        let async_data = async_reader
            .read::<f32>(&[0..10, 0..10, 0..10], None, None)
            .await?;

        assert_eq!(data, async_data);

        // Test 2: Read partial data
        let partial_async = async_reader
            .read::<f32>(&[2..5, 3..7, 1..3], None, None)
            .await?;
        assert_eq!(data.slice(s![2..5, 3..7, 1..3]).into_dyn(), partial_async);

        // Test 4: Test with different IO size parameters
        let small_io = async_reader
            .read::<f32>(&[0..10, 0..10, 0..10], Some(128), Some(32))
            .await?;
        let large_io = async_reader
            .read::<f32>(&[0..10, 0..10, 0..10], Some(65536), Some(1024))
            .await?;

        assert_eq!(small_io, large_io);
        assert_eq!(small_io, data);
    }

    // Clean up
    remove_file_if_exists(file);
    Ok(())
}

fn copy_vec_u64_to_vec_usize(input: &Vec<u64>) -> Vec<usize> {
    input.iter().map(|&x| x as usize).collect()
}

fn nd_assert_eq_with_nan(expected: &ArrayD<f32>, actual: &ArrayD<f32>) {
    nd_assert_eq_with_accuracy_and_nan(expected.view(), actual.view(), f32::EPSILON);
}

fn nd_assert_eq_with_accuracy_and_nan(
    expected: ArrayViewD<f32>,
    actual: ArrayViewD<f32>,
    accuracy: f32,
) {
    assert_eq!(expected.shape(), actual.shape());
    for (e, a) in expected.iter().zip(actual.iter()) {
        if e.is_nan() {
            assert!(a.is_nan(), "Expected NaN, found {}", a);
        } else {
            assert!(
                (e - a).abs() < accuracy,
                "Values differ: expected {}, found {}",
                e,
                a
            );
        }
    }
}
