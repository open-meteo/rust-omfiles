use ndarray::ArrayD;
use omfiles::backend::backends::InMemoryBackend;
use omfiles::backend::mmapfile::MmapFile;
use omfiles::core::compression::CompressionType;
use omfiles::errors::OmFilesRsError;
use omfiles::io::reader::OmFileReader;
use omfiles::io::writer::OmFileWriter;
use std::borrow::BorrowMut;
use std::fs;
use std::sync::Arc;

mod test_utils;
use test_utils::remove_file_if_exists;

#[test]
fn test_mismatching_cube_dimension_length() {
    let mut backend = InMemoryBackend::new(vec![]);
    let mut writer = OmFileWriter::new(backend.borrow_mut(), 1024);

    let result =
        writer.prepare_array::<i32>(vec![10, 10], vec![5], CompressionType::None, 1.0, 0.0);

    assert_eq!(error_string(result), "Mismatching cube dimension length");
}

#[test]
fn test_chunk_has_wrong_number_of_elements() {
    let mut backend = InMemoryBackend::new(vec![]);
    let mut writer = OmFileWriter::new(backend.borrow_mut(), 1024);

    let mut array_writer = writer
        .prepare_array::<i32>(
            vec![10, 10],
            vec![5, 5],
            CompressionType::PforDelta2d,
            1.0,
            0.0,
        )
        .unwrap();

    let array = ArrayD::from_elem(vec![10, 11], 1);
    let result = array_writer.write_data(array.view(), None, None);

    assert_eq!(error_string(result), "Chunk has wrong number of elements");
}

#[test]
fn test_offset_and_count_exceed_dimension() {
    let mut backend = InMemoryBackend::new(vec![]);
    let mut writer = OmFileWriter::new(backend.borrow_mut(), 1024);

    let mut array_writer = writer
        .prepare_array::<i32>(
            vec![10, 10],
            vec![5, 5],
            CompressionType::PforDelta2d,
            1.0,
            0.0,
        )
        .unwrap();

    let array = ArrayD::from_elem(vec![10, 10], 1);
    let result = array_writer.write_data_flat(
        &array.as_slice().unwrap(),
        Some(&[10, 10]),
        Some(&[5, 5]),
        Some(&[6, 6]),
    );

    assert_eq!(
        error_string(result),
        "Offset and count exceed dimension: offset 5, count 6, dimension 10"
    );
}

#[test]
fn test_not_an_om_file() {
    let backend = InMemoryBackend::new(vec![0; 100]);
    let result = OmFileReader::new(Arc::new(backend));

    assert_eq!(error_string(result), "Not an OM file");
}

#[test]
fn test_mismatching_cube_dimension_length_for_read() {
    let mut backend = InMemoryBackend::new(vec![]);

    {
        let mut writer = OmFileWriter::new(backend.borrow_mut(), 1024);

        let mut array_writer = writer
            .prepare_array::<i32>(
                vec![10, 10],
                vec![5, 5],
                CompressionType::PforDelta2d,
                1.0,
                0.0,
            )
            .unwrap();

        let array = ArrayD::from_elem(vec![10, 10], 1);
        array_writer.write_data(array.view(), None, None).unwrap();

        let variable_meta = array_writer.finalize();
        let variable = writer.write_array(variable_meta, "data", &[]).unwrap();
        writer.write_trailer(variable).unwrap();
    }

    let reader = OmFileReader::new(Arc::new(backend)).unwrap();
    let result = reader.read::<i32>(&[0..10], None, None);

    assert_eq!(error_string(result), "Mismatching cube dimension length");
}

#[test]
fn test_opening_not_an_om_file() {
    let short_file = "not_an_om_file.txt";
    fs::write(short_file, b"This is not an OM file. ").unwrap();

    // Try to open the file as an OM file
    let result = OmFileReader::<MmapFile>::from_file(short_file);
    assert!(matches!(result, Err(OmFilesRsError::FileTooSmall)));
    remove_file_if_exists(short_file);

    let longer_file = "not_an_om_file.txt";
    fs::write(
        longer_file,
        b"This is not an OM file. It is longer than the previous one and could accommodate the header.",
    )
    .unwrap();

    // Try to open the file as an OM file
    let result = OmFileReader::<MmapFile>::from_file(longer_file);
    assert!(matches!(result, Err(OmFilesRsError::NotAnOmFile)));
    remove_file_if_exists(longer_file);
}
fn error_string<T>(result: Result<T, OmFilesRsError>) -> String {
    match result {
        Ok(_) => {
            assert!(false, "Expected error");
            String::new() // This line will never be reached
        }
        Err(e) => e.to_string(),
    }
}
