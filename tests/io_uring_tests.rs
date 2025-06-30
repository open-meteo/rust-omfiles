#[cfg(target_os = "linux")]
mod tests {
    use macro_rules_attribute::apply;
    use omfiles_rs::io::reader_async::OmFileReaderAsync;
    use smol_macros::test;
    use std::path::Path;

    #[apply(test!)]
    async fn test_io_uring_reader() {
        // Skip test if not on Linux
        if !cfg!(target_os = "linux") {
            println!("Skipping io_uring test on non-Linux platform");
            return;
        }

        let test_file_path = "era5_temp2m_year_2021.om";
        if !Path::new(test_file_path).exists() {
            println!("Test file not found, skipping test");
            return;
        }

        // Create an io_uring reader
        let reader = OmFileReaderAsync::from_file(test_file_path, Some(16), None)
            .await
            .expect("Failed to create reader");

        // Get dimensions info
        let dimensions = reader.get_dimensions();
        assert!(!dimensions.is_empty(), "Dimensions should not be empty");

        // Read a small slice of data
        let read_range = dimensions
            .iter()
            .map(|&d| 0..std::cmp::min(5, d))
            .collect::<Vec<_>>();

        let data = reader
            .read::<f32>(&read_range, None, None)
            .await
            .expect("Failed to read data");

        // Verify data shape
        let expected_shape: Vec<usize> = read_range
            .iter()
            .map(|r| (r.end - r.start) as usize)
            .collect();
        assert_eq!(data.shape(), expected_shape.as_slice());
    }
}
