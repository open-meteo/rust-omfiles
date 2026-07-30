#[cfg(target_os = "linux")]
mod tests {
    use futures_util::future::join_all;
    use macro_rules_attribute::apply;
    use ndarray::Array2;
    use omfiles::{
        IoUringBackend, OmCompressionType, OmFilesError,
        reader::OmFileReader,
        reader_async::OmFileReaderAsync,
        traits::{OmFileReaderBackendAsync, OmFileVariable as _},
        writer::OmFileWriter,
    };
    use smol_macros::test;
    use std::{
        fs::{File, remove_file},
        io::Write,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(name: &str) -> Self {
            let id = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
            Self(
                std::env::temp_dir().join(format!("omfiles-{name}-{}-{id}.om", std::process::id())),
            )
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = remove_file(&self.0);
        }
    }

    fn open_backend(path: &PathBuf, queue_depth: u32) -> Option<IoUringBackend> {
        match IoUringBackend::from_path(path, Some(queue_depth)) {
            Ok(backend) => Some(backend),
            Err(OmFilesError::FileReaderError { errno, .. })
                if matches!(errno, libc::ENOSYS | libc::EPERM | libc::EACCES) =>
            {
                eprintln!("io_uring is unavailable in this test environment; skipping");
                None
            }
            Err(error) => panic!("failed to create io_uring backend: {error}"),
        }
    }

    #[apply(test!)]
    async fn reads_many_ranges_concurrently_at_queue_depth_one() {
        let file = TestFile::new("raw-concurrent");
        let contents = (0..32_768_u32)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        File::create(&file.0).unwrap().write_all(&contents).unwrap();

        let Some(backend) = open_backend(&file.0, 1) else {
            return;
        };

        let reads = (0..64_u64).map(|index| {
            let offset = index * 257;
            backend.get_bytes_async(offset, 193)
        });
        let results = join_all(reads).await;

        for (index, result) in results.into_iter().enumerate() {
            let offset = index * 257;
            let bytes = result.unwrap();
            assert_eq!(&*bytes, &contents[offset..offset + 193]);
        }
    }

    #[apply(test!)]
    async fn rejects_out_of_bounds_reads() {
        let file = TestFile::new("raw-bounds");
        File::create(&file.0)
            .unwrap()
            .write_all(&[1, 2, 3, 4])
            .unwrap();

        let Some(backend) = open_backend(&file.0, 4) else {
            return;
        };

        assert!(matches!(
            backend.get_bytes_async(3, 2).await,
            Err(OmFilesError::InvalidBackendRead {
                offset: 3,
                count: 2,
                size: 4
            })
        ));
        assert!(backend.get_bytes_async(4, 0).await.unwrap().is_empty());
    }

    #[apply(test!)]
    async fn reads_a_self_contained_om_file() {
        let file = TestFile::new("roundtrip");
        let expected =
            Array2::from_shape_fn((64, 96), |(x, y)| (x as f32 * 0.25) + y as f32).into_dyn();

        {
            let file_handle = File::create(&file.0).unwrap();
            let mut file_writer = OmFileWriter::new(&file_handle, 8);
            let mut writer = file_writer
                .prepare_array::<f32>(
                    vec![64, 96],
                    vec![8, 12],
                    OmCompressionType::PforDelta2dInt16,
                    0.25,
                    0.0,
                )
                .unwrap();
            writer.write_data(expected.view(), None, None).unwrap();
            let metadata = writer.finalize();
            let variable = file_writer.write_array(metadata, "data", &[]).unwrap();
            file_writer.write_trailer(variable).unwrap();
        }

        let expected_decoded = OmFileReader::from_file(file.0.to_str().unwrap())
            .unwrap()
            .expect_array()
            .unwrap()
            .read::<f32>(&[0..64, 0..96])
            .unwrap();

        let Some(backend) = open_backend(&file.0, 16) else {
            return;
        };
        let reader = OmFileReaderAsync::new(Arc::new(backend)).await.unwrap();
        assert_eq!(reader.name(), "data");

        let actual = reader
            .expect_array()
            .unwrap()
            .read::<f32>(&[0..64, 0..96])
            .await
            .unwrap();
        assert_eq!(actual, expected_decoded);
    }
}
