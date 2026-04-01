use criterion::async_executor::SmolExecutor;
use criterion::{Criterion, criterion_group, criterion_main};
use ndarray::{Array, ArrayViewD};
use omfiles::OmCompressionType;
use omfiles::{
    InMemoryBackend, reader::OmFileReader, reader_async::OmFileReaderAsync, writer::OmFileWriter,
};
use rand::Rng;
use std::{
    borrow::BorrowMut,
    fs::{self, File},
    hint::black_box,
    time::{Duration, Instant},
};

const DIM0_SIZE: u64 = 1024 * 1000;
const DIM1_SIZE: u64 = 1024;
const CHUNK0_SIZE: u64 = 20;
const CHUNK1_SIZE: u64 = 20;

fn write_om_file(file: &str, data: ArrayViewD<f32>) {
    let file_handle = File::create(file).unwrap();
    let mut file_writer = OmFileWriter::new(&file_handle, 8);

    let mut writer = file_writer
        .prepare_array::<f32>(
            vec![DIM0_SIZE, DIM1_SIZE],
            vec![CHUNK0_SIZE, CHUNK1_SIZE],
            OmCompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )
        .unwrap();

    writer.write_data(data, None, None).unwrap();
    let variable_meta = writer.finalize();
    let variable = file_writer.write_array(variable_meta, "data", &[]).unwrap();
    file_writer.write_trailer(variable).unwrap();
}

pub fn benchmark_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("In-memory operations");
    group.sample_size(10);

    let data = Array::from_shape_fn((DIM0_SIZE as usize, DIM1_SIZE as usize), |(i, j)| {
        (i * DIM1_SIZE as usize + j) as f32
    })
    .into_dyn();

    group.bench_function("write_in_memory", |b| {
        b.iter_custom(|iters| {
            let mut timer = Timer::new();
            timer.start();
            for _i in 0..iters {
                let mut backend = InMemoryBackend::new(vec![]);
                let mut file_writer = OmFileWriter::new(backend.borrow_mut(), 8);
                let mut writer = file_writer
                    .prepare_array::<f32>(
                        vec![DIM0_SIZE, DIM1_SIZE],
                        vec![CHUNK0_SIZE, CHUNK1_SIZE],
                        OmCompressionType::FpxXor2d,
                        0.1,
                        0.0,
                    )
                    .unwrap();

                black_box(writer.write_data(data.view(), None, None).unwrap());
                let variable_meta = writer.finalize();
                let variable = file_writer.write_array(variable_meta, "data", &[]).unwrap();
                black_box(file_writer.write_trailer(variable).unwrap());
            }
            timer.stop();
            timer.elapsed()
        })
    });

    group.finish();
}

pub fn benchmark_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("Write OM file");
    group.sample_size(10);

    let file = "benchmark.om";

    let data = Array::from_shape_fn((DIM0_SIZE as usize, DIM1_SIZE as usize), |(i, j)| {
        (i * DIM1_SIZE as usize + j) as f32
    })
    .into_dyn();

    group.bench_function("write_om_file", move |b| {
        b.iter_custom(|iters| {
            let mut timer = Timer::new();
            timer.start();
            for _i in 0..iters {
                remove_file_if_exists(file);
                timer.start();
                black_box(write_om_file(file, data.view()));
                timer.stop();
            }
            timer.stop();
            timer.elapsed()
        })
    });

    group.finish();
}

pub fn benchmark_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read OM file");

    let file = "benchmark.om";
    let reader = OmFileReader::from_file(file).unwrap();
    let reader = reader.expect_array().unwrap();

    let dim0_read_size = 256;

    group.bench_function("read_om_file", move |b| {
        b.to_async(SmolExecutor).iter(|| async {
            let random_x: u64 = rand::rng().random_range(0..DIM0_SIZE - dim0_read_size);
            let random_y: u64 = rand::rng().random_range(0..DIM1_SIZE);
            let values = reader
                .read::<f32>(&[random_x..random_x + dim0_read_size, random_y..random_y + 1])
                .expect("Could not read range");

            assert_eq!(values.len(), dim0_read_size as usize);
        });
    });

    group.finish();
}

pub fn benchmark_async_io_uring_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read OM file with async io_uring");

    let file = "benchmark.om";
    // Skip benchmark if not on Linux
    #[cfg(not(target_os = "linux"))]
    {
        group.bench_function("read_om_file_async_io_uring", |b| {
            b.iter(|| {
                println!("Skipping io_uring benchmark on non-Linux platform");
            });
        });
        group.finish();
        return;
    }
    #[cfg(target_os = "linux")]
    {
        // Ensure the file exists
        if !std::path::Path::new(file).exists() {
            let data = Array::from_shape_fn((DIM0_SIZE as usize, DIM1_SIZE as usize), |(i, j)| {
                (i * DIM1_SIZE as usize + j) as f32
            })
            .into_dyn();
            write_om_file(file, data.view());
        }

        let file = std::fs::File::open(file).expect("Failed to open file");
        let backend = omfiles::IoUringBackend::new(file, Some(256)).unwrap();

        let reader = smol::block_on(async {
            use std::sync::Arc;

            OmFileReaderAsync::new(Arc::new(backend))
                .await
                .expect("Failed to create async reader")
        });
        let reader = reader.expect_array().unwrap();
        let reader = &reader;
        let dim0_read_size = 256;

        // Test with multiple concurrent operations
        group.bench_function("read_om_file_async_io_uring_concurrent", move |b| {
            b.to_async(SmolExecutor).iter_custom(|iters| async move {
                let start = Instant::now();
                // Create concurrent operations
                const CONCURRENT_OPS: usize = 200;
                for _ in 0..iters {
                    let mut futures = Vec::with_capacity(CONCURRENT_OPS);
                    for _ in 0..CONCURRENT_OPS {
                        let random_x: u64 = rand::rng().random_range(0..DIM0_SIZE - dim0_read_size);
                        let random_y: u64 = rand::rng().random_range(0..DIM1_SIZE);

                        let future = async move {
                            reader
                                .read::<f32>(&[
                                    random_x..random_x + dim0_read_size,
                                    random_y..random_y + 1,
                                ])
                                .await
                        };
                        futures.push(future);
                    }

                    let mut results = Vec::with_capacity(CONCURRENT_OPS);
                    for future in futures {
                        results.push(future.await)
                    }

                    // Process results as they complete
                    for result in results {
                        let values = result.expect("Could not read range");
                        assert_eq!(values.len(), dim0_read_size as usize);
                    }
                }
                start.elapsed().div_f64(CONCURRENT_OPS as f64)
            })
        });

        // For comparison, also benchmark single operations
        group.bench_function("read_om_file_async_io_uring_sequential", move |b| {
            b.to_async(SmolExecutor).iter(|| async {
                let random_x: u64 = rand::rng().random_range(0..DIM0_SIZE - dim0_read_size);
                let random_y: u64 = rand::rng().random_range(0..DIM1_SIZE);
                let values = reader
                    .read::<f32>(&[random_x..random_x + dim0_read_size, random_y..random_y + 1])
                    .await
                    .expect("Could not read range");

                assert_eq!(values.len(), dim0_read_size as usize);
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    // benchmark_in_memory,
    // benchmark_write,
    benchmark_read,
    benchmark_async_io_uring_read
);
criterion_main!(benches);

struct Timer {
    start: Option<Instant>,
    elapsed: Duration,
}

impl Timer {
    fn new() -> Self {
        Timer {
            start: None,
            elapsed: Duration::new(0, 0),
        }
    }

    fn start(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
        }
    }

    fn stop(&mut self) {
        if let Some(start_time) = self.start {
            self.elapsed += start_time.elapsed();
            self.start = None;
        }
    }

    fn elapsed(&self) -> Duration {
        if let Some(start_time) = self.start {
            self.elapsed + start_time.elapsed()
        } else {
            self.elapsed
        }
    }
}

fn remove_file_if_exists(file: &str) {
    if fs::metadata(file).is_ok() {
        fs::remove_file(file).unwrap();
    }
}
