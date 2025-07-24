use criterion::{Criterion, black_box, criterion_group, criterion_main};
use omfiles_rs::{
    backend::{
        backends::InMemoryBackend,
        mmapfile::{MmapFile, Mode},
    },
    core::compression::CompressionType,
    io::{reader::OmFileReader, writer::OmFileWriter},
};
use rand::Rng;
use std::{
    borrow::BorrowMut,
    fs::{self, File},
    sync::Arc,
    time::{Duration, Instant},
};

const DIM0_SIZE: u64 = 1024 * 1000;
const DIM1_SIZE: u64 = 1024;
const CHUNK0_SIZE: u64 = 20;
const CHUNK1_SIZE: u64 = 20;

fn write_om_file(file: &str, data: &[f32]) {
    let file_handle = File::create(file).unwrap();
    let mut file_writer = OmFileWriter::new(&file_handle, 8);

    let mut writer = file_writer
        .prepare_array::<f32>(
            vec![DIM0_SIZE, DIM1_SIZE],
            vec![CHUNK0_SIZE, CHUNK1_SIZE],
            CompressionType::PforDelta2dInt16,
            1.0,
            0.0,
        )
        .unwrap();

    writer.write_data_flat(data, None, None, None).unwrap();
    let variable_meta = writer.finalize();
    let variable = file_writer.write_array(variable_meta, "data", &[]).unwrap();
    file_writer.write_trailer(variable).unwrap();
}

pub fn benchmark_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("In-memory operations");
    group.sample_size(10);

    let data: Vec<f32> = (0..DIM0_SIZE * DIM1_SIZE).map(|x| x as f32).collect();

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
                        CompressionType::FpxXor2d,
                        0.1,
                        0.0,
                    )
                    .unwrap();

                black_box(writer.write_data_flat(&data, None, None, None).unwrap());
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
    let data: Vec<f32> = (0..DIM0_SIZE * DIM1_SIZE).map(|x| x as f32).collect();

    group.bench_function("write_om_file", move |b| {
        b.iter_custom(|iters| {
            let mut timer = Timer::new();
            for _i in 0..iters {
                remove_file_if_exists(file);
                timer.start();
                black_box(write_om_file(file, &data));
                timer.stop();
            }
            timer.elapsed()
        })
    });

    group.finish();
}

pub fn benchmark_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read OM file");

    let file = "benchmark.om";
    let file_for_reading = File::open(file).unwrap();
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly).unwrap();
    let reader = OmFileReader::new(Arc::new(read_backend)).unwrap();

    let dim0_read_size = 256;

    group.bench_function("read_om_file", move |b| {
        b.iter(|| {
            let random_x: u64 = rand::thread_rng().gen_range(0..DIM0_SIZE - dim0_read_size);
            let random_y: u64 = rand::thread_rng().gen_range(0..DIM1_SIZE);
            let values = reader
                .read::<f32>(
                    &[random_x..random_x + dim0_read_size, random_y..random_y + 1],
                    None,
                    None,
                )
                .expect("Could not read range");

            assert_eq!(values.len(), dim0_read_size as usize);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_in_memory,
    benchmark_write,
    benchmark_read
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
