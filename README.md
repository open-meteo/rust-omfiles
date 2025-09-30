# rust-omfiles

[![codecov](https://codecov.io/github/open-meteo/rust-omfiles/graph/badge.svg?token=ZCOQN3ZKHP)](https://codecov.io/github/open-meteo/rust-omfiles)
[![Test](https://github.com/open-meteo/rust-omfiles/actions/workflows/tests.yml/badge.svg)](https://github.com/open-meteo/rust-omfiles/actions/workflows/tests.yml)
[![GitHub license](https://img.shields.io/github/license/open-meteo/rust-omfiles)](https://github.com/open-meteo/rust-omfiles/blob/main/LICENSE)

Rust reader and writer implementation for the `om` file format.

The file format is documented in the [open-meteo/om-file-format](https://github.com/open-meteo/om-file-format/blob/main/README.md) repository.

## Development

```bash
cargo test
# docs
RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features
```

## Reading files

Assuming the file `data.om` directly contains a floating point array with 3 dimensions

```rust
use omfiles::reader::OmFileReader;

let file = "data.om";
let reader = OmFileReader::from_file(file).expect(&format!("Failed to open file: {}", file));
// print name, data type and number of children
println!("Name: {}", reader.name());
println!("Number of children: {}", reader.number_of_children());
println!("Data type: {:?}", reader.data_type());

// if this variable is an array, we can cast it as such
let array_reader = reader.expect_array().expect("Failed to cast to array");

// Print some metadata information about the array
println!("Dimensions: {:?}", array_reader.get_dimensions());
println!("Chunk size: {:?}", array_reader.get_chunk_dimensions());
println!("Compression type: {:?}", array_reader.compression());
println!("Scale factor: {}", array_reader.scale_factor());
println!("Add offset: {}", array_reader.add_offset());

// read root variable into a (dynamical) 3D array
let data = array_reader
    .read::<f32>(&[50u64..51, 20..21, 10..100])
    .expect("Failed to read data");
println!("Data: {:?}", data);
```

## Features

- [x] Read data from `om` v2 and v3 files
- [x] Write data to `om` v3 files
- [x] Integrates with the [`ndarray`](https://github.com/rust-ndarray/ndarray) crate for data representation
- [x] Tested on Linux, MacOS and Windows in CI
