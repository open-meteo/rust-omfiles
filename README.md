# rust-omfiles

[![codecov](https://codecov.io/github/open-meteo/rust-omfiles/graph/badge.svg?token=ZCOQN3ZKHP)](https://codecov.io/github/open-meteo/rust-omfiles)
[![Test](https://github.com/open-meteo/rust-omfiles/actions/workflows/tests.yml/badge.svg)](https://github.com/open-meteo/rust-omfiles/actions/workflows/tests.yml)
[![GitHub license](https://img.shields.io/github/license/open-meteo/rust-omfiles)](https://github.com/open-meteo/rust-omfiles/blob/main/LICENSE)

Rust reader and writer implementation for the `om` file format.

The file format is documented in the [open-meteo/om-file-format](https://github.com/open-meteo/om-file-format/blob/main/README.md) repository.

## Development

```bash
cargo test
```

## Reading files

Assuming the file `data.om` directly contains a floating point array with 3 dimensions

```rust
use omfiles::io::reader::OmFileReader;

let file = "data.om";
let reader = OmFileReader::from_file(file).expect(format!("Failed to open file: {}", file).as_str());
// read root variable into a (dynamical) 3D array
let data = reader.read::<f32>(&[50u64..51, 20..21, 10..200], None, None).expect("Failed to read data");

// Metadata information
let dimensions = reader.get_dimensions();
let chunk_size = reader.get_chunk_dimensions();
println!("Dimensions: {:?}", dimensions);
println!("Chunk size: {:?}", chunk_size);
```

## Features

- [x] Read data from `om` v2 and v3 files
- [x] Write data to `om` v3 files
- [x] Integrates with the [`ndarray`](https://github.com/rust-ndarray/ndarray) crate for data representation
- [x] Tested on Linux, MacOS and Windows in CI
