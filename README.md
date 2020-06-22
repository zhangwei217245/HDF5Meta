# MIQS - Metadata Indexing and Querying Service for Self-describing Data Formats

## Installation

```bash
git clone 
```

```bash
git submodule init
git submodule update
```

## Compile the rust_mongo_bench library first

### Make sure Rust is installed

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### If Rust is already installed, make sure it is updated

```bash
rustup update
```

### Compile the Rust-based MongoDB test library

```bash
cd lib/rust_mongo_bench
cargo build --release
```



