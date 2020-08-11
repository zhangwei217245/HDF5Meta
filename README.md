# MIQS - Metadata Indexing and Querying Service for Self-describing Data Formats

## Installation

```bash
git clone git@bitbucket.org:berkeleylab/miqs.git MIQS
cd MIQS
export MIQS_HOME=`pwd`
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
cd ${MIQS_HOME}/lib/rust_mongo_bench
cargo build --release
```

### Compile the Json-C library

```bash
cd ${MIQS_HOME}/lib/json-c
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=./json_c_lib ../
make && make install
```

### Compile the MIQS software prototype

```bash
cd ${MIQS_HOME}
mkdir build
cmake --build ${MIQS_HOME}/build --config Debug --target all -- -j 14
```

You can change your CMake build type from `Debug` to one of the following

* Release
* MinSizeRel
* RelWithDebInfo
* BetaTest
* RelWithDebug



