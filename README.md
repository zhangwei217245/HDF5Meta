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

## Ensure Compilation Environment
On Cori, run the following command:


```bash
module unload PrgEnv-intel
module load PrgEnv-gnu/6.0.5
module load cray-hdf5-parallel/1.10.5.2
module load cmake/3.14.4
module load gcc
module load openmpi/3.1.3
module load llvm/10.0.0
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
cd ${MIQS_HOME}/src/lib/rust_mongo_bench
cargo build --release
```

### Compile the Json-C library

```bash
cd ${MIQS_HOME}/src/lib/json-c
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=./json_c_lib ../
make -j 14 && make install
```

### Compile the MIQS software prototype

On Cori supercomputer, you can run the following to make sure dynamic linking is enabled:

```bash
export CRAYPE_LINK_TYPE=dynamic 
```

For other platforms, you can enable dynamic linking using platform-specific methods. 

```bash
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DCMAKE_BUILD_TYPE:STRING=Debug -H${MIQS_HOME} -B${MIQS_HOME}/build -G "Unix Makefiles"
cmake --build ${MIQS_HOME}/build --config Debug --target all -- -j 14
```

You can change your CMake build type from `Debug` to one of the following

* Release
* MinSizeRel
* RelWithDebInfo
* BetaTest
* RelWithDebug



