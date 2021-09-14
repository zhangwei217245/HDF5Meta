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

To compile MIQS software, you need the following dependencies:

```bash
GCC
HDF5 libraries 1.10+
Cmake 3.14+
```

On Linux or MacOS, check out the manual of your software package management tools for installing the above dependencies.


On Cori, run the following command to ensure the compilation environment:


```bash
module unload PrgEnv-intel
module load PrgEnv-gnu/6.0.5
module load cray-hdf5-parallel/1.10.5.2
module load cmake/3.14.4
module load gcc
module load openmpi/3.1.3
module load llvm/10.0.0
```

On nocona cluster at HPCC@TTU, please load the following modules
```
module load git
module load gcc
module load cmake
module load openmpi
module load hdf5
```

### Compile and install the MIQS library

On Cori supercomputer, you can run the following to make sure dynamic linking is enabled:

```bash
export CRAYPE_LINK_TYPE=dynamic 
```

For other platforms, you can enable dynamic linking using platform-specific methods. 

```bash
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DCMAKE_BUILD_TYPE:STRING=Debug -H${MIQS_HOME} -B${MIQS_HOME}/build -G "Unix Makefiles"
cmake --build ${MIQS_HOME}/build --config Debug --target install -- -j 14
```

You can change your CMake build type from `Debug` to one of the following

* Release
* MinSizeRel
* RelWithDebInfo
* BetaTest
* RelWithDebug

If your build type is Debug, the library will be installed into `${MIQS_HOME}/target/debug/`, otherwise, the library will be installed into `${MIQS_HOME}/target/release/`

### Compile and install the MIQS demo program

Compile the rust_mongo_bench library first

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
cd ${MIQS_HOME}/extern/rust_mongo_bench
cargo build 
```
if you choose the build type to be Release, just add `--release` after `cargo build` command.

### Compile the Json-C library

```bash
cd ${MIQS_HOME}/extern/json-c 
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=./json_c_lib ../
make -j 14 && make install
```

### Build MIQS demo apps:

```bash
cd ${MIQS_HOME}/apps/
mkdir build
cd build

cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DCMAKE_BUILD_TYPE:STRING=Debug -H${MIQS_HOME}/apps -B${MIQS_HOME}/apps/build -G "Unix Makefiles"
cmake --build ${MIQS_HOME}/apps/build --config Debug --target install -- -j 14
```


### Running MIQS demo apps:

Put the following code in your job script, before executing the actual executable.

```bash
cd <path/to/miqs_home>
MIQS_HOME=`pwd`
LD_LIBRARY_PATH=${MIQS_HOME}/target/debug/lib/:${LD_LIBRARY_PATH}
```

Change `debug` here to `release` according to your release type.