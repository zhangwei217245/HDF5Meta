#!/bin/bash


# git submodule init
# git submodule update


# MIQS_HOME=`pwd`

# RUSTUP_PATH=`which rustup`

# if [[ "${RUSTUP_PATH}x" == "x" ]]; then
#     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# fi

# rustup update

# cd ${MIQS_HOME}/src/lib/rust_mongo_bench
# cargo build --release


# cd ${MIQS_HOME}/src/lib/json-c
# mkdir build
# cd build
# cmake -DCMAKE_INSTALL_PREFIX=./json_c_lib ../
# make && make install


# cd ${MIQS_HOME}
# export CRAYPE_LINK_TYPE=dynamic 
# cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -DCMAKE_BUILD_TYPE:STRING=Debug -H${MIQS_HOME} -B${MIQS_HOME}/build -G "Unix Makefiles"
# cmake --build ${MIQS_HOME}/build --config Debug --target all -- -j 14


