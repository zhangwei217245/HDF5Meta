
# Specify the minimum version you require.
find_package (libmongoc-1.0 1.7 REQUIRED)

message ("--   mongoc found version \"${MONGOC_VERSION}\"")
message ("--   mongoc include path \"${MONGOC_INCLUDE_DIRS}\"")
message ("--   mongoc libraries \"${MONGOC_LIBRARIES}\"")

find_package(PkgConfig)
pkg_check_modules(JSON_C REQUIRED json-c)


# The "hello_mongoc.c" sample program is shared among four tests.
add_executable (hello_mongoc hello_mongoc.c)
target_include_directories (hello_mongoc PRIVATE "${MONGOC_INCLUDE_DIRS}")
target_link_libraries (hello_mongoc PRIVATE "${MONGOC_LIBRARIES}" 
    ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so     
    )
target_compile_definitions (hello_mongoc PRIVATE "${MONGOC_DEFINITIONS}")

add_executable (hdf5_reader HDF5_reader.c)
include_directories (${PROJECT_SOURCE_DIR}/lib
                    ${JSON_C_INCLUDE_DIRS})
link_directories (${PROJECT_BINARY_DIR}/lib)
target_include_directories(hdf5_reader PRIVATE "${JSON_C_INCLUDE_DIRS}")
target_link_libraries(hdf5_reader PRIVATE "${JSON_C_LIBRARIES}" query_utils timer_utils thpool indexds hdf5_meta)
target_compile_definitions (hdf5_reader PRIVATE "${JSON_C_DEFINITIONS}")
target_compile_options(hdf5_reader PRIVATE ${JSON_C_CFLAGS} ${JSON_C_LIBS})

#ADD_EXECUTABLE(main main.c dira/printa.c dirb/printb.c)
#INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
#LINK_DIRECTORIES(${PROJECT_BINARY_DIR}/lib)
#TARGET_LINK_LIBRARIES(main query_utils timer_utils)
