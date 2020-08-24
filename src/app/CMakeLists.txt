# set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/modules")

# find_package(HDF5 REQUIRED)
# find_package(JSON-C REQUIRED)

if(APPLE)
    set(LIB_RUST_MONGO ${PROJECT_SOURCE_DIR}/src/lib/rust_mongo_bench/target/release/librust_mongo_bench.dylib)
else()
    set(LIB_RUST_MONGO ${PROJECT_SOURCE_DIR}/src/lib/rust_mongo_bench/target/release/librust_mongo_bench.so)
endif()

# # The "test_mongoc.c" sample program is shared among four tests.
# add_executable (test_mongoc test_mongoc.c)
# #target_include_directories (test_mongoc PRIVATE "${MONGOC_INCLUDE_DIRS}")
# target_link_libraries (test_mongoc PRIVATE hdf5_meta ${LIB_RUST_MONGO})
# target_include_directories(test_mongoc PRIVATE "${JSON-C_INCLUDE_DIRS}" "${HDF5_INCLUDE_DIRS}")
# # #target_compile_definitions (test_mongoc PRIVATE "${MONGOC_DEFINITIONS}")

# build hdf5_set_2_mongo
add_executable (hdf5_set_2_mongo HDF5_set_2_mongo.c)
target_link_libraries(hdf5_set_2_mongo PRIVATE 
    hdf52json ${LIB_RUST_MONGO} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_mongo PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

# build hdf5_set_2_index
add_executable (hdf5_set_2_index HDF5_set_2_index.c)
target_link_libraries(hdf5_set_2_index PRIVATE hdf52index index_spi ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_index PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

add_executable(test_concurrent_index test_concurrent_index.c boss_dataset.h)
target_link_libraries(test_concurrent_index PRIVATE hdf52index index_spi foo-threading ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(test_concurrent_index PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")


add_executable(hdf5_string_index_test HDF5_string_index_test.c boss_dataset.h)
target_link_libraries(hdf5_string_index_test PRIVATE index_spi "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_string_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}" "${}")

add_executable(hdf5_number_index_test HDF5_number_index_test.c boss_dataset.h)
target_link_libraries(hdf5_number_index_test PRIVATE index_spi "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_number_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

