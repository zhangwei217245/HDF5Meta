# set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/modules")
find_package(MPI REQUIRED)
# find_package(HDF5 REQUIRED)
# find_package(JSON-C REQUIRED)

if(APPLE)
    set(LIB_RUST_MONGO ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.dylib)
else()
    set(LIB_RUST_MONGO ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so)
endif()

# # The "hello_mongoc.c" sample program is shared among four tests.
# add_executable (hello_mongoc hello_mongoc.c)
# #target_include_directories (hello_mongoc PRIVATE "${MONGOC_INCLUDE_DIRS}")
# target_link_libraries (hello_mongoc PRIVATE hdf5_meta ${LIB_RUST_MONGO})
# target_include_directories(hello_mongoc PRIVATE "${JSON-C_INCLUDE_DIRS}" "${HDF5_INCLUDE_DIRS}")
# # #target_compile_definitions (hello_mongoc PRIVATE "${MONGOC_DEFINITIONS}")

# add_executable (test_unindexed_hdf5 test_unindexed_hdf5.c)
# target_link_libraries(test_unindexed_hdf5 PRIVATE 
#     hdf5_meta)

# add_executable (test_bpt_hdf5 test_bpt_hdf5.c)
# target_link_libraries(test_bpt_hdf5 PRIVATE 
#     hdf5_meta timer_utils indexds)


# add_executable (test_dir_scan test_dir_scan.c)
# target_link_libraries(test_dir_scan PRIVATE 
#     new_hdf5_meta ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so)


# build hdf5_set_2_mongo
add_executable (hdf5_set_2_mongo HDF5_set_2_mongo.c)
target_link_libraries(hdf5_set_2_mongo PRIVATE 
    hdf52json ${LIB_RUST_MONGO} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_mongo PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

# build hdf5_set_2_index
add_executable (hdf5_set_2_index HDF5_set_2_index.c)
target_link_libraries(hdf5_set_2_index PRIVATE hdf52index miqs_index index_spi ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_index PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")


add_executable(hdf5_string_index_test HDF5_string_index_test.c boss_dataset.h)
target_link_libraries(hdf5_string_index_test PRIVATE index_spi "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_string_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

add_executable(hdf5_number_index_test HDF5_number_index_test.c boss_dataset.h)
target_link_libraries(hdf5_number_index_test PRIVATE index_spi "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_number_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}")

#ADD_EXECUTABLE(main main.c dira/printa.c dirb/printb.c)
#INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
#LINK_DIRECTORIES(${PROJECT_BINARY_DIR}/lib)
#TARGET_LINK_LIBRARIES(main query_utils timer_utils)
