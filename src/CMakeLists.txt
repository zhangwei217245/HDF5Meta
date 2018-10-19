
# The "hello_mongoc.c" sample program is shared among four tests.
add_executable (hello_mongoc hello_mongoc.c)
#target_include_directories (hello_mongoc PRIVATE "${MONGOC_INCLUDE_DIRS}")
target_link_libraries (hello_mongoc PRIVATE hdf5_meta 
    ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so     
    )
#target_compile_definitions (hello_mongoc PRIVATE "${MONGOC_DEFINITIONS}")

add_executable (test_unindexed_hdf5 test_unindexed_hdf5.c)
target_link_libraries(test_unindexed_hdf5 PRIVATE 
    hdf5_meta)

add_executable (test_bpt_hdf5 test_bpt_hdf5.c)
target_link_libraries(test_bpt_hdf5 PRIVATE 
    hdf5_meta timer_utils indexds)


add_executable (test_dir_scan test_dir_scan.c)
target_link_libraries(test_dir_scan PRIVATE 
    new_hdf5_meta ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so)


# build hdf5_set_2_mongo
add_executable (hdf5_set_2_mongo HDF5_set_2_mongo.c)
target_link_libraries(hdf5_set_2_mongo PRIVATE 
    hdf52json ${PROJECT_SOURCE_DIR}/lib/rust_mongo_bench/target/release/librust_mongo_bench.so)

# build hdf5_set_2_index
add_executable (hdf5_set_2_index HDF5_set_2_index.c)
target_link_libraries(hdf5_set_2_index PRIVATE hdf52index)



#ADD_EXECUTABLE(main main.c dira/printa.c dirb/printb.c)
#INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/lib)
#LINK_DIRECTORIES(${PROJECT_BINARY_DIR}/lib)
#TARGET_LINK_LIBRARIES(main query_utils timer_utils)
