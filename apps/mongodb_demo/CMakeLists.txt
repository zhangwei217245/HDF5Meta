#-----------------------------------------------------------------------------
# using pkgconfig for json-c 
#-----------------------------------------------------------------------------

set(CMAKE_PREFIX_PATH "${MIQS_PROJECT_SOURCE_DIR}/extern/json-c/build/json_c_lib;${CMAKE_PREFIX_PATH}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${MIQS_PROJECT_SOURCE_DIR}/cmake")
find_package(JSON-C REQUIRED)



#-----------------------------------------------------------------------------
# lib_rust_mongo
#-----------------------------------------------------------------------------


set(LIB_RUST_MONGO ${MIQS_PROJECT_SOURCE_DIR}/extern/rust_mongo_bench/target/${MIQS_BUILD_MODE}/librust_mongo_bench.${SHARED_LIB_SUFFIX})
# message(STATUS "abc ${LIB_RUST_MONGO}")

# # The "test_mongoc.c" sample program is shared among four tests.
# add_executable (test_mongoc test_mongoc.c)
# #target_include_directories (test_mongoc PRIVATE "${MONGOC_INCLUDE_DIRS}")
# target_link_libraries (test_mongoc PRIVATE hdf5_meta ${LIB_RUST_MONGO})
# target_include_directories(test_mongoc PRIVATE "${JSON-C_INCLUDE_DIRS}" "${HDF5_INCLUDE_DIRS}")
# # #target_compile_definitions (test_mongoc PRIVATE "${MONGOC_DEFINITIONS}")

#-----------------------------------------------------------------------------
# hdf52json
#-----------------------------------------------------------------------------

ADD_LIBRARY(hdf52json SHARED h52json/hdf52json.c h52json/hdf52json.h)
target_link_libraries(hdf52json PUBLIC  miqs::miqs_hdf5_meta_extractor ${JSON-C_LIBRARIES}) 
target_include_directories(hdf52json PUBLIC "${JSON-C_INCLUDE_DIRS}" 
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include")

#-----------------------------------------------------------------------------
# build hdf5_set_2_mongo
#-----------------------------------------------------------------------------
add_executable (hdf5_set_2_mongo HDF5_set_2_mongo.c)
target_link_libraries(hdf5_set_2_mongo PRIVATE 
    hdf52json "${MPI_C_LIBRARIES}" ${LIB_RUST_MONGO})
target_include_directories(hdf5_set_2_mongo PRIVATE "${HDF5_INCLUDE_DIRS}" 
                                    "${MPI_C_INCLUDE_DIRS}" 
                                    "mongodb_test/h52json" 
                                    "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include")

INSTALL(TARGETS hdf5_set_2_mongo  hdf52json 
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)