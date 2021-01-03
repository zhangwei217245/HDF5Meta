cmake_minimum_required(VERSION 3.0)

# Setup cmake policies.
foreach(p
    CMP0012
    CMP0013
    CMP0014
    CMP0022 # CMake 2.8.12
    CMP0025 # CMake 3.0
    CMP0053 # CMake 3.1
    CMP0054 # CMake 3.1
    CMP0074 # CMake 3.4
  )
  if(POLICY ${p})
    cmake_policy(SET ${p} NEW)
  endif()
endforeach()


string(TOLOWER ${CMAKE_BUILD_TYPE} _type)
if (_type STREQUAL debug)
  set(MIQS_BUILD_MODE "debug")
else()
  set(MIQS_BUILD_MODE "release")
endif()


if(APPLE)
    set(SHARED_LIB_SUFFIX dylib)
else()
    set(SHARED_LIB_SUFFIX so)
endif()

PROJECT(MIQS_DEMO)

set(MIQS_PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}/..")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${MIQS_PROJECT_SOURCE_DIR}/cmake")

#-----------------------------------------------------------------------------
# hdf5
#-----------------------------------------------------------------------------

find_package(HDF5 REQUIRED)

#-----------------------------------------------------------------------------
# MPI option
#-----------------------------------------------------------------------------
option(MIQS_MPI_ENABLED "Enable MPI on MIQS RPC module." ON)
if(MIQS_MPI_ENABLED)
  find_package(MPI REQUIRED)
  if(MPI_FOUND)
    set(MIQS_MPI_ENABLED 1)
  else()
    set(MIQS_MPI_ENABLED 0)
  endif()
endif()


#-----------------------------------------------------------------------------
# using pkgconfig for json-c and uuid
#-----------------------------------------------------------------------------
set(PKG_CONFIG_USE_CMAKE_PREFIX_PATH ON)
set(CMAKE_PREFIX_PATH "${MIQS_PROJECT_SOURCE_DIR}/extern/json-c/build/json_c_lib;${CMAKE_PREFIX_PATH}")
find_package(PkgConfig REQUIRED)
pkg_search_module(UUID REQUIRED uuid)

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

ADD_LIBRARY(hdf52json SHARED mongodb_demo/h52json/hdf52json.c mongodb_demo/h52json/hdf52json.h)
target_link_libraries(hdf52json PUBLIC  miqs::miqs_hdf5_meta_extractor ${JSON-C_LIBRARIES}) 
target_include_directories(hdf52json PUBLIC "${JSON-C_INCLUDE_DIRS}" 
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include")

#-----------------------------------------------------------------------------
# build hdf5_set_2_mongo
#-----------------------------------------------------------------------------
add_executable (hdf5_set_2_mongo mongodb_demo/HDF5_set_2_mongo.c)
target_link_libraries(hdf5_set_2_mongo PRIVATE 
    hdf52json "${MPI_C_LIBRARIES}" ${LIB_RUST_MONGO})
target_include_directories(hdf5_set_2_mongo PRIVATE "${HDF5_INCLUDE_DIRS}" 
                                    "${MPI_C_INCLUDE_DIRS}" 
                                    "mongodb_test/h52json" 
                                    "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include")


#-----------------------------------------------------------------------------
# build hdf5_set_2_index
#-----------------------------------------------------------------------------
include(${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake) # include the exported cmake file to import all targets. 

# message(STATUS "abc ${MIQS_INCLUDE_DIRS}")


add_executable (hdf5_set_2_index ${CMAKE_CURRENT_SOURCE_DIR}/miqs_demo/HDF5_set_2_index.c)
target_link_libraries(hdf5_set_2_index PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_index PRIVATE  
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MPI_C_INCLUDE_DIRS}"
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include"
)

INSTALL(TARGETS hdf5_set_2_index  hdf5_set_2_mongo  hdf52json 
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)





