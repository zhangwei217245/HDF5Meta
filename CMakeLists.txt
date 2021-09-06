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




# Set a consistent MACOSX_RPATH default across all CMake versions.
# When CMake 2.8.12 is required, change this default to 1.
# When CMake 3.0.0 is required, remove this block (see CMP0042).
if (APPLE) 
  if(NOT DEFINED CMAKE_MACOSX_RPATH)
    set(CMAKE_MACOSX_RPATH 0)
  endif()
endif()

PROJECT(MIQS VERSION 0.6.0)


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


#-----------------------------------------------------------------------------
# dynamic linking
#-----------------------------------------------------------------------------
set(BUILD_SHARED_LIBS ON)
SET(ENV(CRAYPE_LINK_TYPE) "dynamic")
SET(CMAKE_C_FLAGS "-dynamic")
SET(LIB_PATH ${PROJECT_BINARY_DIR}/lib)


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
# pthread option
#-----------------------------------------------------------------------------
set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(CMAKE_THREAD_PREFER_PTHREAD ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# interface library for threading
add_library(foo-threading INTERFACE)

if(THREADS_FOUND)
    if(THREADS_HAVE_PTHREAD_ARG)
        target_compile_options(foo-threading INTERFACE
            $<$<COMPILE_LANGUAGE:C>:-pthread>
            $<$<COMPILE_LANGUAGE:CXX>:-pthread>)
    endif()
    target_link_libraries(foo-threading INTERFACE ${CMAKE_THREAD_LIBS_INIT})
endif()

#-----------------------------------------------------------------------------
# MIQS concurrency level option
#-----------------------------------------------------------------------------
option(MIQS_INDEX_PARALLELIZATION "Enable parallelized metadata index." ON)
set(MIQS_INDEX_PARALLELIZATION OFF)
if (MIQS_INDEX_PARALLELIZATION)
  set(MIQS_PARALLELED_INDEX ON)
else()
  set(MIQS_PARALLELED_INDEX OFF)
endif()

option(LIBHL_THREAD_SAFE "Enable thread safe for libhl." ON)
if (LIBHL_THREAD_SAFE)
  set(THREAD_SAFE ON)
else()
  set(THREAD_SAFE OFF)
endif()

#-----------------------------------------------------------------------------
# HDF5
#-----------------------------------------------------------------------------
find_package(HDF5 REQUIRED)

# message(${HDF5_VERSION})
string(REGEX MATCH "^[1]\\.(10|12)" HDF5_MAJOR_MINOR ${HDF5_VERSION})
string(COMPARE EQUAL ${HDF5_MAJOR_MINOR} "1.12" NEED_HDF5_COMPATIBLE_API)

if(NEED_HDF5_COMPATIBLE_API)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DH5_USE_110_API")
endif()


SET(MIQS_PROJECT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)



CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/miqs_config.h.cmake ${MIQS_PROJECT_INCLUDE_DIR}/miqs_config.h @ONLY)

ADD_SUBDIRECTORY(src)
# ADD_SUBDIRECTORY(tests)

# ADD_SUBDIRECTORY(apps)

# ADD_SUBDIRECTORY(docs)
# ADD_SUBDIRECTORY(scripts)

# message(STATUS "Build documentation open")
# include(${PROJECT_SOURCE_DIR}/cmake/build_doxygen.cmake)
# build_doxygen()