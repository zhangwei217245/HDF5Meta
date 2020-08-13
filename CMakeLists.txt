cmake_minimum_required(VERSION 2.8)

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

PROJECT(MIQS)


#-----------------------------------------------------------------------------
# dynamic linking
#-----------------------------------------------------------------------------
set(BUILD_SHARED_LIBS ON)
SET(ENV(CRAYPE_LINK_TYPE) "dynamic")
SET(CMAKE_C_FLAGS -dynamic)
SET(LIB_PATH ${PROJECT_BINARY_DIR}/lib)


#-----------------------------------------------------------------------------
# MPI option
#-----------------------------------------------------------------------------
option(MIQS_MPI_ENABLED "Enable MPI on MIQS RPC module." OFF)
if(MIQS_MPI_ENABLED)
  find_package(MPI REQUIRED)
  if(MPI_FOUND)
    set(MIQS_MPI_ENABLED 1)
  endif()
endif()


#-----------------------------------------------------------------------------
# pthread option
#-----------------------------------------------------------------------------
set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#-----------------------------------------------------------------------------
# HDF5
#-----------------------------------------------------------------------------
find_package(HDF5 REQUIRED)

#-----------------------------------------------------------------------------
# using pkgconfig for json-c and uuid
#-----------------------------------------------------------------------------
set(PKG_CONFIG_USE_CMAKE_PREFIX_PATH ON)
set(CMAKE_PREFIX_PATH "${PROJECT_SOURCE_DIR}/extern/json-c/build/json_c_lib/lib/pkgconfig;${CMAKE_PREFIX_PATH}")
find_package(PkgConfig REQUIRED)
pkg_search_module(UUID REQUIRED uuid)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake")
find_package(JSON-C REQUIRED)

# pkg_check_modules(JSON-C REQUIRED IMPORTED_TARGET GLOBAL json-c)





ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(tests)
ADD_SUBDIRECTORY(docs)
ADD_SUBDIRECTORY(scripts)