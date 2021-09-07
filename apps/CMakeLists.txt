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
# using pkgconfig for uuid
#-----------------------------------------------------------------------------
set(PKG_CONFIG_USE_CMAKE_PREFIX_PATH ON)
find_package(PkgConfig REQUIRED)
pkg_search_module(UUID REQUIRED uuid)



# ##### Note : currently comment off mongodb demo as the rust mongo driver needs to be updated
# add_subdirectory(mongodb_demo)

add_subdirectory(miqs_demo)
# add_subdirectory(index_ess)





