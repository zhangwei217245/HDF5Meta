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

find_package(HDF5 REQUIRED)


set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/lib/json-c/build/json_c_lib/lib/pkgconfig")


#-----------------------------------------------------------------------------
# MPI option
#-----------------------------------------------------------------------------
option(MIQS_RPC_MPI_ENABLED "Enable MPI on MIQS RPC module." OFF)
if(MIQS_RPC_MPI_ENABLED)
  find_package(MPI)
  if(MPI_FOUND)
    set(MIQS_RPC_MPI_ENABLED 1)
  endif()
endif()


#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${PROJECT_BINARY_DIR}/config.h)


SET(CMAKE_C_FLAGS -dynamic)
SET(LIB_PATH ${PROJECT_BINARY_DIR}/lib)

ADD_SUBDIRECTORY(lib)
ADD_SUBDIRECTORY(src bin)

