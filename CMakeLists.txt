cmake_minimum_required(VERSION 2.8)
PROJECT(HDF5Meta)

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

