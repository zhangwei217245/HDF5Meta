#-----------------------------------------------------------------------------
# build hdf5_set_2_index
#-----------------------------------------------------------------------------
include(${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake) # include the exported cmake file to import all targets. 

# message(STATUS "abc ${MIQS_INCLUDE_DIRS}")


add_executable (hdf5_set_2_index ${CMAKE_CURRENT_SOURCE_DIR}/HDF5_set_2_index.c)
target_link_libraries(hdf5_set_2_index PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(hdf5_set_2_index PRIVATE  
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MPI_C_INCLUDE_DIRS}"
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include"
)

INSTALL(TARGETS hdf5_set_2_index
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)