#-----------------------------------------------------------------------------
# build index_ess executables
#-----------------------------------------------------------------------------
include(${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake) # include the exported cmake file to import all targets. 

# message(STATUS "abc ${MIQS_INCLUDE_DIRS}")


add_executable (HDF5_number_index_test ${CMAKE_CURRENT_SOURCE_DIR}/boss_dataset.h ${CMAKE_CURRENT_SOURCE_DIR}/HDF5_number_index_test.c)
target_link_libraries(HDF5_number_index_test PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(HDF5_number_index_test PRIVATE  
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MPI_C_INCLUDE_DIRS}"
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include"
)

add_executable (HDF5_string_index_test ${CMAKE_CURRENT_SOURCE_DIR}/boss_dataset.h ${CMAKE_CURRENT_SOURCE_DIR}/HDF5_string_index_test.c)
target_link_libraries(HDF5_string_index_test PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(HDF5_string_index_test PRIVATE  
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MPI_C_INCLUDE_DIRS}"
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include"
)

INSTALL(TARGETS HDF5_string_index_test HDF5_number_index_test
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)