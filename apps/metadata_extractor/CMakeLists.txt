include(${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake) # include the exported cmake file to import all targets. 

# message(STATUS "abc ${MIQS_INCLUDE_DIRS}")


add_executable (metadata_extractor ${CMAKE_CURRENT_SOURCE_DIR}/metadata_extractor.c)
target_link_libraries(metadata_extractor PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(metadata_extractor PRIVATE  
                                            "${HDF5_INCLUDE_DIRS}" 
                                            "${MPI_C_INCLUDE_DIRS}"
                                            "${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include"
)

INSTALL(TARGETS metadata_extractor
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)