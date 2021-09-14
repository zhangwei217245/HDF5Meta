include(${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake)
#-----------------------------------------------------------------------------
# pthread
#-----------------------------------------------------------------------------
set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads REQUIRED)
if(CMAKE_USE_PTHREADS_INIT)
    set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-pthread")
endif()

add_executable(test_concurrent_index test_concurrent_index.c)
target_link_libraries(test_concurrent_index ${CMAKE_THREAD_LIBS_INIT} miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(test_concurrent_index PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}" 
                            PUBLIC $<BUILD_INTERFACE:${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include>)


INSTALL(TARGETS test_concurrent_index 
                    RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
                    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
                    ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib)