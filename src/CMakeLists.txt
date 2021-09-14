set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")
find_package(UUID REQUIRED)
find_package(Threads REQUIRED)

if(APPLE)
    set(BASE_LIBRARIES m ${UUID_LIBRARIES})
else()
    set(BASE_LIBRARIES m ${UUID_LIBRARIES})
endif()


include_directories(${MIQS_PROJECT_INCLUDE_DIR})






# C_BASE_UTILS
file(GLOB C_BASE_UTILS_SRC ${CMAKE_CURRENT_SOURCE_DIR}/utils/cbase/c_base_stdlib.c)
file(GLOB C_BASE_UTILS_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/utils/cbase/*.h)

# UTILS
file(GLOB UTILS_SRC ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.c)
file(GLOB UTILS_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.h)


# THREAD

file(GLOB UTILS_THREAD_SRC ${CMAKE_CURRENT_SOURCE_DIR}/utils/thread/thpool.c)
file(GLOB UTILS_THREAD_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/utils/thread/thpool.h)

ADD_LIBRARY(miqs_utils_thpool SHARED ${UTILS_THREAD_SRC} ${UTILS_THREAD_HEADER} )
if(THREADS_HAVE_PTHREAD_ARG)
  target_compile_options(miqs_utils_thpool PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(miqs_utils_thpool PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif()


# Profiling
file(GLOB PROFILING_SRC ${CMAKE_CURRENT_SOURCE_DIR}/utils/profile/*.c)
file(GLOB PROFILING_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/utils/profile/*.h)

# FS

file(GLOB FILESYS_SRC ${CMAKE_CURRENT_SOURCE_DIR}/utils/fs/*.c)
file(GLOB FILESYS_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/utils/fs/*.h)


ADD_LIBRARY(miqs_utils SHARED 
                                ${C_BASE_UTILS_SRC} 
                                ${C_BASE_UTILS_HEADER} 
                                ${UTILS_SRC} 
                                ${UTILS_HEADER} 
                                ${PROFILING_SRC} 
                                ${PROFILING_HEADER}
                                ${FILESYS_SRC}
                                ${FILESYS_HEADER})
target_link_libraries(miqs_utils PUBLIC miqs_utils_thpool ${UUID_LIBRARIES})
target_include_directories(miqs_utils PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include> ${UUID_INCLUDE_DIRS})


file(GLOB MIQS_METADATA_SRC           ${CMAKE_CURRENT_SOURCE_DIR}/metadata/miqs_meta*.c)
file(GLOB MIQS_METADATA_HEADER        ${CMAKE_CURRENT_SOURCE_DIR}/metadata/miqs_meta*.h) 
ADD_LIBRARY(miqs_metadata SHARED  ${MIQS_METADATA_SRC} ${MIQS_METADATA_HEADER})
target_link_libraries(miqs_metadata PUBLIC miqs_utils)
target_include_directories(miqs_metadata PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)


# Index

## libhl

file(GLOB LIBHL_SRC ${CMAKE_CURRENT_SOURCE_DIR}/index/libhl/*.c)
file(GLOB LIBHL_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/index/libhl/*.h)

## art

file(GLOB ART_SRC ${CMAKE_CURRENT_SOURCE_DIR}/index/art/*.c)
file(GLOB ART_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/index/art/*.h)

## spi

file(GLOB SPI_SRC ${CMAKE_CURRENT_SOURCE_DIR}/index/spi/*.c)
file(GLOB SPI_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/index/spi/*.h)

# storage

file(GLOB STORAGE_SRC ${CMAKE_CURRENT_SOURCE_DIR}/index/storage/*.c)
file(GLOB STORAGE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/index/storage/*.h)

## top

file(GLOB INDEX_SRC ${CMAKE_CURRENT_SOURCE_DIR}/index/*.c)
file(GLOB INDEX_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/index/*.h)


ADD_LIBRARY(miqs_index SHARED 
                            ${LIBHL_SRC} ${LIBHL_HEADER} 
                            ${ART_SRC} ${ART_HEADER} 
                            ${SPI_SRC} ${SPI_HEADER}
                            ${STORAGE_SRC} ${STORAGE_HEADER}
                            ${INDEX_SRC} ${INDEX_HEADER}
                            )
target_link_libraries(miqs_index PUBLIC miqs_metadata)
target_include_directories(miqs_index PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
# if(THREADS_HAVE_PTHREAD_ARG)
#   target_compile_options(miqs_index PUBLIC "-pthread")
# endif()
# if(CMAKE_THREAD_LIBS_INIT)
#   target_link_libraries(miqs_index PUBLIC c_base_utils foo-threading "${CMAKE_THREAD_LIBS_INIT}")
# endif()

FILE(GLOB HDF5_META_EXTRACTOR_SRC ${CMAKE_CURRENT_SOURCE_DIR}/metadata/hdf5_*.c)
FILE(GLOB HDF5_META_EXTRACTOR_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/metadata/hdf5_*.h)


ADD_LIBRARY(miqs_hdf5_meta_extractor SHARED 
      ${HDF5_META_EXTRACTOR_SRC}
      ${HDF5_META_EXTRACTOR_HEADER}
)
target_link_libraries(miqs_hdf5_meta_extractor PUBLIC m miqs_index ${HDF5_LIBRARIES})
target_include_directories(miqs_hdf5_meta_extractor PUBLIC "${HDF5_INCLUDE_DIRS}"
                                   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)

FILE(GLOB HDF52INDEX_SRC ${CMAKE_CURRENT_SOURCE_DIR}/h52index/*.c)
FILE(GLOB HDF52INDEX_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/h52index/*.h)

ADD_LIBRARY(miqs_hdf52index SHARED ${HDF52INDEX_SRC} ${HDF52INDEX_HEADER})
target_link_libraries(miqs_hdf52index PUBLIC miqs_utils miqs_index miqs_hdf5_meta_extractor)
target_include_directories(miqs_hdf52index PUBLIC "${HDF5_INCLUDE_DIRS}" $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
                              $<INSTALL_INTERFACE:include>)


file(GLOB MIQS_INTERFACE_SRC ${CMAKE_CURRENT_SOURCE_DIR}/*.c)
file(GLOB MIQS_INTERFACE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

ADD_LIBRARY(miqs SHARED ${MIQS_INTERFACE_SRC} ${MIQS_INTERFACE_HEADER})
target_link_libraries(miqs PUBLIC miqs_hdf52index ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
target_include_directories(miqs PRIVATE ${HDF5_INCLUDE_DIRS} ${MPI_C_INCLUDE_DIRS}
                            INTERFACE $<INSTALL_INTERFACE:include>
                          )



set_target_properties(miqs PROPERTIES PUBLIC_HEADER "${MIQS_INTERFACE_HEADER};${MIQS_PROJECT_INCLUDE_DIR}/miqs_config.h")

INSTALL(TARGETS miqs miqs_utils_thpool miqs_utils miqs_metadata miqs_hdf5_meta_extractor miqs_hdf52index miqs_index
        EXPORT libmiqs
        LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/lib
        ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/lib
        RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/bin
        PUBLIC_HEADER DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include
)


INSTALL(FILES ${UTILS_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/utils)
INSTALL(FILES ${C_BASE_UTILS_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/utils/cbase)
INSTALL(FILES ${PROFILING_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/utils/profile)
INSTALL(FILES ${FILESYS_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/utils/fs)
INSTALL(FILES ${UTILS_THREAD_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/utils/thread)
INSTALL(FILES ${HDF52INDEX_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/h52index)
INSTALL(FILES ${MIQS_METADATA_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/metadata)
INSTALL(FILES ${HDF5_META_EXTRACTOR_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/metadata)

INSTALL(FILES ${INDEX_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/index)
INSTALL(FILES ${LIBHL_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/index/libhl)
INSTALL(FILES ${ART_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/index/art)
INSTALL(FILES ${SPI_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/index/spi)
INSTALL(FILES ${STORAGE_HEADER} DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include/index/storage)



INSTALL(EXPORT libmiqs NAMESPACE miqs:: DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake)


# include(${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/cmake/libmiqs.cmake)


# add_executable(hdf5_string_index_test bench/index_ess/HDF5_string_index_test.c bench/index_ess/boss_dataset.h)
# target_link_libraries(hdf5_string_index_test PRIVATE miqs::miqs_index "${MPI_C_LIBRARIES}")
# target_include_directories(hdf5_string_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}" 
#                             PUBLIC $<BUILD_INTERFACE:${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include>)

# add_executable(hdf5_number_index_test bench/index_ess/HDF5_number_index_test.c bench/index_ess/boss_dataset.h)
# target_link_libraries(hdf5_number_index_test PRIVATE miqs::miqs_index "${MPI_C_LIBRARIES}")
# target_include_directories(hdf5_number_index_test PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}"
#                             PUBLIC $<BUILD_INTERFACE:${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include>)


# add_executable(test_concurrent_index bench/concurrent_index/test_concurrent_index.c)
# target_link_libraries(test_concurrent_index PRIVATE foo-threading miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
# target_include_directories(test_concurrent_index PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}" 
#                             PUBLIC $<BUILD_INTERFACE:${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include>)


# add_executable(h5_file_attr_scan bench/index_ess/h5_file_attr_scan.c)
# target_link_libraries(h5_file_attr_scan PRIVATE miqs::miqs ${MPI_mpi_LIBRARY} "${MPI_C_LIBRARIES}")
# target_include_directories(h5_file_attr_scan PRIVATE "${HDF5_INCLUDE_DIRS}" "${MPI_C_INCLUDE_DIRS}"
#                           PUBLIC $<BUILD_INTERFACE:${MIQS_PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/include>)

# INSTALL(TARGETS hdf5_string_index_test hdf5_number_index_test test_concurrent_index h5_file_attr_scan
#         EXPORT libmiqs_bench
#         LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/bench/lib
#         ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/bench/lib
#         RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/bench/bin
#         PUBLIC_HEADER DESTINATION ${PROJECT_SOURCE_DIR}/target/${MIQS_BUILD_MODE}/bench/include
# )