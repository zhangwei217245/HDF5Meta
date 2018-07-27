set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/modules")

find_package(JSON-C REQUIRED)

ADD_LIBRARY(c_base_utils SHARED base_stdlib.h c95_stdlib.h c99_stdlib.h c11_stdlib.h)
ADD_LIBRARY(string_utils SHARED string_utils.c string_utils.h)
TARGET_LINK_LIBRARIES(string_utils c_base_utils)

ADD_LIBRARY(query_utils SHARED query_utils.c query_utils.h)
TARGET_LINK_LIBRARIES(query_utils string_utils)

ADD_LIBRARY(timer_utils SHARED timer_utils.c timer_utils.h)

ADD_LIBRARY(thpool SHARED thpool.c thpool.h)

ADD_LIBRARY(indexds art.c art.h hashset.c hashset.h vector.c vector.h bplustree.c bplustree.h)

ADD_LIBRARY(hdf5_meta hdf5_meta.c hdf5_meta.h)

ADD_LIBRARY(new_hdf5_meta hdf5_meta.c hdf5_meta.h hdf52json.c hdf52json.h)

include_directories(${JSON-C_INCLUDE_DIRS})
include_directories (${PROJECT_SOURCE_DIR}/lib)
link_directories (${PROJECT_BINARY_DIR}/lib)
target_include_directories(hdf5_meta PRIVATE "${JSON-C_INCLUDE_DIRS}")
target_link_libraries(hdf5_meta PRIVATE "${JSON-C_LIBRARIES}" m query_utils timer_utils thpool indexds c_base_utils)

target_link_libraries(new_hdf5_meta PRIVATE "${JSON-C_LIBRARIES}" m query_utils timer_utils thpool indexds c_base_utils)
