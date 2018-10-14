set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/modules")

find_package(JSON-C REQUIRED)

ADD_LIBRARY(c_base_utils SHARED include/c_base_stdlib.c include/base_stdlib.h include/c95_stdlib.h include/c99_stdlib.h include/c11_stdlib.h)
ADD_LIBRARY(string_utils SHARED utils/string_utils.c utils/string_utils.h)
TARGET_LINK_LIBRARIES(string_utils c_base_utils)

ADD_LIBRARY(query_utils SHARED utils/query_utils.c utils/query_utils.h)
TARGET_LINK_LIBRARIES(query_utils string_utils)

ADD_LIBRARY(timer_utils SHARED utils/timer_utils.c utils/timer_utils.h)

ADD_LIBRARY(thpool SHARED thread/thpool.c thread/thpool.h)

ADD_LIBRARY(indexds SHARED ds/art.c ds/art.h ds/hashset.c ds/hashset.h ds/vector.c ds/vector.h ds/bplustree.c ds/bplustree.h)

ADD_LIBRARY(fs_utils SHARED fs/fs_ops.c fs/fs_ops.h)

TARGET_LINK_LIBRARIES(fs_utils string_utils c_base_utils)

ADD_LIBRARY(hdf5_meta hdf5_meta.c hdf5_meta.h)

include_directories(${JSON-C_INCLUDE_DIRS})
include_directories (${PROJECT_SOURCE_DIR}/lib)
link_directories (${PROJECT_BINARY_DIR}/lib)
target_include_directories(hdf5_meta PRIVATE "${JSON-C_INCLUDE_DIRS}")
target_link_libraries(hdf5_meta PRIVATE "${JSON-C_LIBRARIES}" m  query_utils timer_utils thpool indexds fs_utils c_base_utils)


ADD_LIBRARY(new_hdf5_meta hdf5/new_hdf5_meta.c hdf5/new_hdf5_meta.h)
target_link_libraries(new_hdf5_meta PRIVATE m  query_utils timer_utils thpool indexds fs_utils c_base_utils)

ADD_LIBRARY(hdf52json hdf5/hdf52json.c hdf5/hdf52json.h)
target_include_directories(hdf52json PRIVATE "${JSON-C_INCLUDE_DIRS}")
target_link_libraries(hdf52json PRIVATE "${JSON-C_LIBRARIES}" new_hdf5_meta)

#ADD_LIBRARY(hdf52index hdf5/hdf52index.c hdf5/hdf52index.h)
#target_link_libraries(hdf52json PRIVATE new_hdf5_meta indexds)