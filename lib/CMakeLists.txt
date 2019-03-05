set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/modules")

find_package(JSON-C REQUIRED)

ADD_LIBRARY(c_base_utils SHARED include/c_base_stdlib.c include/base_stdlib.h include/c95_stdlib.h include/c99_stdlib.h include/c11_stdlib.h)
ADD_LIBRARY(string_utils SHARED utils/string_utils.c utils/string_utils.h)
TARGET_LINK_LIBRARIES(string_utils c_base_utils)

ADD_LIBRARY(cmd_utils SHARED utils/cmd_utils.c utils/cmd_utils.h)
TARGET_LINK_LIBRARIES(cmd_utils string_utils)

ADD_LIBRARY(query_utils SHARED utils/query_utils.c utils/query_utils.h)
TARGET_LINK_LIBRARIES(query_utils string_utils)

ADD_LIBRARY(timer_utils SHARED utils/timer_utils.c utils/timer_utils.h)

ADD_LIBRARY(thpool SHARED thread/thpool.c thread/thpool.h)

ADD_LIBRARY(profile SHARED profile/*.h profile/*.c)
TARGET_LINK_LIBRARIES(profile timer_utils)

ADD_LIBRARY(libhl SHARED libhl/*.c libhl/*.h)
TARGET_LINK_LIBRARIES(libhl profile)

#ADD_LIBRARY(indexds SHARED ds/tsearch_tune.c ds/art.c ds/art.h ds/hashset.c ds/hashset.h ds/hashmap.c ds/hashmap.h ds/vector.c ds/vector.h ds/bplustree.c ds/bplustree.h)

ADD_LIBRARY(indexds SHARED ds/art.c ds/art.h ds/hashset.c ds/hashset.h ds/hashmap.c ds/hashmap.h ds/vector.c ds/vector.h ds/bplustree.c ds/bplustree.h)
TARGET_LINK_LIBRARIES(indexds libhl)



ADD_LIBRARY(index_spi SHARED index_spi/*.h index_spi/*.c)
TARGET_LINK_LIBRARIES(index_spi indexds)

ADD_LIBRARY(fs_utils SHARED fs/fs_ops.c fs/fs_ops.h)

ADD_LIBRARY(bin_file_ops SHARED dao/bin_file_ops.c dao/bin_file_ops.h)

TARGET_LINK_LIBRARIES(fs_utils string_utils c_base_utils)

ADD_LIBRARY(hdf5_meta hdf5_meta.c hdf5_meta.h)

include_directories(${JSON-C_INCLUDE_DIRS})
include_directories (${PROJECT_SOURCE_DIR}/lib)
link_directories (${PROJECT_BINARY_DIR}/lib)
target_include_directories(hdf5_meta PRIVATE "${JSON-C_INCLUDE_DIRS}")
target_link_libraries(hdf5_meta PRIVATE "${JSON-C_LIBRARIES}" m  query_utils timer_utils thpool indexds fs_utils c_base_utils)


ADD_LIBRARY(new_hdf5_meta SHARED hdf5/new_hdf5_meta.c hdf5/new_hdf5_meta.h)
target_link_libraries(new_hdf5_meta  m  query_utils timer_utils thpool indexds fs_utils cmd_utils c_base_utils)

ADD_LIBRARY(hdf52json hdf5/hdf52json.c hdf5/hdf52json.h)
target_include_directories(hdf52json PRIVATE "${JSON-C_INCLUDE_DIRS}")
target_link_libraries(hdf52json PRIVATE "${JSON-C_LIBRARIES}" new_hdf5_meta)

ADD_LIBRARY(hdf52index index/in_mem_index.c index/in_mem_index.h index/on_disk_index.c index/on_disk_index.h index/hdf52index.c index/hdf52index.h)
target_link_libraries(hdf52index PRIVATE new_hdf5_meta indexds bin_file_ops index_spi)