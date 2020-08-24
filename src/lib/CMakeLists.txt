if(APPLE)
    set(BASE_LIBRARIES m)
else()
    set(BASE_LIBRARIES m ${UUID_LIBRARIES})
endif()

ADD_LIBRARY(c_base_utils SHARED include/c_base_stdlib.c include/config.h include/base_stdlib.h include/c95_stdlib.h include/c99_stdlib.h include/c11_stdlib.h)

ADD_LIBRARY(hash_utils SHARED utils/hash_utils.c utils/hash_utils.h)

ADD_LIBRARY(string_utils SHARED utils/string_utils.c utils/string_utils.h)
TARGET_LINK_LIBRARIES(string_utils PUBLIC c_base_utils ${BASE_LIBRARIES} hash_utils)
TARGET_INCLUDE_DIRECTORIES(string_utils PUBLIC ${UUID_INCLUDE_DIRS})

ADD_LIBRARY(cmd_utils SHARED utils/cmd_utils.c utils/cmd_utils.h)
TARGET_LINK_LIBRARIES(cmd_utils PUBLIC string_utils)

ADD_LIBRARY(query_utils SHARED utils/query_utils.c utils/query_utils.h)
TARGET_LINK_LIBRARIES(query_utils PUBLIC string_utils)

ADD_LIBRARY(timer_utils SHARED utils/timer_utils.c utils/timer_utils.h)

ADD_LIBRARY(thpool SHARED thread/thpool.c thread/thpool.h)
if(THREADS_HAVE_PTHREAD_ARG)
  target_compile_options(thpool PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(thpool PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif()

ADD_LIBRARY(profile SHARED 
    profile/mem_perf.h
    profile/mem_perf.c
)
TARGET_LINK_LIBRARIES(profile PUBLIC timer_utils)

ADD_LIBRARY(libhl SHARED
    libhl/atomic_defs.h
    libhl/avltree.c
    libhl/avltree.h
    libhl/binheap.c
    libhl/binheap.h
    libhl/bsd_queue.h
    libhl/comparators.h
    libhl/fbuf.c
    libhl/fbuf.h
    libhl/graph.c
    libhl/graph.h
    libhl/hashtable.c
    libhl/hashtable.h
    libhl/linklist.c
    libhl/linklist.h
    libhl/pqueue.c
    libhl/pqueue.h
    libhl/queue.c
    libhl/queue.h
    libhl/rbtree.c
    libhl/rbtree.h
    libhl/rbuf.c
    libhl/rbuf.h
    libhl/refcnt.c
    libhl/refcnt.h
    libhl/rqueue.c
    libhl/rqueue.h
    libhl/skiplist.c
    libhl/skiplist.h
    libhl/sparse_array.c
    libhl/sparse_array.h
    libhl/trie.c
    libhl/trie.h
)
TARGET_LINK_LIBRARIES(libhl PUBLIC profile)
if(THREADS_HAVE_PTHREAD_ARG)
  target_compile_options(libhl PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(libhl PUBLIC c_base_utils "${CMAKE_THREAD_LIBS_INIT}")
endif()

#ADD_LIBRARY(indexds SHARED ds/tsearch_tune.c ds/art.c ds/art.h ds/hashset.c ds/hashset.h ds/hashmap.c ds/hashmap.h ds/vector.c ds/vector.h ds/bplustree.c ds/bplustree.h)

ADD_LIBRARY(indexds SHARED 
    ds/art.c 
    ds/art.h 
    ds/hashset.c 
    ds/hashset.h 
    ds/hashmap.c 
    ds/hashmap.h 
    ds/vector.c 
    ds/vector.h
    )
TARGET_LINK_LIBRARIES(indexds PUBLIC libhl)


ADD_LIBRARY(index_spi SHARED 
    index_spi/art_string_index.c
    index_spi/art_string_index.h
    index_spi/hash_table_string_index.h
    index_spi/hast_table_string_index.c
    index_spi/rbtree_number_index.c
    index_spi/rbtree_number_index.h
    index_spi/rbtree_string_index.c
    index_spi/rbtree_string_index.h
    index_spi/skiplist_number_index.c
    index_spi/skiplist_number_index.h
    index_spi/sparse_array_number_index.c
    index_spi/sparse_array_number_index.h
    index_spi/spi.c
    index_spi/spi.h
    index_spi/trie_string_index.c
    index_spi/trie_string_index.h
    index_spi/tsearch_number_index.c
    index_spi/tsearch_number_index.h
)
TARGET_LINK_LIBRARIES(index_spi PUBLIC indexds query_utils)

ADD_LIBRARY(fs_utils SHARED fs/fs_ops.c fs/fs_ops.h)
TARGET_LINK_LIBRARIES(fs_utils PUBLIC string_utils c_base_utils)


ADD_LIBRARY(bin_file_ops SHARED dao/bin_file_ops.c dao/bin_file_ops.h)


ADD_LIBRARY(miqs_metadata SHARED 
    metadata/miqs_metadata.c 
    metadata/miqs_metadata.h 
    metadata/miqs_meta_collector.c
    metadata/miqs_meta_collector.h
    )
target_link_libraries(miqs_metadata PUBLIC profile m  query_utils timer_utils thpool indexds fs_utils cmd_utils c_base_utils)

ADD_LIBRARY(hdf5_meta_extractor SHARED  
    metadata/extractor/hdf5_meta_extractor.c
    metadata/extractor/hdf5_meta_extractor.h
)
target_link_libraries(hdf5_meta_extractor PUBLIC miqs_metadata ${HDF5_LIBRARIES})
target_include_directories(hdf5_meta_extractor PUBLIC "${HDF5_INCLUDE_DIRS}")


ADD_LIBRARY(miqs_index SHARED 
    index/in_mem_index.c 
    index/in_mem_index.h 
    index/on_disk_index.c 
    index/on_disk_index.h 
)
target_link_libraries(miqs_index PUBLIC miqs_metadata indexds bin_file_ops index_spi)


ADD_LIBRARY(hdf52json SHARED h52json/hdf52json.c h52json/hdf52json.h)
target_link_libraries(hdf52json PUBLIC ${JSON-C_LIBRARIES} hdf5_meta_extractor)
target_include_directories(hdf52json PUBLIC "${JSON-C_INCLUDE_DIRS}" "${HDF5_INCLUDE_DIRS}")

ADD_LIBRARY(hdf52index SHARED h52index/hdf52index.c h52index/hdf52index.h)
target_link_libraries(hdf52index PUBLIC miqs_index hdf5_meta_extractor)
target_include_directories(hdf52index PUBLIC "${HDF5_INCLUDE_DIRS}")