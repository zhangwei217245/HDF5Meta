#ifndef MIQS_SKIPLIST_NUMBER_INDEX_H
#define MIQS_SKIPLIST_NUMBER_INDEX_H

#include "../libhl/skiplist.h"
#include "../libhl/linklist.h"
#include <stdlib.h>

int create_skiplist_index(void **idx_ptr, DATA_TYPE data_type);

int insert_number_to_skiplist(void *index_root, void *key, size_t ksize,void *data);

int search_number_from_skiplist(void *index_root, void *key, size_t ksize, void **out);

linked_list_t *search_numeric_range_from_skiplist(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size);

// size_t get_mem_in_skiplist();

#endif //end MIQS_SKIPLIST_NUMBER_INDEX_H