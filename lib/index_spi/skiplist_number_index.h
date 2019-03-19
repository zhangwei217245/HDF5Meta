#ifndef MIQS_SKIPLIST_NUMBER_INDEX_H
#define MIQS_SKIPLIST_NUMBER_INDEX_H

#include "../libhl/skiplist.h"

int create_skiplist_index(void **idx_ptr);

int insert_number_to_skiplist(void *index_root, void *key, size_t ksize,void *data);

int search_number_from_skiplist(void *index_root, void *key, size_t ksize, void **out);

size_t get_mem_in_skiplist()

#endif //end MIQS_SKIPLIST_NUMBER_INDEX_H