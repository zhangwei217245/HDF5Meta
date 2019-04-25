#ifndef MIQS_TSEARCH_NUMBER_INDEX_H
#define MIQS_TSEARCH_NUMBER_INDEX_H

#include "../include/base_stdlib.h"
#include "../libhl/linklist.h"
#include <search.h>



int create_tsearch_index(void **idx_ptr);

int insert_number_to_tsearch_index(void *index_root, void *key, void *data);

int search_number_from_tsearch_index(void *index_root, void *key, void **out);

linked_list_t *search_numeric_range_from_tsearch_index(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size);

size_t get_mem_in_tsearch();

#endif // END MIQS_TSEARCH_NUMBER_INDEX_H
