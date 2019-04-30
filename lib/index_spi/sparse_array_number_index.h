#ifndef MIQS_SPARSE_ARRAY_NUMBER_INDEX_H
#define MIQS_SPARSE_ARRAY_NUMBER_INDEX_H

#include "../libhl/sparse_array.h"
#include "../libhl/linklist.h"
#include "../libhl/comparators.h"


int create_sparse_array_index(void **idx_ptr, libhl_cmp_callback_t cb);

int insert_number_to_sparse_array(void *index_root, void *key, size_t klen, void *data);

int search_number_from_sparse_array(void *index_root, void *key, size_t klen, void **out);

linked_list_t *search_numeric_range_from_sparse_array(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size);
// linked_list_t *search_numeric_range_from_sparse_array(void *index_root, size_t begin, size_t end);

// size_t get_mem_in_sparse_array();

#endif // end MIQS_SPARSE_ARRAY_NUMBER_INDEX_H