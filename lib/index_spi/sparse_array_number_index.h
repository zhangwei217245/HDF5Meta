#ifndef MIQS_SPARSE_ARRAY_NUMBER_INDEX_H
#define MIQS_SPARSE_ARRAY_NUMBER_INDEX_H

#include "../libhl/sparse_array.h"


int create_sparse_array_index(void **idx_ptr);

int insert_number_to_sparse_array(void *index_root, void *number, size_t num_size, void *data);

int search_number_from_sparse_array(void *index_root, void *number, size_t num_size, void **out);

size_t get_mem_in_sparse_array();

#endif // end MIQS_SPARSE_ARRAY_NUMBER_INDEX_H