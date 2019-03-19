#ifndef MIQS_LIBHL_SPARSE_ARRAY_H
#define MIQS_LIBHL_SPARSE_ARRAY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>

typedef struct sparse_array_s sparse_array_t;

sparse_array_t *create_sparse_array(size_t initial_size);

int set_element_to_sparse_array(sparse_array_t *sparse_arr, void *key, size_t ksize, void *data);

void *get_element_in_sparse_array(sparse_array_t *sparse_arr, void *key, size_t ksize);


/**
 * Get memory usage by sparse array. 
 * 
 */
size_t get_mem_usage_by_all_sparse_array();

#ifdef __cplusplus
}
#endif

#endif // END MIQS_LIBHL_SPARSE_ARRAY_H