#include "sparse_array.h"
#include "hashtable.h"
#include "../profile/mem_perf.h"


size_t mem_usage_all_sparse_array;
// We currently implement this sparse array using hash table for simplicity. 
struct sparse_array_s {
    hashtable_t *core;
};

sparse_array_t *create_sparse_array(size_t initial_size){
    sparse_array_t *rst = ctr_calloc(1, sizeof(sparse_array_t), &mem_usage_all_sparse_array);
    rst->core = ht_create(initial_size, 0, NULL);
    return rst;
}

int set_element_to_sparse_array(sparse_array_t *sparse_arr, void *key, size_t ksize, void *data){
    return ht_set(sparse_arr->core, key, ksize, data, sizeof(data)); //FIXME: what is dlen;
}

void *get_element_in_sparse_array(sparse_array_t *sparse_arr, void *key, size_t ksize){
    return ht_get(sparse_arr->core, key, ksize, NULL);
}


/**
 * Get memory usage by sparse array. 
 * 
 */
size_t get_mem_usage_by_all_sparse_array(){
    return mem_usage_all_sparse_array + get_mem_usage_by_all_hashtable();
}