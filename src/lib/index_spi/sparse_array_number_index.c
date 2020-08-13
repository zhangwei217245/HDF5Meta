#include "sparse_array_number_index.h"

int create_sparse_array_index(void **idx_ptr, DATA_TYPE data_type){
    int rst = -1;
    if (idx_ptr == NULL) {
        return rst;
    }
    LIBHL_CHOOSE_CMP_CB(data_type);
    sparse_array_t *sparse_arr = create_sparse_array(0, 0, free, locate_cb);
    idx_ptr[0] = sparse_arr;
    rst = 0;
    return rst;
}

int insert_number_to_sparse_array(void *index_root, void *key, size_t klen, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    // size_t pos = *((size_t *)key);
    rst = set_element_to_sparse_array((sparse_array_t *)index_root, key, data);
    return rst;
}

int search_number_from_sparse_array(void *index_root, void *key, size_t klen, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    // size_t pos = *((size_t *)key);
    void *data = get_element_in_sparse_array((sparse_array_t *)index_root, key);
    out[0] = data; rst = 0;
    return rst;
}

spa_iterator_status_t range_cb(sparse_array_t *spa, size_t array_idx, void *element, void *user){
    if (user) {
        linked_list_t *collector = (linked_list_t *)user;
        list_push_value(collector, element);
    }
    return SPA_ITERATOR_CONTINUE;
}

// TODO: need to consider float number skewness and scaling problem
linked_list_t *search_numeric_range_from_sparse_array(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size){
    linked_list_t *rst = NULL;
    if (index_root == NULL) {
        return rst;
    }
    // size_t begin = *((size_t *)begin_key);
    // size_t end = *((size_t *)end_key);
    sparse_array_t *spa = (sparse_array_t *)index_root;
    rst = list_create();
    spa_foreach_elements(spa, begin_key, end_key, range_cb, rst);
    return rst;
}

// size_t get_mem_in_sparse_array(){
//     return get_mem_usage_by_all_sparse_array();
// }