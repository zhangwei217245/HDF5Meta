#include "sparse_array_number_index.h"

int create_sparse_array_index(void **idx_ptr){
    int rst = -1;
    if (idx_ptr == NULL) {
        return rst;
    }
    sparse_array_t *sparse_arr = create_sparse_array(256);
    idx_ptr[0] = sparse_arr;
    rst = 0;
    return rst;
}

int insert_number_to_sparse_array(void *index_root, void *number, size_t ksize, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    rst = set_element_to_sparse_array((sparse_array_t *)index_root, number, ksize, data);
    return rst;
}

int search_number_from_sparse_array(void *index_root, void *number, size_t ksize, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    void *data = get_element_in_sparse_array((sparse_array_t *)index_root, number, ksize);
    out[0] = data; rst = 0;
    return rst;
}

size_t get_mem_in_sparse_array(){
    return get_mem_usage_by_all_sparse_array();
}