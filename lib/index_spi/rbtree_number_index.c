#include "rbtree_number_index.h"

int create_rbtree_number_index(void **idx_ptr){
    int rst  = -1 ;
    if (idx_ptr == NULL) {
        return rst;
    }
    rbt_t *rbt = rbt_create(NULL, free);
    idx_ptr[0] = rbt;
    rst = 0;
    return rst;
}

int insert_number_to_rbtree(void *index_root, void *key, size_t ksize, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    rbt_t *rbt = (rbt_t *)index_root;
    rst = rbt_add(rbt, key, ksize, data);
    return rst;
}

int search_number_from_rbtree(void *index_root, void *key, size_t ksize, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    rst = rbt_find((rbt_t *)index_root, key, ksize, out);
    return rst;
}

size_t get_mem_in_number_rbtree(){
    return get_mem_usage_by_all_rbtrees();
}
