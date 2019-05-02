#include "rbtree_number_index.h"

int create_rbtree_number_index(void **idx_ptr, libhl_cmp_callback_t cb){
    int rst  = -1 ;
    if (idx_ptr == NULL) {
        return rst;
    }
    rbt_t *rbt = rbt_create(cb, NULL);
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


rbt_walk_return_code_t rbt_walk_cb(rbt_t *rbtree, void *key, size_t klen,
        void *value, void *priv){
    
    if (priv) {
        linked_list_t *rst = (linked_list_t *)priv;
        list_push_value(rst, value);
    }
    return RBT_WALK_CONTINUE;
}

linked_list_t *search_numeric_range_from_rbtree(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size){
    linked_list_t *rst = NULL;
    if (index_root == NULL) {
        return rst;
    }
    rbt_t *rbtree = (rbt_t *)index_root;
    rst = list_create();
    rbt_range_walk(rbtree, begin_key,  bgk_size, end_key,  edk_size, rbt_walk_cb, rst);
    return rst;
}


// size_t get_mem_in_number_rbtree(){
//     return get_mem_usage_by_all_rbtrees();
// }
