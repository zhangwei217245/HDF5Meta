#include "rbtree_string_index.h"

int create_rbtree(void **idx_ptr){
    int rst  = -1 ;
    if (idx_ptr == NULL) {
        return rst;
    }
    rbt_t *rbt = rbt_create(libhl_cmp_keys_string, free);
    idx_ptr[0] = rbt;
    rst = 0;
    return rst;
}

int insert_string_to_rbtree(void *index_root, char *key, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    rbt_t *rbt = (rbt_t *)index_root;
    rst = rbt_add(rbt, key, strlen(key), data);
    return rst;
}

int search_string_in_rbtree(void *index_root, char *key, size_t len, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    rst = rbt_find((rbt_t *)index_root, key, strlen(key), out);
    return rst;
}