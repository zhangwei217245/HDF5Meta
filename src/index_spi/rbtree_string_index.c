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

rbt_walk_return_code_t rbt_affix_match_cb(rbt_t *rbt, void *key, size_t klen, void *value, void *user){
    affix_t *affix_info = (affix_t *)user;
    if (is_matching_given_affix((const char *)key, affix_info)) {
        linked_list_t *collector = (linked_list_t *)affix_info->user;
        list_push_value(collector, value);
    }
    return RBT_WALK_CONTINUE;
}

linked_list_t *search_affix_in_rbtree(void *index_root, pattern_type_t afx_type, char *affix){
    linked_list_t *rst = NULL;
    if (index_root == NULL) {
        return rst;
    }
    rst = list_create();
    rbt_t *rbtree = (rbt_t *)index_root;
    affix_t *affix_info = create_affix_info(affix, strlen(affix), afx_type, rst);
    rbt_walk(rbtree, rbt_affix_match_cb, affix_info);
    return rst;
}



// size_t get_mem_in_rbtree(){
//     return get_mem_usage_by_all_rbtrees();
// }