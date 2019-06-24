#include "trie_string_index.h"
#include <stdlib.h>

typedef struct _infix_iter_arg{
    linked_list_t *rst;
    char *infix;
} infix_iter_arg_t;

int create_trie(void **idx_ptr){
    int rst = -1;
    if (idx_ptr == NULL) {
        return rst;
    }
    trie_t *trie = trie_create(NULL);
    idx_ptr[0] = trie;
    rst = 0;
    return rst;
}

int insert_string_to_trie(void *index_root, char *key, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    trie_t *trie = (trie_t *)index_root;
    rst = trie_insert(trie, key, &data, sizeof(data), 0);
    rst = trie_insert(trie, reverse_str(key), data, sizeof(data), 0);// insert reverse to achieve suffix query
    return rst;
}

int search_string_in_trie(void *index_root, char *key, size_t len, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    size_t vsize = -1;
    void *data = trie_find((trie_t *)index_root, key, &vsize);
    out[0]= data;
    rst = 0;
    return rst;
}

int trie_iter_infix_cb(char *key_on_node, void *value, size_t vsize, void *user){
    infix_iter_arg_t *args = (infix_iter_arg_t *)user;
    if (contains(key_on_node, args->infix)) {
        list_push_value(args->rst, value);
        return 1;
    } 
    return 1;
}

int trie_iter_prefix_cb(char *key_on_node, void *value, size_t vsize, void *user){
    linked_list_t *list = (linked_list_t *)user;
    list_push_value(list, value);
    return 1;
}

linked_list_t *search_affix_in_trie(void *index_root, pattern_type_t afx_type, char *affix){
    linked_list_t *rst = NULL;
    if (index_root == NULL){
        return rst;
    }
    trie_t *trie = (trie_t *)index_root;
    rst = list_create();
    if (afx_type == PATTERN_MIDDLE) {
        infix_iter_arg_t *infix_args = (infix_iter_arg_t *)calloc(1, sizeof(infix_iter_arg_t));
        infix_args->rst = rst;
        infix_args->infix = affix;
        trie_iter_all(trie, trie_iter_infix_cb, infix_args);
    } else if (afx_type == PATTERN_PREFIX ){
        trie_iter_prefix(trie, affix, trie_iter_prefix_cb, rst);
    } else if(afx_type == PATTERN_SUFFIX) {
        char *reverse_affix = reverse_str(affix);
        trie_iter_prefix(trie, reverse_affix, trie_iter_prefix_cb, rst);
    } else { // exact query
        void *data = NULL;
        search_string_in_trie(index_root, affix, strlen(affix), &data);
        list_push_value(rst, data);
    }
    return rst;
}

// size_t get_mem_in_trie(){
//     return get_mem_usage_by_all_tries();
// }