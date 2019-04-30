#include "art_string_index.h"

typedef struct _infix_iter_arg{
    linked_list_t *rst;
    char *infix;
} infix_iter_arg_t;

int create_art(void **idx_ptr){
    int rst  = -1;
    art_tree *t = (art_tree *)calloc(1, sizeof(art_tree));
    if (t == NULL) {
        return rst;
    }
    rst = art_tree_init(t);
    idx_ptr[0] = t;
    return rst;
}

int insert_string_to_art(void *index_root, char *key, void *data){
    int rst = -1;

    if (index_root == NULL) {
        return rst;
    }
    art_insert((art_tree *)index_root, key, strlen(key), data);
    rst = 0;
    return rst;
}

int search_string_in_art(void *index_root, char *key, size_t len, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    void *data = art_search((art_tree *)index_root, key, strlen(key));
    out[0] = data;
    rst = 0;
    return rst;
}

int infix_callback(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    infix_iter_arg_t *args = (infix_iter_arg_t *)data;
    if (contains((const char *)key, args->infix)) {
        return list_push_value(args->rst, value);
    }
    return 0;
}

int prefix_callback(void *data, const unsigned char *key, uint32_t key_len, void *value){
    linked_list_t *rst = (linked_list_t *)data;
    return list_push_value(rst, value);
}

linked_list_t *search_affix_in_art(void *index_root, pattern_type_t afx_type, char *affix){
    linked_list_t *rst = NULL;
    if (index_root == NULL) {
        return rst;
    }
    rst = list_create();
    art_tree *art = (art_tree *)index_root;
    if (afx_type == PATTERN_PREFIX || afx_type == PATTERN_SUFFIX) {
        art_iter_prefix(art, (const unsigned char *)affix, strlen(affix), prefix_callback, rst);
    } else if (afx_type == PATTERN_MIDDLE) {
        art_iter(art, infix_callback, art);
    } else {
        void *out = NULL;
        search_string_in_art(index_root, affix, strlen(affix), &out);
        list_push_value(rst, out);
    }
    return  rst;
}

// size_t get_mem_in_art(){
//     return get_mem_usage_by_all_arts();
// }