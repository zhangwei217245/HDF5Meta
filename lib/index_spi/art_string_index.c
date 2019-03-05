#include "art_string_index.h"

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
