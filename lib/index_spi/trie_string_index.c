#include "trie_string_index.h"
#include <stdlib.h>

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