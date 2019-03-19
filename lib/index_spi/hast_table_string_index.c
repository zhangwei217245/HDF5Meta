#include "hash_table_string_index.h"

int create_hashtable(void **idx_ptr){
    int rst = -1;
    if (idx_ptr == NULL) {
        return rst;
    }
    hashtable_t *table = ht_create(256, 0, NULL);
    idx_ptr[0] = table;
    rst = 0;
    return rst;
}

int insert_string_to_hashtable(void *index_root, char *key, void *data){
    int rst = -1;
    if (index_root == NULL) {
        return 0;
    }
    hashtable_t *table = (hashtable_t *)index_root;
    rst = ht_set(table, key, strlen(key), data, sizeof(data)); // FIXME: how to determine data size?
    return rst;
}

int search_string_in_hashtable(void *index_root, char *key, size_t len, void **out){
    size_t dlen = -1;
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return 0;
    }
    void *data = ht_get((hashtable_t *)index_root, key, len, &dlen);
    out[0] = data;
    return dlen;
}

size_t get_mem_in_hashtable(){
    return get_mem_usage_by_all_hashtable();
}