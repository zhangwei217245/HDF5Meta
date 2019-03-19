#include "skiplist_number_index.h"


int create_skiplist_index(void **idx_ptr){
    int rst  = -1 ;
    if (idx_ptr == NULL) {
        return rst;
    }
    skiplist_t *skiplist = skiplist_create(5, 50, libhl_cmp_keys_long, NULL);
    idx_ptr[0] = skiplist;
    rst = 0;
    return rst;
}

int insert_number_to_skiplist(void *index_root, void *key, size_t ksize, void *data){

    int rst = -1;
    if (index_root == NULL) {
        return rst;
    }
    rst = skiplist_insert((skiplist_t *)index_root, key, ksize, data);
    return rst;
}

int search_number_from_skiplist(void *index_root, void *key, size_t ksize, void **out){
    int rst = -1;
    if (index_root == NULL || out == NULL) {
        return rst;
    }
    void *data = skiplist_search((skiplist_t *)index_root, key, ksize);
    out[0] = data; rst = 0;
    return rst;
}

size_t get_mem_in_skiplist(){
    return get_mem_usage_by_all_skiplists();
}