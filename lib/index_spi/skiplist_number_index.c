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

void collect_data_from_skiplist(skiplist_t *skl, void *key, size_t klen, void *value, void *user){
    if (user){
        linked_list_t *rst = (linked_list_t *)user;
        list_push_value(rst, value); // TODO: to check if we need to push tagged value
    }
}

linked_list_t *search_numeric_range_from_skiplist(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size){
    linked_list_t *rst = NULL;
    if (index_root == NULL) {
        return rst;
    }
    skiplist_t *skl = (skiplist_t *)index_root;
    rst = list_create();
    skiplist_range_search(skl, begin_key, bgk_size, 
        end_key, edk_size, collect_data_from_skiplist, rst);
    return rst;
}

// size_t get_mem_in_skiplist(){
//     return get_mem_usage_by_all_skiplists();
// }