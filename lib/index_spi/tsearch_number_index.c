#include "tsearch_number_index.h"

static inline int tsearch_cmp_cb(const void *a, const void *b) {
    return (sizeof(a)!=sizeof(b))?(sizeof(a)-sizeof(b)):
        ((sizeof(a)<sizeof(b))?memcmp(a, b, sizeof(a)):memcmp(a, b, sizeof(b)));
}

int create_tsearch_index(void **idx_ptr){
    int rst = -1;

    if (idx_ptr == NULL){
        return rst;
    }

    return 0;
}

int insert_number_to_tsearch_index(void *index_root, void *key, void *data){
    int rst = -1;
    if (index_root == NULL){
        return rst;
    }
    void *tsearch_rst = tsearch(key, index_root, tsearch_cmp_cb);
    return tsearch_rst != NULL;
}

int search_number_from_tsearch_index(void *index_root, void *key, void **out){
    int rst = -1;
    if (index_root == NULL){
        return rst;
    }
    void *tfind_rst = tfind(key, index_root, tsearch_cmp_cb);
    rst = tfind_rst != NULL;
    if (rst) {
        out[0] = tfind_rst;
    }
    return rst;
}

size_t get_mem_in_tsearch(){
    return 0;
}