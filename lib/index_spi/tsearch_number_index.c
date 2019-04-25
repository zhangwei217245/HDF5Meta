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
    void *tsearch_rst = tsearch(key, &index_root, tsearch_cmp_cb);
    return tsearch_rst != NULL;
}

int search_number_from_tsearch_index(void *index_root, void *key, void **out){
    int rst = -1;
    if (index_root == NULL){
        return rst;
    }
    void *tfind_rst = tfind(key, &index_root, tsearch_cmp_cb);
    rst = tfind_rst != NULL;
    if (rst) {
        out[0] = tfind_rst;
    }
    return rst;
}

void walk_with_range(const void *nodep, VISIT value, int level){
    // the problem is that the callback function does not take any
    // user-provided parameter and there is no way to pass the boundaries
    // of the range condition to callback function. 
}

linked_list_t *search_numeric_range_from_tsearch_index(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size){
    linked_list_t *rst = NULL;
    if (index_root == NULL){
        return rst;
    }
    twalk(index_root, walk_with_range);
    return rst; // seems like we cannot provide such implementation 
                //and currently there is no need for this. 
}

size_t get_mem_in_tsearch(){
    return 0;
}