#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <strings.h>
#include <sched.h>

#include "bsd_queue.h"
#include "atomic_defs.h"
#include "hashtable.h"
#include "sparse_array.h"


#ifdef USE_PACKED_STRUCTURES
#define PACK_IF_NECESSARY __attribute__((packed))
#else
#define PACK_IF_NECESSARY
#endif

// We currently implement this sparse array using hash table for simplicity. 
struct _sparse_array_s {
    hashtable_t *core;
    void **array; // the actual array
    spa_size_info_t *size_info;
    spa_free_item_callback_t free_item_cb;
    libhl_cmp_callback_t locate_cb;
    DECLARE_PERF_INFO_FIELDS
};


int spa_init(sparse_array_t *spa, size_t initial_size, size_t max_size, spa_free_item_callback_t cb, libhl_cmp_callback_t locate_cb){
    if (spa == NULL) {
        return -1;
    }
    spa->size_info->size = initial_size > SPA_SIZE_MIN?initial_size:SPA_SIZE_MIN;
    spa->size_info->max_size = max_size;
    spa->array = (void **)ctr_calloc(spa->size_info->size, sizeof(void *), &spa->mem_usage);
    spa->free_item_cb=cb;
    spa->locate_cb = locate_cb;
    return 0;
}

sparse_array_t *create_sparse_array(size_t initial_size, size_t max_size, spa_free_item_callback_t cb, libhl_cmp_callback_t locate_cb){
    sparse_array_t *spa = calloc(1, sizeof(sparse_array_t));
    INIT_PERF_INFO_FIELDS(spa, sparse_array_t);
    spa->size_info = calloc(1, sizeof(spa_size_info_t));
    if (spa && spa_init(spa, initial_size, max_size, cb, locate_cb) != 0) {
        free(spa);
        return NULL;
    }
    return spa;
}

int set_element_to_sparse_array(sparse_array_t *sparse_arr, void *poss, void *data){
    // return ht_set(sparse_arr->core, key, ksize, data, sizeof(data)); //FIXME: what is dlen;
    int rst = -1;
    // size_t pos = (sparse_arr->locate_cb)?
    //     sparse_arr->locate_cb(poss, 0, NULL, 0):(*(size_t *)poss);
    size_t pos = *(size_t *)poss;
    printf("set pos = %ld\n", pos);
    if (pos >= sparse_arr->size_info->size){
        if (pos >= sparse_arr->size_info->max_size) {
            return rst; // if invalid pos, return -1
        }
        stopwatch_t t_expand;
        timer_start(&t_expand);
        //grow sparse array to what is needed. 
        size_t new_size = pos + 1;
        void **new_space = ctr_realloc(sparse_arr->array, new_size, &(sparse_arr->mem_usage));
        sparse_arr->num_of_reallocs++;
        if (new_space == NULL) { // realloc fail due to insufficient memory
            return rst;
        }
        if (new_space != sparse_arr->array) { //new block allocated
            sparse_arr->array = new_space;
            ATOMIC_SET(sparse_arr->size_info->size, new_size);
        } else { // expanded, not much to do. 
            ATOMIC_SET(sparse_arr->size_info->size, new_size);
        }
        timer_pause(&t_expand);
        sparse_arr->time_for_expansion+=timer_delta_ns(&t_expand);
    }
    // ATOMIC_SET(sparse_arr->array[pos], data);
    sparse_arr->array[pos]= data;
    sparse_arr->num_of_comparisons++;
    ATOMIC_INCREMENT(sparse_arr->size_info->count);
    rst = 0;
    return rst;
}

void *get_element_in_sparse_array(sparse_array_t *sparse_arr, void *poss){
    size_t pos = (sparse_arr->locate_cb)?
        sparse_arr->locate_cb(poss, 0, NULL, 0):(*(size_t *)poss);
    if (pos >= sparse_arr->size_info->size){
        return NULL;
    }
    stopwatch_t t_locate;
    timer_start(&t_locate);
    void *rst = sparse_arr->array[pos];
    timer_pause(&t_locate);
    sparse_arr->time_to_locate+=timer_delta_ns(&t_locate);
    sparse_arr->num_of_comparisons++;
    return rst;
}


spa_size_info_t *get_sparse_array_size(sparse_array_t *sparse_arr){
    if (sparse_arr==NULL) {
        return NULL;
    }
    return sparse_arr->size_info;
}


void spa_foreach_elements(sparse_array_t *sparse_arr, void *beginn, void *endd, 
                        spa_iterator_callback_t cb, void *user){
    if (sparse_arr == NULL) {
        return;
    }

    // stopwatch_t t_adjust_range;
    // timer_start(&t_adjust_range);

    size_t begin = (sparse_arr->locate_cb!=NULL)?
        sparse_arr->locate_cb(beginn, 0, NULL, 0):(*(size_t *)beginn);
    size_t end = (sparse_arr->locate_cb!=NULL)?
    sparse_arr->locate_cb(endd, 0, NULL, 0):(*(size_t *)endd);

    printf("[sparse range]%ld, %ld\n", begin, end);

    int _bgn, _end;
    if (begin < 0 || 
        begin >= sparse_arr->size_info->size || 
        end < 0 || 
        end >= sparse_arr->size_info->size ||
        begin >= end ) {
        _bgn = 0;
        _end = sparse_arr->size_info->size;
    } else {
        _bgn = begin;
        _end = end;
    }

    // timer_pause(&t_adjust_range);
    // stw_nanosec_t time_adjust = timer_delta_ns(&t_adjust_range);
    // printf("Time to adjust range is  %llu \n", time_adjust);

    // timer_start(&t_adjust_range);

    printf("[sparse range]%d, %d\n", _bgn, _end);
    int i = _bgn;
    for (i = _bgn; i < _end; i++) {
        if (cb && (sparse_arr->array)[i]){
            printf("[sparse range]%d, %ld\n", i, *(long *)(sparse_arr->array)[i]);
            spa_iterator_status_t st = cb(sparse_arr, i, sparse_arr->array[i], user);
            if (st == SPA_ITERATOR_STOP) {
                break;
            }
            // void *v = sparse_arr->array[i];
            // printf("%d\n",i);
        }
    }
    // timer_pause(&t_adjust_range);
    // stw_nanosec_t time_range = timer_delta_ns(&t_adjust_range);
    // printf("Time to go through range is  %llu \n", time_range);
}

/**
 * Get memory usage by sparse array. 
 */
perf_info_t * get_perf_info_sparse_array(sparse_array_t *spa){
    GET_PERF_INFO(spa);
}