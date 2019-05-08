#include <stdlib.h>
#include <string.h>

#include "trie.h"
#include "../profile/mem_perf.h"

#define COMMON_STR_LEN 4096

// size_t mem_usage_by_all_tries;

typedef struct _trie_iterated_char_seq_s{
    char *seq;
    size_t pos;
} trie_iterated_char_seq_t;

typedef struct _trie_node_s {
    void *value;
    size_t vsize;
    int is_copy;
    int num_children;
    struct _trie_node_s *child[256];
    struct _trie_node_s *parent;
    char pidx;
} trie_node_t;

struct _trie_s {
    int count;
    int node_count;
    trie_node_t *root;
    trie_free_value_callback_t free_value_cb;
    DECLARE_PERF_INFO_FIELDS
};

trie_t *
trie_create(trie_free_value_callback_t free_value_cb)
{
    trie_t *trie = calloc(1, sizeof(trie_t));
    INIT_PERF_INFO_FIELDS(trie, trie_t);
    trie->root = ctr_calloc(1, sizeof(trie_node_t), &(trie->mem_usage));
    trie->free_value_cb = free_value_cb;
    return trie;
}


static inline void
trie_node_destroy(trie_t *trie, trie_node_t *node, trie_free_value_callback_t free_value_cb)
{
    trie_node_t *parent = node->parent;
    int pidx = node->pidx;

    while (parent && parent != trie->root) {
        if (parent->value || parent->num_children > 1)
            break;
        trie_node_t *empty_node = parent;
        pidx = parent->pidx;
        parent = parent->parent;
        free(empty_node);
        trie->node_count--;
    }

    if (parent) {
        parent->child[pidx] = NULL;
        parent->num_children--;
    }

    int i;
    for (i = 0; i < 256; i++)
        if (node->child[i]) {
            node->child[i]->parent = NULL;
            trie_node_destroy(trie, node->child[i], free_value_cb);
            node->child[i] = NULL;
            node->num_children--;
        }

    if (node->value) {
        trie->count--;
        if (node->is_copy)
            free(node->value);
        else if (free_value_cb)
            free_value_cb(node->value);
    }

    trie->node_count--;
    free(node);
}

void
trie_destroy(trie_t *trie)
{
    trie_node_destroy(trie, trie->root, trie->free_value_cb);
    free(trie);
}

static inline void
trie_node_set_value(trie_t *trie, trie_node_t *node, void *value, size_t vsize, int copy)
{
    if (node->value) {
        if (node->is_copy)
            free(node->value);
        else if (trie->free_value_cb)
            trie->free_value_cb(node->value);
    }

    if (copy) {
        node->value = ctr_malloc(vsize, &(trie->mem_usage));
        trie->num_of_reallocs++;
        memcpy(node->value, value, vsize);
        node->vsize = vsize;
        node->is_copy = 1;
    } else {
        node->value = value;
        node->vsize = vsize;
        node->is_copy = 0;
    }
}

static inline int trie_node_iterate(trie_node_t *node, prefix_iter_callback_t cb, trie_iterated_char_seq_t *visited, void *user){
    int rst = 0;
    if (visited == NULL){
        return rst;
    }
    if(node) {
        if (strlen(visited->seq) >= visited->pos ){ 
            // just in case visited-seq is not sufficient for concatenating more characters
            char *new_space = realloc(visited->seq, strlen(visited->seq)*2*sizeof(char));
            if (new_space == NULL) {
                return 0;
            } else {
                if (visited->seq != new_space) {
                    visited->seq = new_space;
                }
            }
        }
        // Concatenating one more character on the path
        visited->seq[visited->pos]=node->pidx;
        if (node->value){
            rst = cb(visited->seq, node->value, node->vsize, user);
        }        
        int i = 0;
        for (i = 0; i < node->num_children; i++) {
            trie_node_t *child_node = node->child[i];
            if (child_node) {
                // For each non-empty child, we recursively call the function and
                // before getting into the recursive call, we increase the pos pointer by 1
                visited->pos++;
                rst+=trie_node_iterate(child_node, cb, visited, user);//DFS
                // after the recursive call, let pos step back to where it was for visiting other children
                visited->pos--;
            }
        }
    }
    return rst;
}

int trie_iter_all(trie_t *trie, prefix_iter_callback_t cb, void *user){
    int rst = 0;
    trie_iterated_char_seq_t *visited = (trie_iterated_char_seq_t *)calloc(1, sizeof(trie_iterated_char_seq_t));
    visited->seq = (char *)calloc(COMMON_STR_LEN, sizeof(char));
    visited->pos = 0;
    trie_node_t *node = trie->root;
    if (node) {
        // if there are still characters to be iterated, we start collect the result from the node. 
        rst = trie_node_iterate(node, cb, visited, user);
    }
    trie->num_of_comparisons += rst;
    return rst;
}

int trie_iter_prefix(trie_t *trie, char *prefix, prefix_iter_callback_t cb, void *user){
    int rst = 0;
    trie_iterated_char_seq_t *visited = (trie_iterated_char_seq_t *)calloc(1, sizeof(trie_iterated_char_seq_t));
    visited->seq = (char *)calloc(COMMON_STR_LEN, sizeof(char));
    visited->pos = 0;
    trie_node_t *node = trie->root, *tmp;
    char *_prefix = prefix;
    while (*_prefix && (tmp = node->child[(int)*_prefix])) { 
        // iterate each character of given prefix until it reaches to the end of the prefix. 
        visited->seq[visited->pos] = *_prefix; 
        node = tmp;
        ++_prefix;
        visited->pos++;
        rst ++;
    }
    if (*prefix) {
        // if character iteration of the prefix is not ended, we consider no matching prefix in the trie. 
        return 0;
    } else {
        if (node) {
            // if there are still characters to be iterated, we start collect the result from the node. 
            rst += trie_node_iterate(node, cb, visited, user);
        }
    }
    trie->num_of_comparisons+=rst;
    return rst;
}

int
trie_insert(trie_t *trie, char *key, void *value, size_t vsize, int copy)
{
    stopwatch_t t_locate;
    timer_start(&t_locate);

    trie_node_t *node = trie->root, *tmp;
    int new_nodes = 0;
    while (*key && (tmp = node->child[(int)*key])) { // skip some characters if given key is longer than the existing path. 
            trie->num_of_comparisons++;
            node = tmp;
            ++key;
    }
    timer_start(&t_locate);
    trie->time_to_locate+=timer_delta_ns(&t_locate);

    stopwatch_t t_expand;
    timer_start(&t_expand);
    while (*key) {
        new_nodes++;
        node->num_children++;
        tmp = node->child[(int)*key] = ctr_calloc(1, sizeof(trie_node_t), &trie->mem_usage);
        trie->num_of_reallocs++;
        tmp->parent = node;
        tmp->pidx = *key;
        node = tmp;
        ++key;
    }
    timer_pause(&t_expand);
    trie->time_for_expansion+=timer_delta_ns(&t_expand);

    if (new_nodes) {
        trie->count++;
        trie->node_count += new_nodes;
    }

    if (node)
        trie_node_set_value(trie, node, value, vsize, copy);

    return new_nodes;
}

static inline trie_node_t *
trie_find_internal(trie_t *trie, char *key)
{
    stopwatch_t t_locate;
    timer_start(&t_locate);

    trie_node_t *node;
    for (node = trie->root; *key && node; ++key) {
        trie->num_of_comparisons++;
        node = node->child[(int)*key];
    }

    timer_start(&t_locate);
    trie->time_to_locate+=timer_delta_ns(&t_locate);

    return node;
}

void *
trie_find(trie_t *trie, char *key, size_t *vsize)
{
    trie_node_t *node = trie_find_internal(trie, key);
    if (!node)
        return NULL;

    if (vsize)
        *vsize = node->vsize;

    return node->value;
}

int
trie_remove(trie_t *trie, char *key, void **value, size_t *vsize)
{
    trie_node_t *node = trie_find_internal(trie, key);
    if (!node)
        return 0;

    int num_nodes = trie->node_count;


    if (vsize)
        *vsize = node->vsize;

    if (value) {
        *value = node->value;
        trie_node_destroy(trie, node, NULL);
    } else {
        trie_node_destroy(trie, node, trie->free_value_cb);
    }

    return num_nodes - trie->node_count;
}

int
trie_find_or_insert(trie_t *trie, char *key, void *value, size_t vsize, void **prev_value, size_t *prev_vsize, int copy)
{
    trie_node_t *node = trie_find_internal(trie, key);
    if (!node)
        return trie_insert(trie, key, value, vsize, copy);

    if (prev_value)
        *prev_value = node->value;
    if (prev_vsize)
        *prev_vsize = node->vsize;

    return 0;
}

int
trie_find_and_insert(trie_t *trie, char *key, void *value, size_t vsize, void **prev_value, size_t *prev_vsize, int copy)
{

    trie_node_t *node = trie_find_internal(trie, key);
    if (node) {
        if (prev_value)
            *prev_value = node->value;
        if (prev_vsize)
            *prev_vsize = node->vsize;
        trie_node_set_value(trie, node, value, vsize, copy);
        return 0;
    }

    return trie_insert(trie, key, value, vsize, copy);
}

perf_info_t *get_perf_info_trie(trie_t *index_root){
    GET_PERF_INFO(index_root);
}

void reset_perf_info_counters_trie(trie_t *trie){
    RESET_PERF_INFO_COUNTERS(trie);
}

// size_t get_mem_usage_by_all_tries(){
//     return mem_usage_by_all_tries;
// }
