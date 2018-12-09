#ifndef MIQS_HDF5_INDEX
#define MIQS_HDF5_INDEX

#include "new_hdf5_meta.h"
#include "../ds/art.h"
#include "../ds/bplustree.h"
#include "../ds/hashset.h"
#include <search.h>

// For index creation,
// This is the opdata that will be passed to on_obj and on_attr functions.
typedef struct {
    char *file_path;
    char *obj_path;
    hid_t object_id;
    art_tree *root_art;

    // info for query and profiling
    char **indexed_attr;
    int num_indexed_field;

    //info solely for profiling.
    size_t total_num_files;
    size_t total_num_objects;
    size_t total_num_attrs;
    suseconds_t us_to_index;
} index_anchor;


typedef struct {
    int is_numeric;
    int is_float;
    struct bplus_tree *bpt;
    art_tree *art;
}art_leaf_content_t;

typedef struct {
    int is_numeric;
    int is_float;
    void ***bpt;
    art_tree *art;
}attr_tree_leaf_content_t;

typedef struct {
    void *k;
    // map_t path_hash_map;
    art_tree *file_path_art;
} value_tree_leaf_content_t;

typedef struct {
    art_tree *file_path_art;
    art_tree *obj_path_art;
} path_bpt_leaf_cnt_t;

typedef struct {
    char *file_path;
    int num_objs;
    hid_t *obj_ids;
} search_result_t;

typedef struct {
    int num_files;
    search_result_t *rst_arr;
}power_search_rst_t;



extern size_t get_btree_mem_size();

/**
 * The content of value tree leaf node should be a hashmap. 
 * The keys of this hashmap are the file paths 
 * The value of each key should be a set of object IDs.
 */

void parse_hdf5_file(char *filepath, index_anchor *idx_anchor);


int int_value_search(index_anchor *idx_anchor, char *attr_name, int value, search_result_t **rst);

int float_value_search(index_anchor *idx_anchor, char *attr_name, double value, search_result_t **rst);

int string_value_search(index_anchor *idx_anchor, char *attr_name, char *value, search_result_t **rst);

size_t get_index_size();


#endif /* !MIQS_HDF5_INDEX */
