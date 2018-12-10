#ifndef MIQS_IN_MEM_INDEX
#define MIQS_IN_MEM_INDEX

#include "hdf5.h"
#include "../ds/art.h"
#include "../ds/bplustree.h"
#include "../ds/hashset.h"
#include <search.h>
#include "on_disk_index.h"



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

    // for on-disk index file name
    FILE *on_disk_file_stream;
    int is_readonly_index_file;

    //info solely for profiling.
    size_t total_num_files;
    size_t total_num_objects;
    size_t total_num_attrs;
    size_t total_num_indexed_kv_pairs;
    size_t total_num_kv_pairs;
    suseconds_t us_to_index;
    suseconds_t us_to_disk_index;
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
    char **obj_paths;
} search_result_t;

typedef struct {
    int num_files;
    search_result_t **rst_arr;
}power_search_rst_t;


index_anchor *root_idx_anchor();

extern size_t get_btree_mem_size();

size_t *get_index_size_ptr();

size_t get_index_size();

int int_value_compare_func(const void *l, const void *r);

int float_value_compare_func(const void *l, const void *r);

int init_in_mem_index();
/**
 * Create index based on given index_record_t.
 */
int indexing_record(index_record_t *ir);

/**
 * This is a function for indexing numeric fields in the HDF5 metadata. 
 * We deal with two different data types, one is integer, the other one is double. 
 * 1. Go through attribute_name art_tree, and we get the pointer for value B-tree
 * 2. After transforming the value to int/double value, we go through value B-tree(tsearch)
 * 3. At the leaf nodes of B-tree, we go through the ART tree again to collect all file paths
 *    which contains the result. 
 * 4. 
 */ 
void indexing_numeric(char *attr_name, void *attr_val, int attribute_value_length,
int (*compare_func)(const void *a, const void *b), 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);


void indexing_string(char *attr_name, char **attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);



int int_value_search(char *attr_name, int value, search_result_t **rst);

int float_value_search(char *attr_name, double value, search_result_t **rst);

int string_value_search(char *attr_name, char *value, search_result_t **rst);





#endif /* !MIQS_IN_MEM_INDEX */