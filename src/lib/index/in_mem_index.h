#ifndef MIQS_IN_MEM_INDEX
#define MIQS_IN_MEM_INDEX

// #include "hdf5.h"
#include "../ds/art.h"
// #include "../ds/bplustree.h"
#include "../ds/hashset.h"
#include "../utils/string_utils.h"
#include "../libhl/linklist.h"
#include "../libhl/rbtree.h"
#include "../metadata/miqs_metadata.h"
// #include <search.h>
#include "on_disk_index.h"

// #define GETTER_SETTER_NAME(_GS, _G_FD_NAME) _GS##_G_FD_NAME
// #define GETTER_SETTER_DECLARE(_type, G_FD_NAME) _type GETTER_SETTER_NAME(get, G_FD_NAME) (index_anchor *idx_anchor);\
// void GETTER_SETTER_NAME(set, G_FD_NAME)(index_anchor *idx_anchor, _type G_FD_NAME);


// For index creation,
// This is the opdata that will be passed to on_obj and on_attr functions.
typedef struct {
    char *file_path;
    char *obj_path;
    void *object_id;
    art_tree *root_art;

    /**
     * 1, mutual exclusion on the entire index
     * 2, read/write lock on attr name and attr value 
     * 3, node-level read/write locks
     */
#if MIQS_INDEX_CONCURRENT_LEVEL==1
        pthread_rwlock_t GLOBAL_INDEX_LOCK;
#elif MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_t TOP_ART_LOCK;
        pthread_rwlock_t LOWER_LEVEL_LOCK;
#else
        /* nothing here for tree-node protection */
#endif

    //Collections of file_paths and object_paths
    //We use tagged entry along with its position
    //So, in leaf node of value tree, we only maintain the position number of the file_path/object_path
    linked_list_t *file_paths_list;
    linked_list_t *object_paths_list;

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


typedef struct{
    index_anchor *idx_anchor;
    int rank;
    int size;
    int is_building;
    int current_file_count;
}index_file_loading_param_t;

// GETTER_SETTER_DECLARE(char         *,file_path                 );
// GETTER_SETTER_DECLARE(char         *,obj_path                  );
// GETTER_SETTER_DECLARE(hid_t        ,object_id                 );
// GETTER_SETTER_DECLARE(art_tree     *,root_art                  );
// GETTER_SETTER_DECLARE(linked_list_t *,file_paths_list           );
// GETTER_SETTER_DECLARE(linked_list_t *,object_paths_list         );
// GETTER_SETTER_DECLARE(char         **,indexed_attr              );
// GETTER_SETTER_DECLARE(int          ,num_indexed_field         );
// GETTER_SETTER_DECLARE(FILE         *,on_disk_file_stream       );
// GETTER_SETTER_DECLARE(int          ,is_readonly_index_file    );
// GETTER_SETTER_DECLARE(size_t, total_num_files)
// GETTER_SETTER_DECLARE(size_t, total_num_objects)
// GETTER_SETTER_DECLARE(size_t, total_num_attrs)
// GETTER_SETTER_DECLARE(size_t, total_num_indexed_kv_pairs)
// GETTER_SETTER_DECLARE(size_t, total_num_kv_pairs)
// GETTER_SETTER_DECLARE(suseconds_t, us_to_index)
// GETTER_SETTER_DECLARE(suseconds_t, us_to_disk_index)

typedef struct {
    char *file_path;
    int num_objs;
    char **obj_paths;
} search_result_t;

typedef struct {
    char *file_path;
    char *obj_path;
} search_rst_entry_t;

typedef struct {
    size_t num_files;
    // search_result_t **rst_arr;
    linked_list_t *rst_arr;
}power_search_rst_t;


typedef struct{
    size_t metadata_size;
    size_t overall_index_size;
} mem_cost_t;

index_anchor *root_idx_anchor();

extern size_t get_btree_mem_size();

size_t *get_index_size_ptr();

size_t get_index_size();

int init_in_mem_index();
/**
 * Create index based on given index_record_t.
 */
int indexing_record(index_record_t *ir);


int indexing_attr(index_anchor *idx_anchor, miqs_meta_attribute_t *attr);

void create_in_mem_index_for_attr(index_anchor *idx_anchor, miqs_meta_attribute_t *attr);

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
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);


void indexing_string(char *attr_name, char **attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);



power_search_rst_t *int_value_search(char *attr_name, int value);

power_search_rst_t *float_value_search(char *attr_name, double value);

power_search_rst_t *string_value_search(char *attr_name, char *value);


void convert_index_record_to_in_mem_parameters(index_anchor *idx_anchor, miqs_meta_attribute_t *attr, index_record_t *ir);

int dump_mdb_index_to_disk(char *filename, index_anchor *idx_anchor);
int load_mdb_file_to_index(char *filename, index_anchor *idx_anchor);

void write_aof(FILE *stream, miqs_meta_attribute_t *attr, char *file_path, char *obj_path);
int load_mdb(char *filepath, index_file_loading_param_t *param);
int load_aof(char *filepath, index_file_loading_param_t *param);



#endif /* !MIQS_IN_MEM_INDEX */