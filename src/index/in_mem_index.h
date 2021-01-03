#ifndef MIQS_IN_MEM_INDEX
#define MIQS_IN_MEM_INDEX

// #include "hdf5.h"
#include "art/art.h"
#include "../utils/string_utils.h"
#include "libhl/linklist.h"
#include "libhl/rbtree.h"
#include "../metadata/miqs_metadata.h"
#include "on_disk_index.h"
#include <miqs_config.h>




// For index creation,
// This is the opdata that will be passed to on_obj and on_attr functions.
typedef struct {
    char *file_path;
    char *obj_path;
    void *object_id;
    int parallelism;
    art_tree **root_art_array;

    /**
     * 1, mutual exclusion on the entire index
     * 2, read/write lock on attr name and attr value 
     * 3, node-level read/write locks
     */

    pthread_rwlock_t *GLOBAL_INDEX_LOCK;


    //Collections of file_paths and object_paths
    //We use tagged entry along with its position
    //So, in leaf node of value tree, we only maintain the position number of the file_path/object_path
    linked_list_t **file_paths_list;
    linked_list_t **object_paths_list;

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
} index_anchor_t;

typedef struct{
    size_t metadata_size;
    size_t overall_index_size;
} mem_cost_t;

index_anchor_t *root_idx_anchor();

extern size_t get_btree_mem_size();

size_t *get_index_size_ptr();

size_t get_index_size();

/**
 * The caller of this function should be the main thread
 */
int init_in_mem_index(int _parallelism);
/**
 * Create index based on given index_record_t.
 */
int indexing_record(index_record_t *ir);


int indexing_attr(index_anchor_t *idx_anchor, miqs_meta_attribute_t *attr);

void create_in_mem_index_for_attr(index_anchor_t *idx_anchor, miqs_meta_attribute_t *attr);

/**
 * This is a function for indexing numeric fields in the HDF5 metadata. 
 * We deal with two different data types, one is integer, the other one is double. 
 * 1. Go through attribute_name art_tree, and we get the pointer for value B-tree
 * 2. After transforming the value to int/double value, we go through value B-tree(tsearch)
 * 3. At the leaf nodes of B-tree, we go through the ART tree again to collect all file paths
 *    which contains the result. 
 * 4. 
 */ 
void indexing_numeric(size_t parallel_index_pos, void *attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);


void indexing_string(size_t parallel_index_pos, char **attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt);



void convert_index_record_to_in_mem_parameters(index_anchor_t *idx_ancr, miqs_meta_attribute_t *attr, index_record_t *ir);

int dump_mdb_index_to_disk(char *filename, index_anchor_t *idx_ancr);
int load_mdb_file_to_index(char *filename, index_anchor_t *idx_ancr);

void write_aof(FILE *stream, miqs_meta_attribute_t *attr, char *file_path, char *obj_path);
/**
 * Loading MDB file into in-memory index. 
 * The loading process will initialize the in-memory index. 
 */
int load_mdb(char *filepath, index_anchor_t *idx_ancr);
/**
 * AOF must be load only after the in-memory index is initialized.
 */
int load_aof(char *filepath, index_anchor_t *idx_ancr);



#endif /* !MIQS_IN_MEM_INDEX */