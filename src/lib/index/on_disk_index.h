#ifndef MIQS_ON_DISK_INDEX
#define MIQS_ON_DISK_INDEX
#include "../dao/bin_file_ops.h"
#include "../libhl/linklist.h"
#include "../libhl/rbtree.h"
#include "../ds/art.h"
#include "../fs/fs_ops.h"
#include "../utils/string_utils.h"
#include "../metadata/miqs_metadata.h"
#include "../include/base_stdlib.h"
#include <sys/stat.h>
#include <unistd.h>
// #include "in_mem_index.h"

// #define MIQS_INDEX_CONCURRENT_LEVEL 1

typedef struct {
    int is_numeric;
    int is_float;
    // void ***bpt;
    rbt_t *rbt;
    art_tree *art;

    void *secondary_idx;

#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_t VALUE_TREE_LOCK;
#else
        /* nothing here for tree-node protection */
    #endif

    // size_t file_path_pos;
    // size_t obj_path_pos;
} attr_tree_leaf_content_t;

typedef struct {
    // void *k;
    // map_t path_hash_map;
    // art_tree *file_path_art;
    linked_list_t *file_obj_pair_list;

#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_t VALUE_TREE_LEAF_LOCK;
#else
        /* nothing here for tree-node protection */
    #endif
} value_tree_leaf_content_t;

typedef struct {
    size_t file_list_pos;
    size_t obj_list_pos;
} file_obj_pair_t;

typedef struct{
    miqs_attr_type_t type; // type: 1, int, 2, float, 3. string
    char *name;
    void *data;
    char *file_path;
    char *object_path;
} index_record_t;

/**
 * Create index record in memory.
 * 
 */
index_record_t *create_index_record(miqs_attr_type_t type, char *name, void *data, char *file_path, char *object_path);

/**
 *  append index_record to the current position of the stream
 *  users can call fseek to adjust the position of the file pointer to the end of the file. 
 */
void append_index_record(index_record_t *ir, FILE *stream);

/**
 * Print the index record just for visual verification.
 * 
 */
void print_index_record(index_record_t ir);

/**
 * Search over disk, from the beginning of a file, and collect result matching given compare function
 * 
 */
index_record_t **find_index_record(char *name,
        int (*compare_value)(const void *data, const void *criterion), void *criterion,
        FILE *stream, int *out_len);

/**
 * Read one index record from current position of the file.
 */
index_record_t *read_index_record(FILE *stream);

int int_equals(const void *data, const void *criterion);
int double_equals(const void *data, const void *criterion);
int string_equals(const void *data, const void *criterion);

int append_attr_root_tree(art_tree *art, FILE *stream);
int append_path_list(linked_list_t *list, FILE *stream);

int read_into_path_list(linked_list_t *list, FILE *stream);
int read_into_attr_root_tree(art_tree **art_arr, int parallelism, FILE *stream);

size_t get_num_kv_pairs_loaded_mdb();
size_t get_num_attrs_loaded_mdb();

int is_aof(const struct dirent *entry);

int is_mdb(const struct dirent *entry);

#endif /* !MIQS_ON_DISK_INDEX */