#ifndef MIQS_DS_SPI_H
#define MIQS_DS_SPI_H


#include "../../utils/cbase/base_stdlib.h"
#include "../../utils/string_utils.h"
#include "../libhl/comparators.h"
#include "../libhl/linklist.h"
#include "../../utils/profile/mem_perf.h"

#include "hash_table_string_index.h"
#include "rbtree_string_index.h"
#include "trie_string_index.h"
#include "art_string_index.h"


#include "sparse_array_number_index.h"
#include "rbtree_number_index.h"
#include "skiplist_number_index.h"
#include "tsearch_number_index.h"


#define MIQS_STRING_IDX_VAR_NAME "MIQS_STR_IDX_IMPL"
#define MIQS_NUMBER_IDX_VAR_NAME "MIQS_NUM_IDX_IMPL"




/**
 * create index data structure and initialize it. 
 */
int create_string_index(void **idx_ptr);

/**
 * insert a string to a string wise index
 */
int insert_string(void *index_root, char *key, void *data);

/**
 * delete a string from a string wise index
 */
int delete_string(void *index_root, char *key, void *data);

/**
 * update a string for a string key with a new data item
 */
int update_string(void *index_root, char *key, void *newdata);

/**
 * search a data item with a given string of specified length. 
 * if len = 0, it is an exact search. Otherwise, it is an affix search.
 */
int search_string(void *index_root, char *key, int len, void **out);


linked_list_t *search_affix(void *index_root, pattern_type_t afx_type, char *affix);

/**
 * Get string data structure memory consumption
 */
perf_info_t *get_string_ds_perf_info(void *index_root);

void reset_string_ds_perf_info_counters(void *index_root);

int destroy_string_index(void **index_ptr);


int create_number_index(void **idx_ptr, DATA_TYPE num_type);

/**
 * insert a number into an index with given data
 */
int insert_number(void *index_root, void *key, size_t ksize, void *data);

/**
 * delete a number from an index
 */
int delete_number(void *index_root, void *key, size_t ksize);

/**
 * update a number on the index with given data
 */
int update_number(void *index_root, void *key, size_t ksize, void *newdata);

/**
 * search a number on the index for related data. 
 */
int search_number(void *index_root, void *key, size_t ksize, void **out);


linked_list_t *search_numeric_range(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size);

int destroy_number_index(void **idx_ptr);

perf_info_t *get_number_ds_perf_info(void *index_root);

void reset_number_ds_perf_info_counters(void *index_root);

#endif // endif  MIQS_DS_SPI_H