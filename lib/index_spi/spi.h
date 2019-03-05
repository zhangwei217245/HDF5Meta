#ifndef MIQS_DS_SPI_H
#define MIQS_DS_SPI_H

#include "../include/base_stdlib.h"
#include "../utils/string_utils.h"


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


int destroy_string_index(void **index_ptr);


int create_number_index(void **idx_ptr);

/**
 * insert a number into an index with given data
 */
int insert_number(void *index_root, void *key, void *data);

/**
 * delete a number from an index
 */
int delete_number(void *index_root, void *key);

/**
 * update a number on the index with given data
 */
int update_number(void *index_root, void *key, void *newdata);

/**
 * search a number on the index for related data. 
 */
int search_number(void *index_root, void *key, void **out);


int destroy_number_index(void **idx_ptr);

#endif // endif  MIQS_DS_SPI_H