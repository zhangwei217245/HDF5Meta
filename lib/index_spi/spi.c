#include "spi.h"
#include "hash_table_string_index.h"
#include "rbtree_string_index.h"
#include "trie_string_index.h"
#include "art_string_index.h"
#include "../utils/timer_utils.h"

// #include "libhl/sparse_array_number_index.h"
// #include "libhl/rbtree_number_index.h"
// #include "libhl/skip_list_number_index.h"
// #include "libhl/b_plus_tree_number_index.h"



/**
 * create index data structure and initialize it. 
 */
int create_string_index(void **idx_ptr){
    int rst = -1;
    const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);
    if (strcmp(s, "HASHTABLE")==0) {
        rst = create_hashtable(idx_ptr);
    } else if (strcmp(s, "SBST")==0) {
        rst = create_rbtree(idx_ptr);
    } else if (strcmp(s, "TRIE")==0) {
        rst = create_trie(idx_ptr);
    } else {
        perror("[CREATE]Data Structure not specified, fallback to ART");
        rst = create_art(idx_ptr);
    }
    return rst;
}


/**
 * insert a string to a string wise index
 */
int insert_string(void *index_root, char *key, void *data){
    int rst = -1;
    const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);

    stopwatch_t time_to_insert;
    timer_start(&time_to_insert);

    if (strcmp(s, "HASHTABLE")==0) {
        rst = insert_string_to_hashtable(void *index_root, char *key, void *data);
    } else if (strcmp(s, "SBST")==0) {
        rst = insert_string_to_rbtree(void *index_root, char *key, void *data);
    } else if (strcmp(s, "TRIE")==0) {
        rst = insert_string_to_trie(void *index_root, char *key, void *data);
    } else {
        perror("[INSERT]Data Structure not specified, fallback to ART");
        rst = insert_string_to_art(void *index_root, char *key, void *data);
    }
    timer_pause(&time_to_insert);
    suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
    printf("[%s]Time to insert is %ld us.\n", s, index_insertion_duration);
    return rst; 
}

/**
 * delete a string from a string wise index
 */
int delete_string(void *index_root, char *key, void *data){
    int rst = -1;
    // const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);
    // if (strcmp(s, "HASHTABLE")==0) {
    //     delete_string_from_hashtable(index_root, key, data);
    // } else if (strcmp(s, "SBST")==0) {
    //     delete_string_from_rbtree(index_root, key, data);
    // } else if (strcmp(s, "TRIE")==0) {
    //     delete_string_from_trie(index_root, key, data);
    // } else {
    //     perror("[DELETE]Data Structure not specified, fallback to ART");
    //     delete_string_from_art(index_root, key, data);
    // }
    return rst; 
}

/**
 * update a string for a string key with a new data item
 */
int update_string(void *index_root, char *key, void *newdata){
    int rst = -1;
    // const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);
    // if (strcmp(s, "HASHTABLE")==0) {
    //     update_string_in_hashtable(index_root, key, newdata);
    // } else if (strcmp(s, "SBST")==0) {
    //     update_string_in_rbtree(index_root, key, newdata);
    // } else if (strcmp(s, "TRIE")==0) {
    //     update_string_in_trie(index_root, key, newdata);
    // } else {
    //     perror("[UPDATE]Data Structure not specified, fallback to ART");
    //     update_string_in_art(index_root, key, newdata);
    // }
    return rst; 
}

/**
 * search a data item with a given string of specified length. 
 * if len = 0, it is an exact search. Otherwise, it is an affix search.
 */
int search_string(void *index_root, char *key, int len, void **out){
    int rst = -1;
    const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);

    stopwatch_t time_to_search;
    timer_start(&time_to_search);

    if (strcmp(s, "HASHTABLE")==0) {
        search_string_in_hashtable(index_root, key, len, out);
    } else if (strcmp(s, "SBST")==0) {
        search_string_in_rbtree(index_root, key, len, out);
    } else if (strcmp(s, "TRIE")==0) {
        search_string_in_trie(index_root, key, len, out);
    } else {
        perror("[SEARCH]Data Structure not specified, fallback to ART");
        search_string_in_art(index_root, key, len, out);
    }

    timer_pause(&time_to_search);
    suseconds_t index_search_duration = timer_delta_us(&time_to_search);
    printf("[%s]Time to search is %ld us.\n", s, index_search_duration);

    return rst; 
}





/**
 * 
 */
int destroy_string_index(void **index_ptr){
    int rst = -1;
    // const char* s = getenv(MIQS_STRING_IDX_VAR_NAME);
    // if (strcmp(s, "HASHTABLE")==0) {
    //     rst = destroy_hashtable(index_ptr);
    // } else if (strcmp(s, "SBST")==0) {
    //     rst = destroy_rbtree(index_ptr);
    // } else if (strcmp(s, "TRIE")==0) {
    //     rst = destroy_trie(index_ptr);
    // } else {
    //     perror("[DESTROY]Data Structure not specified, fallback to ART");
    //     rst = destroy_art(index_ptr);
    // }
    return rst;
}

/**
 * insert a number into an index with given data
 */
int insert_number(void *index_root, void *key, void *data){
    int rst = -1;
    // const char* s = getenv(MIQS_NUMBER_IDX_VAR_NAME);
    // if (strcmp(s, "SPARSEARRAY")==0) {

    // } else if (strcmp(s, "SBST")==0) {

    // } else if (strcmp(s, "SKIPLIST")==0) {

    // } else {
    //     perror("Data Structure not specified, fallback to B+Tree");
    // }
    return rst; 
}

/**
 * delete a number from an index
 */
int delete_number(void *index_root, void *key){
    int rst = -1;
    // const char* s = getenv(MIQS_NUMBER_IDX_VAR_NAME);
    // if (strcmp(s, "SPARSEARRAY")==0) {

    // } else if (strcmp(s, "SBST")==0) {

    // } else if (strcmp(s, "SKIPLIST")==0) {

    // } else {
    //     perror("Data Structure not specified, fallback to B+Tree");
    // }
    return rst; 
}

/**
 * update a number on the index with given data
 */
int update_number(void *index_root, void *key, void *newdata){
    int rst = -1;
    // const char* s = getenv(MIQS_NUMBER_IDX_VAR_NAME);
    // if (strcmp(s, "SPARSEARRAY")==0) {

    // } else if (strcmp(s, "SBST")==0) {

    // } else if (strcmp(s, "SKIPLIST")==0) {

    // } else {
    //     perror("Data Structure not specified, fallback to B+Tree");
    // }
    return rst; 
}

/**
 * search a number on the index for related data. 
 */
int search_number(void *index_root, void *key, void **out){
    int rst = -1;
    // const char* s = getenv(MIQS_NUMBER_IDX_VAR_NAME);
    // if (strcmp(s, "SPARSEARRAY")==0) {

    // } else if (strcmp(s, "SBST")==0) {

    // } else if (strcmp(s, "SKIPLIST")==0) {

    // } else {
    //     perror("Data Structure not specified, fallback to B+Tree");
    // }
    return rst; 
}