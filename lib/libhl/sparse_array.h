#ifndef MIQS_LIBHL_SPARSE_ARRAY_H
#define MIQS_LIBHL_SPARSE_ARRAY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>
#include "../profile/mem_perf.h"
#include "comparators.h"


#define SPA_SIZE_MIN 128

typedef struct{
    size_t size;        // Overall length of the array
    size_t max_size;    // maximum length of the array that has been specified
    size_t count;       // number of non-empty elements in the array. 
} spa_size_info_t;

typedef void (*spa_free_item_callback_t)(void *);

typedef struct _sparse_array_s sparse_array_t;

typedef enum {
    SPA_ITERATOR_STOP = 0,
    SPA_ITERATOR_CONTINUE = 1
} spa_iterator_status_t;

/**
 * @brief Create a new sparse array descriptor
 * @param initial_size : initial size of the sparse array; if 0 SPA_SIZE_MIN will be used as initial size
 * @param max_size     : maximum size the sparse array can be grown up to
 * @param free_item_cb : the callback to use when an item needs to be released
 * @return a newly allocated and initialized sparse array
 *
 * The sparse array will be expanded if necessary
 */
sparse_array_t *create_sparse_array(size_t initial_size, size_t max_size, spa_free_item_callback_t cb, libhl_cmp_callback_t locate_cb);

/**
 * @brief put data into corresponding element of the array at the specified position
 * @param sparse_arr : a valid pointer to the sparse_array_t structure
 * @param pos : a valid position in the array. 
 * @param data : the data that should be put in the array. 
 * @return 0 on success, -1 otherwise.
 */
int set_element_to_sparse_array(sparse_array_t *sparse_arr, void *pos, void *data);

/**
 * @brief get the data stored in the corresponding space of the array indicated by 'pos'
 * @param sparse_arr : a valid pointer to the sparse_array_t structure
 * @param pos : a valid position in the array. 
 * @return : a valid pointer to the data element in the array specified by 'pos'. 
 */
void *get_element_in_sparse_array(sparse_array_t *sparse_arr, void *pos);

/**
 * @brief get the size information of sparse array
 * @param sparse_arr : a valid pointer to the sparse_array_t structure
 * @return a pointer of type spa_size_info_t, which include the size of the array,
 *  the count of the array, and  the  maximum size of the array .
 */ 
spa_size_info_t *get_sparse_array_size(sparse_array_t *sparse_arr);

/**
 * @brief Callback for the element iterator
 * @param spa : a valid pointer to a sparse_array_t structure
 * @param array_idx : the index of the element in the array.
 * @param element : element that is being visited
 * @param user : user-provided data pointer, as an argument for the spa_elemenmt_iterator() function
 * @return SPA_ITERATOR_CONTINUE to go ahead with the iteration,
 *         SPA_ITERATOR_STOP to stop the iteration,
 *         SPA_ITERATOR_REMOVE to remove the current item from the table and go ahead with the iteration
 *         SPA_ITERATOR_REMOVE_AND_STOP to remove the current item from the table and stop the iteration
 * 
 */
typedef spa_iterator_status_t (*spa_iterator_callback_t)(sparse_array_t *spa, size_t array_idx, void *element, void *user);

/**
 * @brief element iterator. Any invalid value of begin and end will cause full iteration on the entire array.
 * @param sparse_arr : A valid pointer to an sparse_array_t structure
 * @param begin : beginning of a range, inclusive
 * @param end : end of a range, exclusive
 * @param cb    : an spa_iterator_callback_t function
 * @param user  : A pointer which will be passed to the iterator callback at each call
 */
void spa_foreach_elements(sparse_array_t *sparse_arr, void *begin, void *end, spa_iterator_callback_t cb, void *user);

/**
 * Get memory usage by sparse array. 
 * 
 */
perf_info_t * get_perf_info_sparse_array(sparse_array_t *spa);

void reset_perf_info_counters_sparse_array(sparse_array_t *spa);

#ifdef __cplusplus
}
#endif

#endif // END MIQS_LIBHL_SPARSE_ARRAY_H