#ifndef MIQS_RBTREE_NUMBER_INDEX_H
#define MIQS_RBTREE_NUMBER_INDEX_H


#include <stdlib.h>
#include "../libhl/rbtree.h"
#include "../libhl/linklist.h"


int create_rbtree_number_index(void **idx_ptr, libhl_cmp_callback_t cb);

int insert_number_to_rbtree(void *index_root, void *key, size_t ksize, void *data);

int search_number_from_rbtree(void *index_root, void *key, size_t ksize, void **out);

linked_list_t *search_numeric_range_from_rbtree(void *index_root, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size);


// Formally described : Time complexity(worst case, best case, avg case), 
// space complexity on differnet data structures, in the context of different queries. 


// size_t get_mem_in_number_rbtree();

#endif // end MIQS_RBTREE_NUMBER_INDEX_H
