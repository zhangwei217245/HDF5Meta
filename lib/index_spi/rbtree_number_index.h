#ifndef MIQS_RBTREE_NUMBER_INDEX_H
#define MIQS_RBTREE_NUMBER_INDEX_H


#include <stdlib.h>
#include "../libhl/rbtree.h"


int create_rbtree_number_index(void **idx_ptr);

int insert_number_to_rbtree(void *index_root, void *key, size_t ksize, void *data);

int search_number_from_rbtree(void *index_root, void *key, size_t ksize, void **out);


size_t get_mem_in_number_rbtree();

#endif // end MIQS_RBTREE_NUMBER_INDEX_H
