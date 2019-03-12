#ifndef MIQS_RBTREE_STRING_INDEX_H
#define MIQS_RBTREE_STRING_INDEX_H

#include <stdlib.h>
#include "../libhl/rbtree.h"


int create_rbtree(void **idx_ptr);

int insert_string_to_rbtree(void *index_root, char *key, void *data);

int search_string_in_rbtree(void *index_root, char *key, size_t len, void **out);

size_t get_mem_in_rbtree();

#endif // ENDIF MIQS_RBTREE_STRING_INDEX_H