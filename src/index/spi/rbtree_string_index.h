#ifndef MIQS_RBTREE_STRING_INDEX_H
#define MIQS_RBTREE_STRING_INDEX_H

#include <stdlib.h>
#include "../libhl/rbtree.h"
#include "../libhl/linklist.h"
#include "../../utils/string_utils.h"
#include "../../utils/profile/mem_perf.h"


int create_rbtree(void **idx_ptr);

int insert_string_to_rbtree(void *index_root, char *key, void *data);

int search_string_in_rbtree(void *index_root, char *key, size_t len, void **out);

linked_list_t *search_affix_in_rbtree(void *index_root, pattern_type_t afx_type, char *affix);

perf_info_t *get_perf_info_sbst(rbt_t *index_root);

// size_t get_mem_in_rbtree();

#endif // ENDIF MIQS_RBTREE_STRING_INDEX_H