#ifndef MIQS_ART_STRING_INDEX_H
#define MIQS_ART_STRING_INDEX_H

#include "../ds/art.h"
#include "../libhl/linklist.h"
#include "../utils/string_utils.h"


int create_art(void **idx_ptr);

int insert_string_to_art(void *index_root, char *key, void *data);

int search_string_in_art(void *index_root, char *key, size_t len, void **out);

linked_list_t *search_affix_in_art(void *index_root, pattern_type_t afx_type, char *affix);

// size_t get_mem_in_art();

#endif // ENDIF MIQS_TRIE_STRING_INDEX_H