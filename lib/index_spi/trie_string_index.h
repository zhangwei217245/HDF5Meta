#ifndef MIQS_TRIE_STRING_INDEX_H
#define MIQS_TRIE_STRING_INDEX_H


#include "../libhl/trie.h"


int create_trie(void **idx_ptr);

int insert_string_to_trie(void *index_root, char *key, void *data);

int search_string_in_trie(void *index_root, char *key, size_t len, void **out);

size_t get_mem_in_trie();

#endif // ENDIF MIQS_TRIE_STRING_INDEX_H