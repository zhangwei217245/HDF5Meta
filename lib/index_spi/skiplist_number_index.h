#ifndef MIQS_SKIPLIST_NUMBER_INDEX_H
#define MIQS_SKIPLIST_NUMBER_INDEX_H

int create_skiplist_index(void **idx_ptr);

int insert_number_to_skiplist(void *index_root, void *key, void *data);

int search_number_from_skiplist(void *index_root, void *key, void **out);

#endif //end MIQS_SKIPLIST_NUMBER_INDEX_H