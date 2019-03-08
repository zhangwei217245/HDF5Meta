#ifndef MIQS_RBTREE_NUMBER_INDEX_H
#define MIQS_RBTREE_NUMBER_INDEX_H


int create_rbtree_number_index(void **idx_ptr);

int insert_number_to_rbtree(void *index_root, void *key, void *data);

int search_number_from_rbtree(void *index_root, void *key, void **out);

#endif // end MIQS_RBTREE_NUMBER_INDEX_H
