#ifndef MIQS_SPARSE_ARRAY_NUMBER_INDEX_H
#define MIQS_SPARSE_ARRAY_NUMBER_INDEX_H


int create_sparse_array(void **idx_ptr);

int insert_number_to_sparse_array(void *index_root, void *number, void *data);

int search_number_from_sparse_array(void *index_root, void *key, void **out);

#endif // end MIQS_SPARSE_ARRAY_NUMBER_INDEX_H