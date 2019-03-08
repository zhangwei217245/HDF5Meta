#ifndef MIQS_TSEARCH_NUMBER_INDEX_H
#define MIQS_TSEARCH_NUMBER_INDEX_H


int create_tsearch_idx(void **idx_ptr);

int insert_number_to_tsearch_idx(void *index_root, void *key, void *data);

int search_number_from_tsearch_idx(void *index_root, void *key, void **out);

#endif // END MIQS_TSEARCH_NUMBER_INDEX_H
