#ifndef MIQS_HDF5_INDEX
#define MIQS_HDF5_INDEX

#include "new_hdf5_meta.h"
#include "in_mem_index.h"



/**
 * The content of value tree leaf node should be a hashmap. 
 * The keys of this hashmap are the file paths 
 * The value of each key should be a set of object IDs.
 */

void parse_hdf5_file(char *filepath, index_anchor *idx_anchor);






#endif /* !MIQS_HDF5_INDEX */
