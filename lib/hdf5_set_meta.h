#include "hdf5_set_meta.h"

/**
 * This function recursively go through all objects in a single HDF5 file
 * And calling action function in meta_collector, passing its data field as parameter.
 * 
 * @dir_path            : the path of HDF5 file
 * @depth               : depth of recursive directory traversals.
 * @meta_collector      : the metadata collector 'object' including data field and action method. 
 */
int scan_hdf5_set(char *dir_path, int depth, metadata_collector_t *meta_collector, int is_visit_link);
