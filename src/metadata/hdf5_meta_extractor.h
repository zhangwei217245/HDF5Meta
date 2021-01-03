#ifndef MIQS_HDF5_META_EXTRACTOR_H
#define MIQS_HDF5_META_EXTRACTOR_H

#include <hdf5.h>
#include "miqs_meta_collector.h"


miqs_object_type_t get_miqs_type_from_h5(H5O_type_t obj_type);

/**
 * This function recursively go through all objects in a single HDF5 file
 * And calling action function in meta_collector, passing its data field as parameter.
 * 
 * @file_path           : the path of HDF5 file
 * @meta_collector      : the metadata collector 'object' including data field and action method. 
 */
int scan_hdf5(char *file_path, miqs_metadata_collector_t *meta_collector, int is_visit_link);

size_t get_hdf5_meta_size();

#endif /* !MIQS_HDF5_META_EXTRACTOR_H */



