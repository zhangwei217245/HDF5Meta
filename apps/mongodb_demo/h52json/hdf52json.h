/* File foo.  */
#ifndef MIQS_HDF5_2_JSON_H
#define MIQS_HDF5_2_JSON_H

#include "metadata/hdf5_meta_extractor.h"
#include <json-c/json.h>
// #include "json.h"
#include <libgen.h>

void parse_hdf5_file(char *filepath, json_object **out);

void parse_hdf5_meta_as_json_str(char *filepath, char **result);

#endif /* !MIQS_HDF5_2_JSON_H */