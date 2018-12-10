/* File foo.  */
#ifndef MIQS_HDF5_2_JSON_H
#define MIQS_HDF5_2_JSON_H

#include "new_hdf5_meta.h"
#include <json-c/json.h>
#include <libgen.h>

void parse_hdf5_file(char *filepath, json_object **out);

void parse_hdf5_meta_as_json_str(char *filepath, char **result);

#endif /* !MIQS_HDF5_2_JSON_H */