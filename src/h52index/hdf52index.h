#ifndef MIQS_HDF5_INDEX
#define MIQS_HDF5_INDEX

#include "index/in_mem_index.h"
#include "metadata/hdf5_meta_extractor.h"



void parse_hdf5_file(char *filepath);

void print_mem_usage();
mem_cost_t *get_mem_cost();

#endif /* !MIQS_HDF5_INDEX */
