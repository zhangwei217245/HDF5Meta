#ifndef MIQS_HDF5_INDEX
#define MIQS_HDF5_INDEX

#include "in_mem_index.h"
#include "../hdf5/new_hdf5_meta.h"

typedef struct{
    size_t metadata_size
    size_t overall_index_size
} mem_cost_t;


void convert_index_record_to_in_mem_parameters(index_anchor *idx_anchor, h5attribute_t *attr, index_record_t *ir);


int on_attr(void *opdata, h5attribute_t *attr);

void parse_hdf5_file(char *filepath);

// /*
//  * Measures the current (and peak) resident and virtual memories
//  * usage of your linux C process, in kB
//  */
// void getMemory(
//     int* currRealMem, int* peakRealMem,
//     int* currVirtMem, int* peakVirtMem);

void print_mem_usage();

mem_cost_t *get_mem_cost();



#endif /* !MIQS_HDF5_INDEX */
