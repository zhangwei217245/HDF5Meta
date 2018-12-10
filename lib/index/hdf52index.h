#ifndef MIQS_HDF5_INDEX
#define MIQS_HDF5_INDEX

#include "in_mem_index.h"
#include "../hdf5/new_hdf5_meta.h"


void parse_hdf5_file(char *filepath, index_anchor *idx_anchor);

// /*
//  * Measures the current (and peak) resident and virtual memories
//  * usage of your linux C process, in kB
//  */
// void getMemory(
//     int* currRealMem, int* peakRealMem,
//     int* currVirtMem, int* peakVirtMem);

// void print_mem_usage();



#endif /* !MIQS_HDF5_INDEX */
