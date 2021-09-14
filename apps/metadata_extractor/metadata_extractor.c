#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "miqs_indexing.h"
#include "miqs_querying.h"
#include "utils/timer_utils.h"
#include "utils/string_utils.h"

#include "metadata/miqs_metadata.h"
#include "metadata/miqs_meta_collector.h"
#include "metadata/hdf5_meta_extractor.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int on_obj(void *opdata, miqs_data_object_t *obj){
    // index_anchor_t *idx_anchor = (index_anchor_t *)opdata;
    // size_t path_len = strlen(obj->obj_name)+1;
    // idx_anchor->obj_path = (char *)ctr_calloc(path_len+1, sizeof(char), get_index_size_ptr());
    // strncpy(idx_anchor->obj_path, obj->obj_name, path_len);
    // idx_anchor->object_id = (void *)obj->obj_id;
    // idx_anchor->total_num_objects+=1;
    return 1;
}

int on_attr(void *opdata, miqs_meta_attribute_t *attr){
    // index_anchor_t *idx_anchor = (index_anchor_t *)opdata;
    // attr->file_path_str = idx_anchor->file_path;
    // attr->obj_path_str = idx_anchor->obj_path;
    int rst = 0;
    // rst = indexing_attr(idx_anchor, attr);
    return rst;
}


int
main (int argc, char **argv)
{
    int rank = 0, size = 1;
    int rst = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    printf("%d out of %d\n", rank, size);

    int i = 0;
    for (i = 0; i < argc; i++) {
        printf("%d %s\n", i, argv[i]);
    }

    // miqs_metadata_collector_t *meta_collector = (miqs_metadata_collector_t *)ctr_calloc(1, sizeof(miqs_metadata_collector_t), get_index_size_ptr());
    // init_metadata_collector(meta_collector, 0, (void *)root_idx_anchor(), NULL, on_obj, on_attr);

    // scan_hdf5(filepath, meta_collector, 0);

#ifdef ENABLE_MPI
    rst = MPI_Finalize();
#endif
    return rst;
}