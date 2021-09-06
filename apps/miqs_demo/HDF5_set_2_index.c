#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "miqs_indexing.h"
#include "miqs_querying.h"
#include "utils/timer_utils.h"
// #include "string_utils.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


int num_indexed_field = 0; //number of attributes to be indexed.


void print_usage(){
    printf("Usage: ./hdf5_set_2_index /path/to/hdf5/file topk num_indexed_fields parallelism /path/to/index/files\n");
}

indexing_config_t *parse_args(int argc, char *argv[], int rank, int size){
    if (argc < 2) {
        print_usage();
        return 0;
    }
    int topk = 0; // number of files to be scanned.
    int parallelism = 1;

    int persistence_type = 1; // none = 0; 1 = mdb, aof = 2

    char *dataset_path = argv[1];

    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    if (argc >= 5){
        parallelism = atoi(argv[4]);
    }

    char *index_dir_path = argv[5];

    indexing_config_t *rst = init_indexing_config(parallelism, rank, size, topk, 1, 0, dataset_path, index_dir_path);
    return rst;
}

void get_attribute_value_ptr(char *value_str, miqs_attr_value_type_t value_type, void **output){
    if (value_type ==  MIQS_ATV_INTEGER) {
        int value = atoi(value_str);
        output[0] = (void *)&value;
    } else if (value_type ==  MIQS_ATV_FLOAT){
        double value = atof(value_str);
        output[0] = (void *)&value;
    } else {
        output[0] = (void *)value_str;
    }
}

int 
main(int argc, char *argv[])
{
    int rank = 0, size = 1;
    int rst = 1;

    #ifdef ENABLE_MPI
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
    #endif

    char *indexed_attr[]={
        "AUTHOR", 
        "BESTEXP", 
        "HELIO_RV",
        "FILENAME", 
        "DARKTIME", 
        "IOFFSTD",
        "EXPOSURE", 
        "BADPIXEL", 
        "CRVAL1",
        "LAMPLIST",
        "COLLB", 
        "M1PISTON",
        "COMMENT",
        "HIGHREJ",
        "FBADPIX2", 
        "DAQVER",
        NULL};
    char *search_values[]={
        "Scott Burles & David Schlegel",
        "103179", 
        "26.6203",
        "badpixels-56149-b1.fits.gz", 
        "0", 
        "0.0133138",
        "sdR-b2-00154990.fit", 
        "155701", 
        "3.5528",
        "lamphgcdne.dat",
        "26660", 
        "661.53",
        "sp2blue cards follow",
        "8",
        "0.231077", 
        "1.2.7",
        NULL};

    //  string value = 0, int value = 1, float value = 2
    miqs_attr_value_type_t search_types[] = {MIQS_ATV_STRING,MIQS_ATV_INTEGER,MIQS_ATV_FLOAT,
                          MIQS_ATV_STRING,MIQS_ATV_INTEGER,MIQS_ATV_FLOAT,
                          MIQS_ATV_STRING,MIQS_ATV_INTEGER,MIQS_ATV_FLOAT,
                          MIQS_ATV_STRING,MIQS_ATV_INTEGER,MIQS_ATV_FLOAT,
                          MIQS_ATV_STRING,MIQS_ATV_INTEGER,MIQS_ATV_FLOAT,
                          MIQS_ATV_STRING};
    
    indexing_config_t *param = parse_args(argc, argv, rank, size);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    // 1. Try loading index files
    int load_rst = recovering_index(param);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    // 2. if fail loading, build from scratch
    if (load_rst != 0) {
        indexing_data_collection(param);
    }


#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    // 3. do search test.
    int num_queries = 16;
    if (num_indexed_field > 0) {
        num_queries = num_indexed_field;
    }

    void **value_ptr_container = calloc(2, sizeof(void *));

    stopwatch_t timer_search;
    timer_start(&timer_search);
    int numrst = 0;
    int i = 0;
    for (i = 0; i < 1024; i++) {
        int c = i % 16;
        get_attribute_value_ptr(search_values[c], search_types[c], value_ptr_container);
        power_search_rst_t *search_rst = exact_metadata_search(indexed_attr[c], value_ptr_container[0], search_types[c]);
        numrst += (search_rst->size);
    }
    timer_pause(&timer_search);
    printf("[META_SEARCH_MEMO] Rank %d : Time for 1024 queries on %d indexes and spent %d microseconds.  %d\n", 
    rank, num_indexed_field, timer_delta_us(&timer_search), numrst);

   
#ifdef ENABLE_MPI
    rst = MPI_Finalize();
#endif
    return rst;
}