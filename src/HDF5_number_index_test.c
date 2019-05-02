// #define ENABLE_MPI

#include <math.h>
#include "../lib/index_spi/spi.h"
#include "../lib/utils/timer_utils.h"
#include "../lib/utils/string_utils.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

long *generating_even_numbers(int count){
    long *rst = calloc(count, sizeof(long));
    int i = 0;
    for (i =0; i < count; i++) {
        rst[0] = (long)(1000 + i);
    }
    return rst;
}

long *generating_skew_numbers(int count){
    return generating_even_numbers(count);
}

int main(int argc, const char *argv[]){

    int i, j; 
    int rank = 0, size = 1;

    int count = 2000;
    char *dataset_name = "";
    if (argc >= 2) {
        dataset_name = (char *)argv[1];
    }
    if (argc >= 3) {
        count = atoi(argv[2]);
    }

    if (argc >= 4) {
        rank = atoi(argv[3]);
    }

    // int pwr = rank / 4;

    // count = count * (long)pow(10.0, (double)pwr);

    long *keys;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (rank % 4 == 0) {
        setenv(MIQS_NUMBER_IDX_VAR_NAME, "SPARSEARRAY", 1);
    } else if (rank % 4 == 1 ) {
        setenv(MIQS_NUMBER_IDX_VAR_NAME, "SBST", 1);
    } else if (rank % 4 == 2) {
        setenv(MIQS_NUMBER_IDX_VAR_NAME, "SKIPLIST", 1);
    } else if (rank %4 == 3) {
        setenv(MIQS_NUMBER_IDX_VAR_NAME, "SBST", 1);
    }

    // char *alpha = "Hello ";
    // char *beta = "World!";
    // char *gamma = " This is a program!";
    // println("%s", concat(alpha, beta, gamma));

    

    if (strcmp(dataset_name, "SKEW")==0) {
        keys = generating_skew_numbers(count);
    } else {
        keys = generating_even_numbers(count);
    } 

    void *index_root;
    create_number_index(&index_root, libhl_cmp_keys_long);
    stopwatch_t time_to_insert;
    timer_start(&time_to_insert);
    for (i = 0; i < count; i++) {
        insert_number(index_root, &keys[i], sizeof(long), &keys[i]);
    }
    timer_pause(&time_to_insert);
    suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
    perf_info_t *perf_info = get_number_ds_perf_info(index_root);
    size_t ds_mem = perf_info->mem_usage;
    uint64_t n_comp = perf_info->num_of_comparisons;
    uint64_t n_realloc = perf_info->num_of_reallocs;
    stw_nanosec_t t_locate=perf_info->time_to_locate;
    stw_nanosec_t t_expand=perf_info->time_for_expansion;
    println("[Total] Insert %d keys into %s took %ld us. %llu memory consumed, %llu comparisons, %llu reallocations, %llu ns for locate, %llu ns for expansion", 
    count,  getenv(MIQS_NUMBER_IDX_VAR_NAME), index_insertion_duration, ds_mem, n_comp, n_realloc, t_locate, t_expand);

    stopwatch_t time_to_search;
    timer_start(&time_to_search);
    for (i = 0; i < count; i++) {
        void *out;
        search_number(index_root, &keys[i], sizeof(long), &out);
    }
    timer_pause(&time_to_search);
    suseconds_t index_search_duration = timer_delta_us(&time_to_search);
    perf_info = get_number_ds_perf_info(index_root);
    n_comp = perf_info->num_of_comparisons;
    t_locate = perf_info->time_to_locate;
    println("[Total] time to search %d keys in %s is %ld us. %llu ns for locate. %llu comparisons", 
    count, getenv(MIQS_NUMBER_IDX_VAR_NAME), index_search_duration, t_locate, n_comp);

    timer_start(&time_to_search);
    for (i = 0; i < count; i++) {
        void *out;
        long end = keys[i]+20;
        search_numeric_range(index_root, &keys[i], sizeof(long), 
            &end, sizeof(long));
    }
    timer_pause(&time_to_search);
    index_search_duration = timer_delta_us(&time_to_search);
    println("[Total] time for range query %d keys in %s is %ld us.", 
    count, getenv(MIQS_NUMBER_IDX_VAR_NAME), index_search_duration);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}