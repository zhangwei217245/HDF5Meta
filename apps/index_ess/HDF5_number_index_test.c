#define ENABLE_MPI

#include <math.h>
#include "index/spi/spi.h"
#include "utils/timer_utils.h"
#include "utils/string_utils.h"
#include "boss_dataset.h"
#include "sdss_dataset.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

long *generating_even_numbers(int count){
    long *rst = calloc(count, sizeof(long));
    int i = 0;
    for (i =0; i < count; i++) {
        rst[i] = (long)(1000 + i);
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

    long *int_keys;
    double *float_keys;
    int is_float = 0;

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

    int len_key_arr = count;
    libhl_cmp_callback_t cmp_callback = libhl_cmp_keys_long;
    DATA_TYPE dt = INT;

    if (strcmp(dataset_name, "SKEW")==0) {
        int_keys = generating_skew_numbers(count);
    } else if (strcmp(dataset_name, "EXPOSURE")==0){
        cmp_callback = libhl_cmp_keys_long; 
        dt = LONG;
        int_keys = int_EXPOSURE;
        len_key_arr = len_int_vals[0];
    } else if (strcmp(dataset_name, "COLLB")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = int_COLLB;
        len_key_arr = len_int_vals[1];
    } else if (strcmp(dataset_name, "DUSTA")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = int_DUSTA;
        len_key_arr = len_int_vals[2];
    } else if (strcmp(dataset_name, "TILEID")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = int_TILEID;
        len_key_arr = len_int_vals[3];
    } else if (strcmp(dataset_name, "FIBERID")==0){
        cmp_callback = libhl_cmp_keys_long; 
        dt = LONG;
        int_keys = sdss_int_FIBERID;
        len_key_arr = sdss_len_int_vals[0];
    } else if (strcmp(dataset_name, "IMAGE_MINMAXRANGE")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = sdss_int_IMAGE_MINMAXRANGE;
        len_key_arr = sdss_len_int_vals[1];
    } else if (strcmp(dataset_name, "COLUMN_ID")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = sdss_int_COLUMN_ID;
        len_key_arr = sdss_len_int_vals[2];
    } else if (strcmp(dataset_name, "MJD")==0){
        cmp_callback = libhl_cmp_keys_long;
        dt = LONG;
        int_keys = sdss_int_MJD;
        len_key_arr = sdss_len_int_vals[3];
    } else if (strcmp(dataset_name, "DEREDSN2")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = float_DEREDSN2;
        len_key_arr = len_float_vals[0];
    } else if (strcmp(dataset_name, "AZ")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = float_AZ;
        len_key_arr = len_float_vals[1];
    } else if (strcmp(dataset_name, "ARCOFFX")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = float_ARCOFFX;
        len_key_arr = len_float_vals[2];
    } else if (strcmp(dataset_name, "RMSOFF20")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = float_RMSOFF20;
        len_key_arr = len_float_vals[3];
    } else if (strcmp(dataset_name, "PLUG_DEC")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = sdss_float_PLUG_DEC;
        len_key_arr = sdss_len_float_vals[0];
    } else if (strcmp(dataset_name, "PLUG_RA")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = sdss_float_PLUG_RA;
        len_key_arr = sdss_len_float_vals[1];
    } else if (strcmp(dataset_name, "IMAGE_MINMAXRANGE")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = sdss_float_IMAGE_MINMAXRANGE;
        len_key_arr = sdss_len_float_vals[2];
    } else if (strcmp(dataset_name, "SKYCHI2")==0){
        is_float=1;
        cmp_callback = libhl_cmp_keys_double;
        dt = DOUBLE;
        float_keys = sdss_float_SKYCHI2;
        len_key_arr = sdss_len_float_vals[3];
    } else {
        int_keys = generating_even_numbers(count);
        dt = INT;
    } 

    int round = 0;
    for (round = 0; round < 1; round++) {
        void *index_root;
        create_number_index(&index_root, dt);

        int insert_count = len_key_arr-(len_key_arr/4)*(round);

        stopwatch_t time_to_insert;
        timer_start(&time_to_insert);
        if (is_float) {
            for (i = 0; i < insert_count; i++) {
                insert_number(index_root, &float_keys[i], sizeof(double), &float_keys[i]);
            }
        } else {
            for (i = 0; i < insert_count; i++) {
                insert_number(index_root, &int_keys[i], sizeof(long), &int_keys[i]);
            }
        }
        
        timer_pause(&time_to_insert);
        suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
        perf_info_t *perf_info = get_number_ds_perf_info(index_root);
        size_t ds_mem = perf_info->mem_usage;
        uint64_t n_comp = perf_info->num_of_comparisons;
        uint64_t n_realloc = perf_info->num_of_reallocs;
        stw_nanosec_t t_locate=perf_info->time_to_locate;
        stw_nanosec_t t_expand=perf_info->time_for_expansion;
        println("[Total%d] Insert %d keys into %s took %ld us. %llu memory consumed, %llu comparisons, %llu reallocations, %llu ns for locate, %llu ns for expansion", 
        insert_count, insert_count,  getenv(MIQS_NUMBER_IDX_VAR_NAME), index_insertion_duration, ds_mem, n_comp, n_realloc, t_locate, t_expand);

        reset_number_ds_perf_info_counters(index_root);
        // srand(0);

        stopwatch_t time_to_search;
        timer_start(&time_to_search);
        if (is_float) {
            for (i = 0; i < count; i++) {
                void *out;
                int rnd = i%  insert_count;
                search_number(index_root, &float_keys[rnd], sizeof(double), &out);
            }
        } else {
            for (i = 0; i < count; i++) {
                void *out;
                int rnd = i %  insert_count;
                search_number(index_root, &int_keys[rnd], sizeof(long), &out);
            }
        }
        
        timer_pause(&time_to_search);
        suseconds_t index_search_duration = timer_delta_us(&time_to_search);
        perf_info = get_number_ds_perf_info(index_root);
        n_comp = perf_info->num_of_comparisons;
        t_locate = perf_info->time_to_locate;
        println("[Total%d] time to search %d keys in %s is %ld us. %llu ns for locate. %llu comparisons", 
        insert_count, count, getenv(MIQS_NUMBER_IDX_VAR_NAME), index_search_duration, t_locate, n_comp);

        int range_size_arr[]={20, 40,  60};
        int k  = 0;
        for (k = 0; k < 3; k++) {
            reset_number_ds_perf_info_counters(index_root);

            linked_list_t *range_rst;
            timer_start(&time_to_search);
            if (is_float) {
                for (i = 0; i < count; i++) {
                    void *out;
                    int rnd = i %  insert_count;
                    double end = float_keys[rnd]+(double)range_size_arr[k];
                    range_rst = search_numeric_range(index_root, &float_keys[rnd], sizeof(double), 
                        &end, sizeof(double));
                    // println("[spi range] %ld, %ld, rst = %d", float_keys[rnd], end, list_count(range_rst));
                }
            } else {
                for (i = 0; i < count; i++) {
                    void *out;
                    int rnd = i %  insert_count;
                    long end = int_keys[rnd]+(long)range_size_arr[k];
                    range_rst = search_numeric_range(index_root, &int_keys[rnd], sizeof(long), 
                        &end, sizeof(long));
                    // println("[spi range] %ld, %ld, rst = %d", int_keys[rnd], end, list_count(range_rst));
                }
            }
            
            timer_pause(&time_to_search);
            index_search_duration = timer_delta_us(&time_to_search);
            perf_info = get_number_ds_perf_info(index_root);
            println("[Total%d] search for %d range_%d query in %s is %ld us. Number of comparisons = %llu", 
            insert_count, count, range_size_arr[k], getenv(MIQS_NUMBER_IDX_VAR_NAME), index_search_duration, perf_info->num_of_comparisons);
        }
    }
    
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}