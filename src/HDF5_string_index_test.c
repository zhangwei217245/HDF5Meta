// #define ENABLE_MPI

#include <math.h>
#include "../lib/index_spi/spi.h"
#include "../lib/utils/timer_utils.h"
#include "../lib/utils/string_utils.h"
#include "boss_dataset.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

char *get_affix(pattern_type_t affix_type, const char *original){
    char *rst=NULL;
    size_t target_len = strlen(original)/2;
    if (target_len < 2) {
        rst = (char *)original;
    }
    rst = (char *)calloc(target_len+1, sizeof(char));
    if (affix_type == PATTERN_MIDDLE) {
        strncpy(rst, &original[target_len/2+target_len%2], target_len);
    } else if (affix_type == PATTERN_SUFFIX) {
        strncpy(rst, &original[strlen(original)-target_len], target_len);
    } else if (affix_type == PATTERN_PREFIX) {
        strncpy(rst, original, target_len);
    } else {
        rst = (char *)original;
    }
    return rst;
}

int main(int argc, char **argv){

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

    char **keys;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (rank % 4 == 0) {
        setenv(MIQS_STRING_IDX_VAR_NAME, "HASHTABLE", 1);
    } else if (rank % 4 == 1 ) {
        setenv(MIQS_STRING_IDX_VAR_NAME, "SBST", 1);
    } else if (rank % 4 == 2) {
        setenv(MIQS_STRING_IDX_VAR_NAME, "TRIE", 1);
    } else if (rank %4 == 3) {
        setenv(MIQS_STRING_IDX_VAR_NAME, "ART", 1);
    }

    // char *alpha = "Hello ";
    // char *beta = "World!";
    // char *gamma = " This is a program!";
    // println("%s", concat(alpha, beta, gamma));

    int len_key_arr = count;

    if (strcmp(dataset_name, "UUID")==0) {
        keys = gen_uuids_strings(len_key_arr);
    } else if (strcmp(dataset_name, "RANDOM") == 0) {
        keys = gen_random_strings(len_key_arr, 10, 128);
    } else if (strcmp(dataset_name, "WIKI") == 0) {
        keys = read_words_from_text("/global/cscratch1/sd/wzhang5/data/dart/mini_wiki_no_count.txt", &len_key_arr);
    } else if (strcmp(dataset_name, "DICT") == 0) {
        keys = read_words_from_text("/global/cscratch1/sd/wzhang5/data/dart/words_lower.txt", &len_key_arr);
    } else if (strcmp(dataset_name, "BOSSNAME")==0){
        len_key_arr = len_string_ATTR_NAMES;
        keys = string_ATTR_NAMES;
    } else if (strcmp(dataset_name, "OBJFILE")== 0){
        len_key_arr = len_string_vals[0];
        keys = string_OBJFILE;
    } else if (strcmp(dataset_name, "DATE-OBS")== 0){
        len_key_arr = len_string_vals[1];
        keys = string_DATE_OBS;
    } else if (strcmp(dataset_name, "GUIDERN")== 0){
        len_key_arr = len_string_vals[2];
        keys = string_GUIDERN;
    } else if (strcmp(dataset_name, "EXPID02")== 0){
        len_key_arr = len_string_vals[3];
        keys = string_EXPID02;
    } else {
        len_key_arr = len_string_ATTR_NAMES;
        keys = string_ATTR_NAMES;
    }

    

    int round = 0;
    for  (round = 0; round < 1; round ++) {
        void *index_root;
        create_string_index(&index_root);

        int insert_count = len_key_arr-(len_key_arr/4)*(round);

        stopwatch_t time_to_insert;
        timer_start(&time_to_insert);
        for (i = 0; i < (insert_count); i++) {
            insert_string(index_root, keys[i], keys[i]);
        }
        timer_pause(&time_to_insert);
        suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
        perf_info_t *perf_info = get_string_ds_perf_info(index_root);
        size_t ds_mem = perf_info->mem_usage;
        uint64_t n_comp = perf_info->num_of_comparisons;
        uint64_t n_realloc = perf_info->num_of_reallocs;
        stw_nanosec_t t_locate=perf_info->time_to_locate;
        stw_nanosec_t t_expand=perf_info->time_for_expansion;
        println("[Total%d] Insert %d keys into %s took %ld us. %llu memory consumed, %llu comparisons, %llu reallocations, %llu ns for locate, %llu ns for expansion", 
        insert_count, insert_count,  getenv(MIQS_STRING_IDX_VAR_NAME), index_insertion_duration, ds_mem, n_comp, n_realloc, t_locate, t_expand);

        reset_string_ds_perf_info_counters(index_root);

        // srand(time(0));
        srand(0);
        

        stopwatch_t time_to_search;
        timer_start(&time_to_search);
        for (i = 0; i < count; i++) {
            void *out; 
            int rnd = rand() %  insert_count;
            search_string(index_root, keys[rnd], strlen(keys[rnd]), &out);
        }
        timer_pause(&time_to_search);
        suseconds_t index_search_duration = timer_delta_us(&time_to_search);
        perf_info = get_string_ds_perf_info(index_root);
        n_comp = perf_info->num_of_comparisons;
        t_locate = perf_info->time_to_locate;
        println("[Total%d] time to search %d keys in %s is %ld us. %llu ns for locate. %llu comparisons", 
        insert_count, count, getenv(MIQS_STRING_IDX_VAR_NAME), index_search_duration, t_locate, n_comp);


        

        pattern_type_t affix_types[]={
            PATTERN_PREFIX,
            PATTERN_SUFFIX,
            PATTERN_MIDDLE
        };
        char *afx_type_names[]={
            "PREFIX",
            "SUFFIX",
            "INFIX"
            };
        int k = 0;
        for (k = 0; k < 3; k++) {
            // reset_number_ds_perf_info_counters(index_root);

            pattern_type_t affix_type= affix_types[k];
            // stopwatch_t time_to_search;
            size_t rst_count = 0;
            timer_start(&time_to_search);
            for (i = 0; i < count; i++) {
                void *out;
                int rnd = rand() %  insert_count;
                char *affix = get_affix(affix_type, keys[rnd]);
                linked_list_t *rst = search_affix(index_root, affix_type, affix);
                rst_count+=list_count(rst);
            }
            timer_pause(&time_to_search);
            index_search_duration = timer_delta_us(&time_to_search);
            perf_info = get_number_ds_perf_info(index_root);
            n_comp = perf_info->num_of_comparisons;
            println("[Total%d] time to search %d %s in %s is %ld us. Number of comparisons = %llu. Number of results = %lu", 
            insert_count, count, afx_type_names[k], getenv(MIQS_STRING_IDX_VAR_NAME), index_search_duration, n_comp, rst_count);
        }

    }
    
    
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}