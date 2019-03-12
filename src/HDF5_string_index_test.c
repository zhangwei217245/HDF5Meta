//#define ENABLE_MPI

#include <math.h>
#include "../lib/index_spi/spi.h"
#include "../lib/utils/timer_utils.h"
#include "../lib/utils/string_utils.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif



int main(int argc, const char *argv[]){

    int i, j; 
    int rank = 0, size = 1;

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

    int count = 1000;
    char *dataset_name = "RANDOM";
    if (argc >= 2) {
        count = atoi(argv[1]);
    }
    if (argc >= 3) {
        dataset_name = (char *)argv[2];
    }

    int pwr = rank / 4;

    count = count * (long)pow(10.0, (double)pwr);

    char **keys;

    if (strcmp(dataset_name, "UUID")==0) {
        keys = gen_uuids_strings(count);
    } else if (strcmp(dataset_name, "RANDOM") == 0) {
        keys = gen_random_strings(count, 10, 128);
    } else if (strcmp(dataset_name, "WIKI") == 0) {
        keys = read_words_from_text("/global/cscratch1/sd/wzhang5/data/dart/mini_wiki_no_count.txt", &count);
    } else if (strcmp(dataset_name, "DICT") == 0) {
        keys = read_words_from_text("/global/cscratch1/sd/wzhang5/data/dart/words_lower.txt", &count);
    }

    void *index_root;
    create_string_index(&index_root);
    stopwatch_t time_to_insert;
    timer_start(&time_to_insert);
    for (i = 0; i < count; i++) {
        insert_string(index_root, keys[i], keys[i]);
    }
    timer_pause(&time_to_insert);
    suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
    println("Total time to insert %d keys into %s is %ld us.", count,  getenv(MIQS_STRING_IDX_VAR_NAME), index_insertion_duration);

    size_t ds_mem = get_string_ds_mem();
    println("Total memory consumed by %s is %ld", getenv(MIQS_STRING_IDX_VAR_NAME), ds_mem);

    stopwatch_t time_to_search;
    timer_start(&time_to_search);
    for (i = 0; i < count; i++) {
        void *out;
        search_string(index_root, keys[i], strlen(keys[i]), &out);
    }
    timer_pause(&time_to_search);
    suseconds_t index_search_duration = timer_delta_us(&time_to_search);
    println("Total time to search %d keys in %s is %ld us.", count, getenv(MIQS_STRING_IDX_VAR_NAME), index_search_duration);


#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
}