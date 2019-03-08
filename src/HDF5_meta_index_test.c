#include "../lib/index_spi/spi.h"
#include "../lib/utils/timer_utils.h"
#include "../lib/utils/string_utils.h"


char *gen_random(const int len) {
    if (len <=0 ){
        return NULL;
    }
    char *rst = (char *)calloc(len, sizeof(char));
    static const char alphanum[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        rst[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    rst[len] = 0;
    return rst;
}

int main(int argc, const char *argv[]){
    void *index_root;
    int i, j, k;
    k = 1000;
    if (argc >= 2) {
        int t = atoi(argv[1]);
        k = t * k;
    }
    create_string_index(&index_root);
    char **keys = (char **)calloc(k, sizeof(char *));
    stopwatch_t time_to_insert;
    timer_start(&time_to_insert);
    for (i = 0; i < k; i++) {
        char *rand_str_k = gen_random(10);
        keys[i] =  rand_str_k;
        for (j = 0; j < k; j++) {
            char *rand_str_v = gen_random(10);
            insert_string(index_root, rand_str_k, rand_str_v);
        }
    }
    timer_pause(&time_to_insert);
    suseconds_t index_insertion_duration = timer_delta_us(&time_to_insert);
    println("Total time to insert %d keys with %d values on each key is %ld us.", k, k, index_insertion_duration);


    stopwatch_t time_to_search;
    timer_start(&time_to_search);
    for (i = 0; i < k; i++) {
        void *out;
        search_string(index_root, keys[i], strlen(keys[i]), &out);
    }
    timer_pause(&time_to_search);
    suseconds_t index_search_duration = timer_delta_us(&time_to_search);
    println("Total time to search %d keys is %ld us.", k, k, index_search_duration);
}