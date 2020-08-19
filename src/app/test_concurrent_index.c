#include "../lib/include/base_stdlib.h"
#include "../lib/h52index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include <pthread.h>



typedef struct test_config{
    int num_threads;
    long n_attrs;
    long n_avg_attr_vals;
    int use_pool;
} test_config_t;

typedef struct test_thread_param{
    test_config_t test_cfg;
    int tid;
    long N;
}test_thread_param_t;

// int NUM_THREADS = 1;
// miqs_meta_attribute_t **attr_arr;
// int N;

size_t mem_size;

miqs_meta_attribute_t *attr_arr;

index_anchor *idx_anchor;

void *doWork(void *tp);

void *doQuery(void *tp);

void *genData(void *tp);


char *mkrndstr(size_t length) { // const size_t length, supra

    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"; // could be const
    char *randomString;

    if (length) {
        randomString = malloc(length + 1); // sizeof(char) == 1, cf. C99

        if (randomString) {
            int l = (int) (sizeof(charset) - 1); // (static/global, could be const or #define SZ, would be even better)
            int key;  // one-time instantiation (static/global would be even better)
            for (int n = 0; n < length; n++) {
                key = rand() % l;   // no instantiation, just assignment, no overhead from sizeof
                randomString[n] = charset[key];
            }

            randomString[length] = '\0';
        }
    }

    return randomString;
}


void *genData(void *tp){
    long i, j;
    int l, t, tid;
    test_thread_param_t *thread_param = (test_thread_param_t *) tp;
    test_config_t test_cfg = thread_param->test_cfg;
    miqs_attr_type_t attr_types_array[] = {MIQS_AT_INTEGER, MIQS_AT_FLOAT, MIQS_AT_STRING};
    tid = thread_param->tid;
    for (i = 0; i < test_cfg.n_attrs; i++) {
        // char file_path_str[100];
        // sprintf(file_path_str,"file_%ld", i);
        
        char *buff = mkrndstr(rand() % 11);
        // miqs_meta_attribute_t *curr_attr = (miqs_meta_attribute_t *)ctr_calloc(1, sizeof(miqs_meta_attribute_t), &mem_size);

        for (j = 0; j < test_cfg.n_avg_attr_vals; j++){
            miqs_meta_attribute_t *curr_attr = &attr_arr[tid++];
            // char obj_path_str[100];
            // sprintf(obj_path_str,"obj_%ld", j);
            l = rand() % 31;
            t = rand() % 3;

            curr_attr->attr_name = (char *) ctr_calloc(strlen(buff), sizeof(char), &mem_size);
            curr_attr->next = NULL;
            sprintf(curr_attr->attr_name, "%s", buff);
            curr_attr->attr_type = attr_types_array[t];
            curr_attr->attribute_value_length = 1;

            void *_value;

            if (curr_attr->attr_type == MIQS_AT_INTEGER) {
                int *_integer = (int *) malloc(sizeof(int));
                *_integer = rand() % 20 + 5;
                _value = (void *)_integer;
            } else if (curr_attr->attr_type == MIQS_AT_FLOAT) {
                double *_double = (double *)malloc(sizeof(double));
                *_double = ((double) rand() / (double) (RAND_MAX)) * 10.5;
                _value = (void *)_double;
            } else {
                char *val_temp = mkrndstr(l);
                _value = val_temp;
                curr_attr->attribute_value_length = l;
            }
            curr_attr->attribute_value = _value;
            // curr_attr->
        }
        
        // attr_arr[i] = curr_attr;
    }
}


void *doWork(void *tp) {

    
    test_thread_param_t *thread_param = (test_thread_param_t *) tp;
    int i, r, t;
    int offset = 0;
    int quotion = thread_param->N / thread_param->test_cfg.num_threads;
    int mod = thread_param->N % thread_param->test_cfg.num_threads;
    if (thread_param->tid < mod) quotion += 1;
//    printf("Thread %d was assigned %d jobs\n",*thread_index,quotion);
    int currentIndex = thread_param->tid * quotion;
    if (thread_param->tid >= mod) currentIndex += mod;
    int lastIndex = currentIndex + quotion;

//    printf("Thread %d start from %d to %d\n",*thread_index, currentIndex, lastIndex-1);
    //Start doing the work here

    for (i = currentIndex; i < lastIndex; i++) {
        stopwatch_t timerWatch;
        timer_start(&timerWatch);
        create_in_mem_index_for_attr(idx_anchor, &attr_arr[i]);
        timer_pause(&timerWatch);
        printf("%d\t%d\t%llu\n", thread_param->tid, i, timer_delta_ns(&timerWatch));

    }

    pthread_exit((void *)((long)i));
}

void *doQuery(void *tp) {
    test_thread_param_t *thread_param = (test_thread_param_t *) tp;
    int i, r, t;
    int offset = 0;
    int resultCount = 0;
    int quotion = thread_param->N / thread_param->test_cfg.num_threads;
    int mod = thread_param->N % thread_param->test_cfg.num_threads;
    if (thread_param->tid - thread_param->test_cfg.num_threads < mod) quotion += 1;
    int currentIndex = thread_param->tid * quotion;
    if (thread_param->tid - thread_param->test_cfg.num_threads >= mod) currentIndex += mod;
    int lastIndex = currentIndex + quotion;
//    printf("Thread %d was assigned %d jobs from %d to %d\n",*thread_index,quotion, currentIndex, lastIndex);

    //Start doing the work here
    stopwatch_t timerWatch;
    timer_start(&timerWatch);
    for (i = currentIndex; i < lastIndex; i++) {
        if (attr_arr[i].attr_type == MIQS_AT_INTEGER) {
            int *value = (int *)attr_arr[i].attribute_value;
            power_search_rst_t *rst = int_value_search(attr_arr[i].attr_name, *value);
            if(rst->num_files==0){
                printf("Not found for type INTEGER with value %d\n",*value);
            }
            resultCount += rst->num_files;
        } else if (attr_arr[i].attr_type == MIQS_AT_FLOAT) {
            float *value = (float *)attr_arr[i].attribute_value;
            power_search_rst_t *rst = float_value_search(attr_arr[i].attr_name, *value);
            if(rst->num_files==0){
                printf("Not found for type FLOAT with value %f\n",*value);
            }
            resultCount += rst->num_files;
        } else {
            char *value = attr_arr[i].attribute_value;
            power_search_rst_t *rst = string_value_search(attr_arr[i].attr_name, value);
            if(rst->num_files==0){
                printf("Not found for type STRING with value %s\n",value);
            }
            resultCount += rst->num_files;
        }
    }
    timer_pause(&timerWatch);
    printf("Thread %d execute %d queries in %llu nanoseconds results %d\n", thread_param->tid, quotion, timer_delta_ns(&timerWatch), resultCount);
    pthread_exit((void *)((long)i));
}


int main(int argc, char *argv[]) {
    test_config_t test_cfg = {4, 1000, 1000};

    test_cfg.num_threads = atoi(argv[1]);
    // test_cfg.use_pool = atoi(argv[2]);
    // test_cfg.n_attrs = atoi(argv[2]);
    // test_cfg.n_attrs = atoi(argv[3]);

    if (init_in_mem_index() == 0) {
        return 0;
    }

    

    int status;
    void **ret;
    long i, j;
    int l, t, tid;

    int num_kvs = test_cfg.n_attrs*test_cfg.n_avg_attr_vals;

    idx_anchor = root_idx_anchor();

    attr_arr = (miqs_meta_attribute_t *)malloc(sizeof(miqs_meta_attribute_t) * num_kvs * test_cfg.num_threads * 10);
    printf("preparing dataset... \n");
    pthread_t data_threads[test_cfg.num_threads];
    test_thread_param_t *tparam = (test_thread_param_t *)calloc(test_cfg.num_threads, sizeof(test_thread_param_t));
    
    for (i = 0; i < test_cfg.num_threads; i++) {
        tparam[i].test_cfg=test_cfg;
        tparam[i].tid=(int)i;
        tparam[i].N = num_kvs;
        status = pthread_create(&data_threads[i], NULL, genData, (void *) &tparam[i]);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }

    


    pthread_t wr_threads[test_cfg.num_threads];
    pthread_t rd_threads[test_cfg.num_threads];


    int partial_list_size = (num_kvs / (int) (test_cfg.num_threads)) + (num_kvs % (int) (test_cfg.num_threads));
    test_thread_param_t *thread_param = (test_thread_param_t *)calloc(test_cfg.num_threads * 2, sizeof(test_thread_param_t));
    for (tid = 0; tid < 2*test_cfg.num_threads; tid++) {
         thread_param[i].N = num_kvs;
    }
    printf("Sample size = %d - Number of Threads used: %d\n", num_kvs, test_cfg.num_threads);
    //Print out sample of generated data
    printf("List 10 sample data \n");
    for(i=0;i<10;i++){
        if (attr_arr[i].attr_type == MIQS_AT_INTEGER) {
            int *value = (int *)attr_arr[i].attribute_value;
            printf("Key = %s - Type = INT - Value = %d\n",attr_arr[i].attr_name,*value);
        } else if (attr_arr[i].attr_type == MIQS_AT_FLOAT) {
            float *value = (float *)attr_arr[i].attribute_value;
            printf("Key = %s - Type = FLOAT - Value = %f\n",attr_arr[i].attr_name,*value);

        } else {
            char *value = attr_arr[i].attribute_value;
            printf("Key = %s - Type = STRING - Value = %s\n",attr_arr[i].attr_name,value);

        }
    }

    // Do Indexing
    stopwatch_t timer_index;
    timer_start(&timer_index);
    for (i = 0; i < test_cfg.num_threads; i++) {
        thread_param[i].test_cfg=test_cfg;
        thread_param[i].tid=(int)i;
        status = pthread_create(&wr_threads[i], NULL, doWork, (void *) &thread_param[i]);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < test_cfg.num_threads; i++) {
        if (pthread_join(wr_threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }
//
    timer_pause(&timer_index);
    double time_int_second = (double)timer_delta_ms(&timer_index)/1000;
    int throughputI = (int)((double)num_kvs/time_int_second);
    long int responseI = (int)timer_delta_ns(&timer_index)/num_kvs;
    printf("%d attributes indexed in %.6f seconds, overall throughput is %d qps, overall average response time is %ld nano seconds \n",
           num_kvs, (double)timer_delta_ms(&timer_index)/1000,throughputI,responseI);

    // do querying
    stopwatch_t timer_query;
    timer_start(&timer_query);
    for (i = 0; i < test_cfg.num_threads; i++) {
        thread_param[test_cfg.num_threads+i].test_cfg=test_cfg;
        thread_param[test_cfg.num_threads+i].tid=test_cfg.num_threads+(int)i;
        status = pthread_create(&rd_threads[i], NULL, doQuery, (void *) &thread_param[test_cfg.num_threads+i]);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < test_cfg.num_threads; i++) {
        if (pthread_join(rd_threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }
////
////
    timer_pause(&timer_query);
    double time_int_second_Q = (double)timer_delta_ms(&timer_query)/1000;
    int throughputQ = (int)((double)num_kvs/time_int_second_Q);
    long int responsetimeQ = (int)timer_delta_ns(&timer_query)/num_kvs;
    printf("%d queries finished in %0.6f seconds, overall throughput is %d qps, overall average response time is %ld nano seconds \n",
           num_kvs, (double)timer_delta_ms(&timer_query)/1000, throughputQ,responsetimeQ );

    free(attr_arr);
    exit(0);
}