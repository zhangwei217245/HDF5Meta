#include "../lib/include/base_stdlib.h"
#include "../lib/include/c99_stdlib.h"
#include "../lib/h52index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include <pthread.h>



typedef struct test_param{
    int num_threads;
    long n_attrs;
    long n_avg_attr_vals;
    int tid;
    stopwatch_t *timerWatch;
    int extra_test;
} test_param_t;

size_t mem_size;

pthread_rwlock_t ATTR_ARRAY_LOCK;

miqs_meta_attribute_t **attr_arr;

miqs_meta_attribute_t **attr_arr_2;

index_anchor *idx_anchor;

void *genData(void *tp);

void *doIndexing(void *tp);

void *doQuery(void *tp);

test_param_t *gen_test_param(int num_threads, long n_attrs, long n_avg_attr_vals, int tid) {
    test_param_t * rst = (test_param_t *)calloc(1, sizeof(test_param_t));
    rst->num_threads = num_threads;
    rst->n_attrs = n_attrs;
    rst->n_avg_attr_vals = n_avg_attr_vals;
    rst->tid = tid;
    rst->timerWatch = (stopwatch_t *)calloc(1, sizeof(stopwatch_t));
    rst->extra_test = 0;
    return rst;
}

void *genData(void *tp){
    long c = 0;
    long n = 0;
    long i, j;
    int t = 0;
    test_param_t *tparam = (test_param_t *) tp;
    miqs_attr_type_t attr_types_array[] = {MIQS_AT_INTEGER, MIQS_AT_FLOAT, MIQS_AT_STRING};
    long num_kvs = tparam->n_attrs * tparam->n_avg_attr_vals;
    timer_start(tparam->timerWatch);
    for (i = 0; i < tparam->n_attrs; i++) {
        char *file_path_str = (char *)calloc(100, sizeof(char));
        sprintf(file_path_str,"file_%ld", i);
        
        char *buff = gen_rand_strings(1, 11)[0];

        for (j = 0; j < tparam->n_avg_attr_vals; j++){
            if (c % tparam->num_threads == tparam->tid) {
                miqs_meta_attribute_t *curr_attr = (miqs_meta_attribute_t *)ctr_calloc(1, sizeof(miqs_meta_attribute_t), &mem_size);
                char *obj_path_str = (char *)calloc(100, sizeof(char));
                sprintf(obj_path_str,"obj_%ld", j);
                t = rand() % 3;

                curr_attr->attr_name = (char *)ctr_calloc(strlen(buff), sizeof(char), &mem_size);
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
                    char **val_temp = gen_rand_strings(1, 31);
                    _value = (void *)val_temp;
                    curr_attr->attribute_value_length = 1;
                }
                curr_attr->attribute_value = _value;
                curr_attr->file_path_str = file_path_str;
                curr_attr->obj_path_str = obj_path_str;

                pthread_rwlock_wrlock(&(ATTR_ARRAY_LOCK));
                
                if (tparam->extra_test) {
                    attr_arr_2[c] = curr_attr;
                } else {
                    attr_arr[c] = curr_attr;
                }

                pthread_rwlock_unlock(&(ATTR_ARRAY_LOCK));

                n++;
            }
            c++;
        }
    }
    timer_pause(tparam->timerWatch);
    printf("t %d created %ld attributes in %"PRIu64" ns.\n", tparam->tid, n, timer_delta_ns(tparam->timerWatch));
    pthread_exit((void *)n);
}


void *doIndexing(void *tp) {

    test_param_t *tparam = (test_param_t *) tp;
    long c = 0;
    long num_kvs = tparam->n_attrs * tparam->n_avg_attr_vals;
    long num_indexed = 0;
    timer_start(tparam->timerWatch);
    for (c = 0; c < num_kvs; c++) {
        if (c % tparam->num_threads == tparam->tid) {
            // timer_start(tparam->timerWatch);

            pthread_rwlock_rdlock(&(ATTR_ARRAY_LOCK));

            miqs_meta_attribute_t *attr = tparam->extra_test?attr_arr_2[c]:attr_arr[c];

            pthread_rwlock_unlock(&(ATTR_ARRAY_LOCK));

            create_in_mem_index_for_attr(idx_anchor, attr);
            // timer_pause(tparam->timerWatch);
            num_indexed++;
            // printf("thread %d indexed the %ld th attribute in %" PRIu64 " ns \n", tparam->tid, c, timer_delta_ns(tparam->timerWatch));
        }
    }
    timer_pause(tparam->timerWatch);
    printf("thread %d indexed %ld attributes in %" PRIu64 " ns \n", tparam->tid, num_indexed, timer_delta_ns(tparam->timerWatch));
    pthread_exit((void *)c);
}

void *doQuery(void *tp) {
    test_param_t *tparam = (test_param_t *) tp;
    long c = 0;
    long num_kvs = tparam->n_attrs * tparam->n_avg_attr_vals;
    size_t resultCount = 0;
    long num_queried = 0;

    //Start doing the work here
    
    timer_start(tparam->timerWatch);
    for (c = 0; c < num_kvs; c++) {
        if (c % tparam->num_threads == tparam->tid) {

            pthread_rwlock_rdlock(&(ATTR_ARRAY_LOCK));

            miqs_meta_attribute_t *meta_attr = tparam->extra_test?attr_arr_2[c]:attr_arr[c];

            pthread_rwlock_unlock(&(ATTR_ARRAY_LOCK));

            power_search_rst_t *rst = metadata_search(meta_attr->attr_name, meta_attr->attribute_value, meta_attr->attr_type);
            if(rst->num_files==0){
                // printf("Not found for type INTEGER with value %d", *value);
            }
            resultCount += rst->num_files;
            num_queried++;
        }
    }
    timer_pause(tparam->timerWatch);
    printf("Thread %d execute %ld queries in %"PRIu64" nanoseconds results %ld \n"  , 
        tparam->tid, num_queried, timer_delta_ns(tparam->timerWatch), resultCount);
    pthread_exit((void *)c);
}


int main(int argc, char *argv[]) {

    int status;
    void **ret;
    long i, j;
    int l, t, tid;

    int thread_count = 4;
    int parallelism = 1;
    int use_pool = 0;
    long n_attrs = 1000;
    long n_avg_attr_vals = 1000;

    thread_count = atoi(argv[1]);
    parallelism = atoi(argv[2]);
    // use_pool = atoi(argv[3]);
    // n_attrs = atoi(argv[4]);
    // n_avg_attr_vals = atoi(argv[5]);

    if (init_in_mem_index(parallelism) == 0) {
        return 0;
    }

    long num_kvs = n_attrs*n_avg_attr_vals;

    idx_anchor = root_idx_anchor();

    attr_arr = (miqs_meta_attribute_t **)calloc(num_kvs, sizeof(miqs_meta_attribute_t *));

    attr_arr_2 = (miqs_meta_attribute_t **)calloc(num_kvs, sizeof(miqs_meta_attribute_t *));

    pthread_rwlock_init(&ATTR_ARRAY_LOCK, NULL);

    printf("preparing dataset... ");
    int gen_data_t_count = 20;
    pthread_t data_threads[gen_data_t_count];
    
    for (i = 0; i < gen_data_t_count; i++) {
        test_param_t *tparam = gen_test_param(gen_data_t_count, n_attrs, n_avg_attr_vals, i);
        status = pthread_create(&data_threads[i], NULL, genData, (void *)tparam);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d \n", status);
            exit(-1);
        }
    }

    for (i = 0; i < gen_data_t_count; i++) {
        if (pthread_join(data_threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld \n", i);
            return -1;
        }
    }

    printf("Sample size = %ld - Number of Threads used: %d \n", num_kvs, gen_data_t_count);
    // Print out sample of generated data
    printf("List 10 sample data \n");
    for(i=0;i<10;i++){
        if (attr_arr[i]->attr_type == MIQS_AT_INTEGER) {
            int *value = (int *)attr_arr[i]->attribute_value;
            printf("Key = %s - Type = INT - Value = %d \n",attr_arr[i]->attr_name,*value);
        } else if (attr_arr[i]->attr_type == MIQS_AT_FLOAT) {
            float *value = (float *)attr_arr[i]->attribute_value;
            printf("Key = %s - Type = FLOAT - Value = %.2f \n",attr_arr[i]->attr_name,*value);

        } else {
            char **value = attr_arr[i]->attribute_value;
            printf("Key = %s - Type = STRING - Value = %s \n",attr_arr[i]->attr_name,value[0]);

        }
    }

    pthread_t wr_threads[thread_count];
    pthread_t rd_threads[thread_count];

    // Do Indexing
    stopwatch_t timer_index;
    timer_start(&timer_index);
    for (i = 0; i < thread_count; i++) {
        test_param_t *tparam = gen_test_param(thread_count, n_attrs, n_avg_attr_vals, i);
        status = pthread_create(&wr_threads[i], NULL, doIndexing, (void *)tparam);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d \n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < thread_count; i++) {
        if (pthread_join(wr_threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld \n", i);
            return -1;
        }
    }

    timer_pause(&timer_index);
    double index_duration = (double)timer_delta_ms(&timer_index)/1000;
    double throughputI = (double)num_kvs/index_duration;
    uint64_t responseI = timer_delta_ns(&timer_index)/(uint64_t)num_kvs;
    printf("[INDEX] %ld attributes indexed in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds \n",
           num_kvs, index_duration,throughputI,responseI);

    // do querying
    stopwatch_t timer_query;
    timer_start(&timer_query);
    for (i = 0; i < thread_count; i++) {
        test_param_t *tparam = gen_test_param(thread_count, n_attrs, n_avg_attr_vals, i);
        status = pthread_create(&rd_threads[i], NULL, doQuery, (void *)tparam);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < thread_count; i++) {
        if (pthread_join(rd_threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }


    timer_pause(&timer_query);
    double query_duration = (double)timer_delta_ms(&timer_query)/1000;
    double throughputQ = (double)num_kvs/query_duration;
    uint64_t responseQ = timer_delta_ns(&timer_query)/num_kvs;
    printf("[SEARCH] %ld attributes queried in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds \n",
           num_kvs, query_duration,throughputQ,responseQ);


    /** *****************************************************************************
     *  ****************** Indexing while searching *********************************
     *  *****************************************************************************/
    
    // for (i = 0; i < gen_data_t_count; i++) {
    //     test_param_t *tparam = gen_test_param(gen_data_t_count, n_attrs, n_avg_attr_vals, i);
    //     tparam->extra_test = 1;
    //     status = pthread_create(&data_threads[i], NULL, genData, (void *)tparam);
    //     if (status != 0) {
    //         printf("ERROR; return code from pthread_create() is %d \n", status);
    //         exit(-1);
    //     }
    // }

    // for (i = 0; i < gen_data_t_count; i++) {
    //     if (pthread_join(data_threads[i], ret) != 0) {
    //         printf("Error : pthread_join failed on joining thread %ld \n", i);
    //         return -1;
    //     }
    // }

    // printf("Another %ld data generated with %d threads. \n", num_kvs, gen_data_t_count);

    // timer_start(&timer_index);
    // for (i = 0; i < thread_count; i++) {
    //     test_param_t *tparam = gen_test_param(thread_count, n_attrs, n_avg_attr_vals, i);
    //     tparam->extra_test = 1;
    //     status = pthread_create(&wr_threads[i], NULL, doIndexing, (void *)tparam);
    //     if (status != 0) {
    //         printf("ERROR; return code from pthread_create() is %d \n", status);
    //         exit(-1);
    //     }
    // }

    // timer_start(&timer_query);
    // for (i = 0; i < thread_count; i++) {
    //     test_param_t *tparam = gen_test_param(thread_count, n_attrs, n_avg_attr_vals, i);
    //     status = pthread_create(&rd_threads[i], NULL, doQuery, (void *)tparam);
    //     if (status != 0) {
    //         printf("ERROR; return code from pthread_create() is %d\n", status);
    //         exit(-1);
    //     }
    // }

    // for (i = 0; i < thread_count; i++) {
    //     if (pthread_join(wr_threads[i], ret) != 0) {
    //         printf("Error : pthread_join failed on joining thread %ld \n", i);
    //         return -1;
    //     }
    // }
    // timer_pause(&timer_index);
    // index_duration = (double)timer_delta_ms(&timer_index)/1000;
    // throughputI = (double)num_kvs/index_duration;
    // responseI = timer_delta_ns(&timer_index)/(uint64_t)num_kvs;
    // printf("[INDEX WHEN SEARCH] %ld attributes indexed in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds \n",
    //        num_kvs, index_duration,throughputI,responseI);


    // for (i = 0; i < thread_count; i++) {
    //     if (pthread_join(rd_threads[i], ret) != 0) {
    //         printf("Error : pthread_join failed on joining thread %ld\n", i);
    //         return -1;
    //     }
    // }

    // timer_pause(&timer_query);
    // query_duration = (double)timer_delta_ms(&timer_query)/1000;
    // throughputQ = (double)num_kvs/query_duration;
    // responseQ = timer_delta_ns(&timer_query)/num_kvs;
    // printf("[SEARCH WHEN INDEX] %ld attributes queried in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds \n",
    //        num_kvs, query_duration,throughputQ,responseQ);

    pthread_rwlock_destroy(&ATTR_ARRAY_LOCK);
    free(attr_arr);
    exit(0);
}