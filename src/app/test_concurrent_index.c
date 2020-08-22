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
} test_param_t;

size_t mem_size;

miqs_meta_attribute_t **attr_arr;

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
        char file_path_str[100];
        sprintf(file_path_str,"file_%ld", i);
        
        char *buff = gen_rand_strings(1, 11)[0];

        for (j = 0; j < tparam->n_avg_attr_vals; j++){
            if (c % tparam->num_threads == tparam->tid) {
                miqs_meta_attribute_t *curr_attr = (miqs_meta_attribute_t *)ctr_calloc(1, sizeof(miqs_meta_attribute_t), &mem_size);
                char obj_path_str[100];
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
                attr_arr[c] = curr_attr;
                n++;
            }
            c++;
        }
    }
    timer_pause(tparam->timerWatch);
    println("t %ld created %ld attributes in "PRIu64" ns.", tparam->tid, n, timer_delta_ns(tparam->timerWatch));
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
            create_in_mem_index_for_attr(idx_anchor, attr_arr[c]);
            // timer_pause(tparam->timerWatch);
            // num_indexed++;
            // println("thread %d indexed the %ld th attribute in %" PRIu64 " ns", tparam->tid, c, timer_delta_ns(tparam->timerWatch));
        }
    }
    timer_pause(tparam->timerWatch);
    println("thread %d indexed %ld attributes in %" PRIu64 " ns", tparam->tid, num_indexed, timer_delta_ns(tparam->timerWatch));
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
            miqs_meta_attribute_t *meta_attr = attr_arr[c];
            if (meta_attr->attr_type == MIQS_AT_INTEGER) {
                int *value = (int *)meta_attr->attribute_value;
                power_search_rst_t *rst = int_value_search(meta_attr->attr_name, *value);
                if(rst->num_files==0){
                    // println("Not found for type INTEGER with value %d", *value);
                }
                resultCount += rst->num_files;
            } else if (meta_attr->attr_type == MIQS_AT_FLOAT) {
                double *value = (double *)meta_attr->attribute_value;
                power_search_rst_t *rst = float_value_search(meta_attr->attr_name, *value);
                if(rst->num_files==0){
                    // println("Not found for type FLOAT with value %.2f", *value);
                }
                resultCount += rst->num_files;
            } else {
                char *value = meta_attr->attribute_value;
                power_search_rst_t *rst = string_value_search(meta_attr->attr_name, value);
                if(rst->num_files==0){
                    // println("Not found for type STRING with value %s",value);
                }
                resultCount += rst->num_files;
            }
            num_queried++;
        }
    }
    timer_pause(tparam->timerWatch);
    println("Thread %d execute %ld queries in %"PRIu64" nanoseconds results %ld", 
        tparam->tid, num_queried, timer_delta_ns(tparam->timerWatch), resultCount);
    pthread_exit((void *)c);
}


int main(int argc, char *argv[]) {

    int status;
    void **ret;
    long i, j;
    int l, t, tid;

    int thread_count = 4;
    int use_pool = 0;
    long n_attrs = 1000;
    long n_avg_attr_vals = 1000;

    thread_count = atoi(argv[1]);
    // use_pool = atoi(argv[2]);
    // n_attrs = atoi(argv[2]);
    // n_avg_attr_vals = atoi(argv[3]);

    if (init_in_mem_index() == 0) {
        return 0;
    }

    long num_kvs = n_attrs*n_avg_attr_vals;

    idx_anchor = root_idx_anchor();

    attr_arr = (miqs_meta_attribute_t **)calloc(num_kvs, sizeof(miqs_meta_attribute_t *));

    println("preparing dataset... ");
    int gen_data_t_count = 20;
    pthread_t data_threads[gen_data_t_count];
    
    for (i = 0; i < gen_data_t_count; i++) {
        test_param_t *tparam = gen_test_param(gen_data_t_count, n_attrs, n_avg_attr_vals, i);
        status = pthread_create(&data_threads[i], NULL, genData, (void *)tparam);
        if (status != 0) {
            println("ERROR; return code from pthread_create() is %d", status);
            exit(-1);
        }
    }

    for (i = 0; i < gen_data_t_count; i++) {
        if (pthread_join(data_threads[i], ret) != 0) {
            println("Error : pthread_join failed on joining thread %ld", i);
            return -1;
        }
    }

    println("Sample size = %ld - Number of Threads used: %d", num_kvs, gen_data_t_count);
    // Print out sample of generated data
    println("List 10 sample data ");
    for(i=0;i<10;i++){
        if (attr_arr[i]->attr_type == MIQS_AT_INTEGER) {
            int *value = (int *)attr_arr[i]->attribute_value;
            println("Key = %s - Type = INT - Value = %d",attr_arr[i]->attr_name,*value);
        } else if (attr_arr[i]->attr_type == MIQS_AT_FLOAT) {
            float *value = (float *)attr_arr[i]->attribute_value;
            println("Key = %s - Type = FLOAT - Value = %.2f",attr_arr[i]->attr_name,*value);

        } else {
            char **value = attr_arr[i]->attribute_value;
            println("Key = %s - Type = STRING - Value = %s",attr_arr[i]->attr_name,value[0]);

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
            println("ERROR; return code from pthread_create() is %d", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < thread_count; i++) {
        if (pthread_join(wr_threads[i], ret) != 0) {
            println("Error : pthread_join failed on joining thread %ld", i);
            return -1;
        }
    }
//
    timer_pause(&timer_index);
    double index_duration = (double)timer_delta_ms(&timer_index)/1000;
    double throughputI = (double)num_kvs/index_duration;
    uint64_t responseI = timer_delta_ns(&timer_index)/(uint64_t)num_kvs;
    println("%ld attributes indexed in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds",
           num_kvs, (double)(timer_delta_ms(&timer_index)/1000),throughputI,responseI);

    // do querying
    stopwatch_t timer_query;
    timer_start(&timer_query);
    for (i = 0; i < thread_count; i++) {
        test_param_t *tparam = gen_test_param(thread_count, n_attrs, n_avg_attr_vals, i);
        status = pthread_create(&rd_threads[i], NULL, doQuery, (void *)tparam);
        if (status != 0) {
            println("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < thread_count; i++) {
        if (pthread_join(rd_threads[i], ret) != 0) {
            println("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }
////
////
    timer_pause(&timer_query);
    double query_duration = (double)timer_delta_ms(&timer_query)/1000;
    double throughputQ = (double)num_kvs/query_duration;
    long responseQ = timer_delta_ns(&timer_query)/num_kvs;
    println("%ld attributes queried in %.2f seconds, overall throughput is %.2f qps, overall average response time is %"PRIu64" nano seconds",
           num_kvs, (double)(timer_delta_ms(&timer_index)/1000),throughputQ,responseQ);

    free(attr_arr);
    exit(0);
}