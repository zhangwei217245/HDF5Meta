#include "../lib/include/base_stdlib.h"
#include "../lib/h52index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include <pthread.h>



int NUM_THREADS = 1;
miqs_meta_attribute_t **attr_arr;
pthread_rwlock_t LEAF_VALUE_LOCK;
pthread_rwlock_t GLOBAL_ART_LOCK;
int N;
int partial_list_size;
size_t hdf5_meta_size;
index_anchor *idx_anchor;
void *doWork(void *tid);

void *doQuery(void *tid);


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



void *doWork(void *tid) {


    int *thread_index = (int *) tid;
    int i, r, t;
    int offset = 0;
    int quotion = N / NUM_THREADS;
    int mod = N % NUM_THREADS;
    if (*thread_index < mod) quotion += 1;
//    printf("Thread %d was assigned %d jobs\n",*thread_index,quotion);
    int currentIndex = *thread_index * quotion;
    if (*thread_index >= mod) currentIndex += mod;
    int lastIndex = currentIndex + quotion;

//    printf("Thread %d start from %d to %d\n",*thread_index, currentIndex, lastIndex-1);
    //Start doing the work here

    for (i = currentIndex; i < lastIndex; i++) {
        stopwatch_t timerWatch;
        timer_start(&timerWatch);
        create_in_mem_index_for_attr(idx_anchor, attr_arr[i]);
        timer_pause(&timerWatch);
        printf("%d\t%d\t%llu\n", *thread_index, i, timer_delta_ns(&timerWatch));

    }

    pthread_exit((void *)((long)i));
}

void *doQuery(void *tid) {
    int *thread_index = (int *) tid;
    int i, r, t;
    int offset = 0;
    int resultCount = 0;
    int quotion = N / NUM_THREADS;
    int mod = N % NUM_THREADS;
    if (*thread_index < mod) quotion += 1;
    int currentIndex = *thread_index * quotion;
    if (*thread_index >= mod) currentIndex += mod;
    int lastIndex = currentIndex + quotion;
//    printf("Thread %d was assigned %d jobs from %d to %d\n",*thread_index,quotion, currentIndex, lastIndex);

    //Start doing the work here
    stopwatch_t timerWatch;
    timer_start(&timerWatch);
    for (i = currentIndex; i < lastIndex; i++) {
        if (attr_arr[i]->attr_type == H5T_INTEGER) {
            int *value = (int *)attr_arr[i]->attribute_value;
            power_search_rst_t *rst = int_value_search(attr_arr[i]->attr_name, *value);
            if(rst->num_files==0){
                printf("Not found for type INTEGER with value %d\n",*value);
            }
            resultCount += rst->num_files;
        } else if (attr_arr[i]->attr_type == H5T_FLOAT) {
            float *value = (float *)attr_arr[i]->attribute_value;
            power_search_rst_t *rst = float_value_search(attr_arr[i]->attr_name, *value);
//            if(rst->num_files==0){
//                printf("Not found for type FLOAT with value %f\n",*value);
//            }
            resultCount += rst->num_files;
        } else {
            char *value = attr_arr[i]->attribute_value;
            power_search_rst_t *rst = string_value_search(attr_arr[i]->attr_name, value);
            if(rst->num_files==0){
                printf("Not found for type STRING with value %s\n",value);
            }
            resultCount += rst->num_files;
        }
    }
    timer_pause(&timerWatch);
    printf("Thread %d execute %d queries in %llu nanoseconds results %d\n", *thread_index, quotion, timer_delta_ns(&timerWatch), resultCount);
    pthread_exit((void *)((long)i));
}


int main(int argc, char *argv[]) {
    NUM_THREADS = atoi(argv[1]);
    N = 100000;
    if (init_in_mem_index() == 0) {
        return 0;
    }
    int status;
    void **ret;
    long i;
    int r, t;
    pthread_rwlock_init(&GLOBAL_ART_LOCK, NULL);
    pthread_rwlock_init(&LEAF_VALUE_LOCK, NULL);
    idx_anchor = root_idx_anchor();
    attr_arr = (miqs_meta_attribute_t **) malloc(sizeof(miqs_meta_attribute_t) * N);
    for (i = 0; i < N; i++) {
        miqs_meta_attribute_t *curr_attr = (miqs_meta_attribute_t *) ctr_calloc(1, sizeof(miqs_meta_attribute_t), &hdf5_meta_size);
        r = rand() % 8 + 5;
        t = rand() % 3;
        if (t == 2)t = 3;
        char *buff = mkrndstr(r);
        curr_attr->attr_name = (char *) ctr_calloc(strlen(buff), sizeof(char), &hdf5_meta_size);
        curr_attr->next = NULL;
        sprintf(curr_attr->attr_name, "%s", buff);

        curr_attr->attr_type = t;
        curr_attr->attribute_value_length = 1;
        void *_value;

        if (curr_attr->attr_type == MIQS_AT_INTEGER) {
            int *_integer = (int *) malloc(sizeof(int));
            *_integer = rand() % 20 + 5;
            _value = _integer;
        } else if (curr_attr->attr_type == MIQS_AT_FLOAT) {
            float _float = ((float) rand() / (float) (RAND_MAX)) * 10.5;
            _value = &(_float);
        } else {
            char *val_temp = mkrndstr(r);
            _value = val_temp;


        }
        curr_attr->attribute_value = _value;
        attr_arr[i] = curr_attr;

    }




    pthread_t threads[NUM_THREADS];


    partial_list_size = (N / (int) (NUM_THREADS)) + (N % (int) (NUM_THREADS));
    int *index = calloc(NUM_THREADS, sizeof(int));
    for (i = 0; i < NUM_THREADS; i++) {
        index[i] = i;
    }
    printf("Sample size = %d - Number of Threads used: %d\n", N, NUM_THREADS);
    //Print out sample of generated data
    printf("List 10 sample data \n");
    for(i=0;i<10;i++){
        if (attr_arr[i]->attr_type == H5T_INTEGER) {
            int *value = (int *)attr_arr[i]->attribute_value;
            printf("Key = %s - Type = INT - Value = %d\n",attr_arr[i]->attr_name,*value);
        } else if (attr_arr[i]->attr_type == H5T_FLOAT) {
            float *value = (float *)attr_arr[i]->attribute_value;
            printf("Key = %s - Type = FLOAT - Value = %f\n",attr_arr[i]->attr_name,*value);

        } else {
            char *value = attr_arr[i]->attribute_value;
            printf("Key = %s - Type = STRING - Value = %s\n",attr_arr[i]->attr_name,value);

        }
    }
    stopwatch_t timer_index;
    timer_start(&timer_index);
    for (i = 0; i < NUM_THREADS; i++) {
        status = pthread_create(&threads[i], NULL, doWork, (void *) &index[i]);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < NUM_THREADS; i++) {
        if (pthread_join(threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }
//

    timer_pause(&timer_index);
    double time_int_second = (double)timer_delta_ms(&timer_index)/1000;
    int throughputI = (int)((double)N/time_int_second);
    long int responseI = (int)timer_delta_ns(&timer_index)/N;
    printf("%d attributes indexed in %.6f seconds, overall throughput is %d qps, overall average response time is %ld nano seconds \n",
           N, (double)timer_delta_ms(&timer_index)/1000,throughputI,responseI);

    stopwatch_t timer_query;
    timer_start(&timer_query);
    for (i = 0; i < NUM_THREADS; i++) {
        status = pthread_create(&threads[i], NULL, doQuery, (void *) &index[i]);
        if (status != 0) {
            printf("ERROR; return code from pthread_create() is %d\n", status);
            exit(-1);
        }
    }
    /* join threads */
    for (i = 0; i < NUM_THREADS; i++) {
        if (pthread_join(threads[i], ret) != 0) {
            printf("Error : pthread_join failed on joining thread %ld\n", i);
            return -1;
        }
    }
////
////
    timer_pause(&timer_query);
    double time_int_second_Q = (double)timer_delta_ms(&timer_query)/1000;
    int throughputQ = (int)((double)N/time_int_second_Q);
    long int responsetimeQ = (int)timer_delta_ns(&timer_query)/N;
    printf("%d queries finished in %0.6f seconds, overall throughput is %d qps, overall average response time is %ld nano seconds \n",
           N, (double)timer_delta_ms(&timer_query)/1000, throughputQ,responsetimeQ );

    free(attr_arr);
    exit(0);
}