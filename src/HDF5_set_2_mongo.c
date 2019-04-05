#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "../lib/hdf5/hdf52json.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include "../lib/utils/cmd_utils.h"
#include <unistd.h>


typedef struct parallel_args{
    int size;
    int rank;
    int current_file_count;
} parallel_args_t;



extern int64_t init_db();
extern int64_t clear_all_docs();
extern void clear_all_indexes();
extern void drop_current_coll();
extern void create_index(const char *index_key);
extern void create_doc_id_index();
extern void create_dataset_name_index();
extern void create_root_obj_path_index();
extern void create_lv2_obj_path_index();
extern void create_lv3_obj_path_index();
extern void create_any_index(const char *index_str);
extern int64_t query_count(const char *query_condition);
extern int64_t query_result_count(const char *query_condition);
extern void query_result_and_print(const char *query_condition);
extern int64_t get_all_doc_count();
extern int64_t importing_json_doc_to_db(const char *json_str);
extern int64_t split_sub_objects_to_db(const char *json_str);
extern int64_t importing_fake_json_docs_to_db(const char *json_str, int count);
extern void random_test();

void print_usage() {
    printf("Usage: ./hdf5_set_2_mongo /path/to/hdf5/file topk num_indexed_fields\n");
}

char *chk_mongo_size_cmd = "/usr/bin/mongo mongodb03.nersc.gov/HDF5MetadataTest -u HDF5MetadataTest_admin -p ekekek19294jdwss2k --eval 'db.runCommand({dbStats:1, scale:1024})' | egrep \"(indexSize|dataSize)\" | xargs echo";

void clear_everything(){
    drop_current_coll();
}

int parse_single_file(char *filepath, parallel_args_t *pargs) {

    // Each time this function is called, we consider one file is there. 

    pargs->current_file_count = pargs->current_file_count+1;
    if (pargs->current_file_count % pargs->size != pargs->rank) {
        return 0;
    }

    // ****** MongoDB has 16MB size limit on each document. ******
    // TODO: To confirm that you need to comment off line #32 in hdf52json.c
    // char *json_str = NULL;
    // parse_hdf5_meta_as_json_str(filepath, &json_str);
    // printf("%s\n", json_str);
    // printf("============= Importing %s to MongoDB =============\n", filepath);
    // importing_json_doc_to_db(json_str);
    
    

    // ****** Let's split the entire JSON into multiple sub objects ******
    // TODO: To confirm that you need to uncomment line #32 in hdf52json.c
    stopwatch_t one_file;
    stopwatch_t parse_file;
    stopwatch_t import_one_doc;

    timer_start(&one_file);
    timer_start(&parse_file);
    json_object *rootObj;
    parse_hdf5_file(filepath, &rootObj);

    // const char *root_json_str = json_object_to_json_string(rootObj);
    // printf("%s\n", root_json_str);

    timer_pause(&parse_file);
    json_object *root_array = NULL;
    json_object_object_get_ex(rootObj, "sub_objects", &root_array);
    size_t json_array_len = json_object_array_length(root_array);
    size_t idx = 0;
    timer_start(&import_one_doc);
    for (idx = 0; idx < json_array_len; idx++) {
        json_object *sub_group_object = json_object_array_get_idx(root_array, idx);
        json_object_object_add(sub_group_object, "hdf5_filename", 
            json_object_new_string(basename(filepath)));
        const char *json_doc = json_object_to_json_string(sub_group_object);
        importing_json_doc_to_db(json_doc);
        // json_object_put(sub_group_object);
    }
    
    timer_pause(&import_one_doc);
    timer_pause(&one_file);
    suseconds_t one_file_duration = timer_delta_us(&one_file);
    suseconds_t parse_file_duration = timer_delta_us(&parse_file);
    suseconds_t import_one_doc_duration = timer_delta_us(&import_one_doc);

    char *size_output=execute_cmd(chk_mongo_size_cmd);

    println("[IMPORT_META] Rank %d finished in %ld us for %s, with %ld us for parsing and %ld us for inserting. [MEM] : %s",
        pargs->rank, one_file_duration, basename(filepath), parse_file_duration, import_one_doc_duration, size_output);
    
    json_object_put(rootObj);
    // ******** There is another way which is to pass entire JSON object into insert_many function in Rust *****
    // // TODO: Timing for extracting and importing metadata object
    // // TODO: To confirm that you need to uncomment line #32 in hdf52json.c 
    // char *json_str = NULL;
    // // TODO: timing for extracting HDF5 metadata
    // parse_hdf5_meta_as_json_str(filepath, &json_str);
    // split_sub_objects_to_db(json_str);

    return 0;
}

int is_hdf5(const struct dirent *entry){
    if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0) {
        return 0;
    }
    if (entry->d_type == DT_DIR){
        return 1;
    }
    if( endsWith(entry->d_name, ".hdf5") || endsWith(entry->d_name, ".h5")) {
        return 1;
    }
    return 0;
}

int on_file(struct dirent *f_entry, const char *parent_path, void *args) {


    char *filepath = (char *)calloc(512, sizeof(char));

    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);
    parse_single_file(filepath, (parallel_args_t *)args);
    
    // println("[PARSEFILE]");
    sleep(10);

    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *args) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path, const int topk, void *args) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, args, NULL, NULL);
    return 0;
}

char *gen_query(int idx, char **attr_arr, char **value_arr, int *type_arr) {
    char *string_query_template = "{\"attributes.%s\":\"%s\"}";
    char *numeric_query_template = "{\"attributes.%s\":%s}";
    char *query = (char *)calloc(1024, sizeof(char));
    if (type_arr[idx]==0) { // String
        sprintf(query, string_query_template, attr_arr[idx], value_arr[idx]);
    } else if (type_arr[idx]==1) { // Int
        sprintf(query, numeric_query_template, attr_arr[idx], value_arr[idx]);
    } else if (type_arr[idx]==2) { // float
        sprintf(query, numeric_query_template, attr_arr[idx], value_arr[idx]);
    }
    return query;
}

char *gen_index_str(int idx, char **attr_arr) {
    // char *index_str_template = "{\"attributes.%s\":1}";
    char *index_str_template = "attributes.%s";
    char *index_str = (char *)calloc(1024, sizeof(char));
    sprintf(index_str, index_str_template, attr_arr[idx]);
    return index_str;
}

int
main(int argc, char **argv)
{

    int rank = 0, size = 1;

    #ifdef ENABLE_MPI
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
    #endif

    if (argc < 2) {
        print_usage();
        return 0;
    }
    int rst = 0;
    int topk = 0; // number of files to be scanned.
    int num_indexed_field = 0; //number of attributes to be indexed.
    int task_id = 0; // 0, insert, 1, query
    const char *path = argv[1];
    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    if (argc >=5) {
        task_id=atoi(argv[4]);
    }

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
    int search_types[] = {0,1,2,0,1,2,0,1,2,0,1,2,0,1,2,0};

    int64_t doc_count = init_db();
    println("successfully init db, %d documents in mongodb.", doc_count);
    int query_num = (num_indexed_field > 0)? num_indexed_field :16;

    if (task_id == 0) {

        if (rank == 0) { //only rank 0 need to clean DB. By default, there is only rank 0 if no MPI
            clear_everything();
            println("db cleaned!");
        }

        if (query_num > 0) {
            int f = 0;
            for (f = 0; f < query_num; f++) {
                char *index_str = gen_index_str(f, indexed_attr);
                create_any_index(index_str);
            }
        }

        parallel_args_t *pargs = (parallel_args_t *)calloc(1, sizeof(parallel_args_t));
        pargs->size = size;
        pargs->rank = rank;
        pargs->current_file_count = 0;
        
#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif


        if (is_regular_file(path)) {
            rst = parse_single_file((char *)path, pargs);
        } else {
            rst = parse_files_in_dir((char *)path, topk, pargs);
        }

    } else if (task_id == 1) {

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        //generate query:
        int i = 0;
        char **queries = (char **)calloc(query_num, sizeof(char *));
        for (i = 0; i < query_num; i++) {
            queries[i] = gen_query(i, indexed_attr, search_values, search_types);
        }


        stopwatch_t timer_search;
        timer_start(&timer_search);

        // issue queries
        int qcount = 0;
        for (i = 0; i < 1024; i++) {
            int c = i % query_num;
            qcount += query_count(queries[c]);
        }

        timer_pause(&timer_search);
        println("[META_SEARCH_MONGO] Time for 1024 queries on %d indexes and spent %d microseconds.", num_indexed_field, timer_delta_us(&timer_search));

    }



#ifdef ENABLE_MPI
    rst = MPI_Finalize();
#endif

    return rst;
}