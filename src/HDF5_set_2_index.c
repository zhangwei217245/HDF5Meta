// #define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "../lib/include/base_stdlib.h"
#include "../lib/index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include <sys/stat.h>
#include <unistd.h>


#define INDEX_DIR_PATH "/global/cscratch1/sd/wzhang5/data/miqs/idx";
#define PATH_DELIMITER "/";
#define MDB_NAME_TEMPLATE "index_miqs_%d.mdb";
#define AOF_NAME_TEMPLATE "index_miqs_%d.aof";

void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/dir topk num_indexed_fields on_disk_file\n");
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

int on_file(struct dirent *f_entry, const char *parent_path, void *arg) {
    char *filepath = (char *)calloc(512, sizeof(char));

    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);
    parse_hdf5_file(filepath);

    print_mem_usage(filepath);
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *arg) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path, const int topk) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, NULL, NULL, NULL);
    return 0;
}

/**
 * @param on_disk_index_path mdb file path
 * 
 */
int load_mdb_files(int rank, index_anchor *idx_anchor, int is_building){
    char *file_name = (char *)calloc(strlen(mdb_name_template)+10, sizeof(char));

}


int 
main(int argc, char const *argv[])
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
    int persistence_type = 0; // none = 0; 1 = mdb, aof = 2



    const char *path = argv[1];
    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    if (argc >= 5){
        persistence_type = atoi(argv[4]);
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
    
    if (init_in_mem_index()==0) {
        return 0;
    }
    index_anchor *idx_anchor = root_idx_anchor();
    idx_anchor->indexed_attr = indexed_attr;
    idx_anchor->num_indexed_field = num_indexed_field;

    char *on_disk_index_path = (char *)calloc(strlen(on_disk_index_dir_path_template)+50, sizeof(char));

    int need_to_build_from_scratch = 1;

    if (persistence_type > 0) { 
        // 1. try to load different index files, if return 1, means index files does not exists. 
        if (persistence_type == 1) {// mdb
            need_to_build_from_scratch = load_mdb_files(rank, idx_anchor, 0);
        } else if (persistence_type == 2){ // aof
            need_to_build_from_scratch = load_aof_files(rank, idx_anchor, 0);
        }
    } 

    if (need_to_build_from_scratch==1) {
        // build index from HDF5 files
        if (persistence_type == 2) {
            idx_anchor->on_disk_file_stream = fopen(on_disk_index_path, "w");
            idx_anchor->is_readonly_index_file=0;
        }
        
        suseconds_t mem_indexing_time = 0;
        suseconds_t disk_indexing_time = 0;
        stopwatch_t hdf5_indexing_time;
        timer_start(&hdf5_indexing_time);
        int count = 0;
        if (is_regular_file(path)) {
            parse_hdf5_file((char *)path);
            rst = 0;
        } else {
            rst = parse_files_in_dir((char *)path, topk);
        }
        if (persistence_type == 2) {
            fclose(idx_anchor->on_disk_file_stream);
            disk_indexing_time += idx_anchor->us_to_disk_index;
        }
        
        if (persistence_type == 1) {//mdb
            // 1. resolve name
            char *full_file_name = (char *)calloc(strlen(INDEX_DIR_PATH)+strlen(MDB_NAME_TEMPLATE)+11, sizeof(char));
            strcpy(full_file_name, INDEX_DIR_PATH);
            strcat(full_file_name, PATH_DELIMITER);
            char *file_name = (char *)calloc(strlen(MDB_NAME_TEMPLATE)+11, sizeof(char));
            sprintf(file_name, MDB_NAME_TEMPLATE, rank);
            strcat(full_file_name, file_name);
            // 2. dump to mdb file
            stopwatch_t mdb_indexing_time;
            timer_start(&mdb_indexing_time;);

            dump_mdb_index_to_disk(full_file_name);

            timer_pause(&mdb_indexing_time);
            disk_indexing_time += timer_delta_us(&mdb_indexing_time);

#ifdef ENABLE_MPI
            // 3. load mdb by other processes

#endif
        }

        timer_pause(&hdf5_indexing_time);
        println("[LOAD_INDEX_FROM_HDF5_FILE] Rank %d : Time for loading index from %ld HDF5 files with %ld objects and %ld attributes and %ld kv-pairs was %ld us, %ld us on in-memory, %ld us on on-disk.", 
        rank,
        idx_anchor->total_num_files,
        idx_anchor->total_num_objects,
        idx_anchor->total_num_attrs,
        idx_anchor->total_num_kv_pairs,
        timer_delta_us(&hdf5_indexing_time),
        idx_anchor->us_to_index,
        disk_indexing_time
        );
    }

    // check if index file exists

    // TODO: check if the index dir exists. If true, load index files, otherwise, build index files.
    
    if (access(on_disk_index_path, F_OK)==0 && 
        access(on_disk_index_path, R_OK)==0 
        ){
        size_t fsize = get_file_size(on_disk_index_path);
        if (fsize > 0) {
            // file exists, readable. try to load index 
            idx_anchor->on_disk_file_stream = fopen(on_disk_index_path, "r");
            idx_anchor->is_readonly_index_file=1;
            fseek(idx_anchor->on_disk_file_stream, 0, SEEK_SET);
            stopwatch_t disk_indexing_time;
            timer_start(&disk_indexing_time);
            size_t count = 0;
            while (1) {
                index_record_t *ir = read_index_record(idx_anchor->on_disk_file_stream);
                if (ir == NULL) {
                    break;
                }
                // convert to required parameters from IR.
                h5attribute_t attr;
                convert_index_record_to_in_mem_parameters(idx_anchor, &attr, ir);
                //insert into in-mem index.
                on_attr((void *)idx_anchor, &attr);
                count++;
            }
            fclose(idx_anchor->on_disk_file_stream);

            timer_pause(&disk_indexing_time);
            println("[LOAD_INDEX_FROM_MIQS_FILE] Time for loading %ld index records and get %ld kv-pairs was %ld us, %ld us on in-memory.", 
            count, idx_anchor->total_num_kv_pairs, 
            timer_delta_us(&disk_indexing_time),
            idx_anchor->us_to_index);
            need_to_build_from_scratch = 0;
        } else {
            need_to_build_from_scratch = 1;
        }
    } 
    
    

    print_mem_usage("MEMALL");

    int num_queries = 16;
    if (num_indexed_field > 0) {
        num_queries = num_indexed_field;
    }

    stopwatch_t timer_search;
    timer_start(&timer_search);
    int numrst = 0;
    int i = 0;
    for (i = 0; i < 1024; i++) {
        int c = i % 16;
        if (search_types[c]==1) {
            int value = atoi(search_values[c]);
            power_search_rst_t *rst = int_value_search(indexed_attr[c], value);
            numrst += (rst->num_files);
        }else if (search_types[c]==2) {
            double value = atof(search_values[c]);
            power_search_rst_t *rst = float_value_search(indexed_attr[c], value);
            numrst += (rst->num_files); 
        } else {
            char *value = search_values[c];
            power_search_rst_t *rst = string_value_search(indexed_attr[c], value);
            numrst += (rst->num_files); 
        }
    }
    timer_pause(&timer_search);
    println("[META_SEARCH_MEMO] Time for 1024 queries on %d indexes and spent %d microseconds.  %d", 
    num_indexed_field, timer_delta_us(&timer_search), numrst);

    // search on disk
    // timer_start(&timer_search);
    // numrst = 0;
    // i = 0;
    // // open file
    // FILE *idx_to_search = fopen(on_disk_index_path, "r");
    // for (i = 0; i < 1024; i++) {
        
    //     int c = i % 16;
    //     if (search_types[c]==1) {
    //         int value = atoi(search_values[c]);
    //         int out_len = 0;
    //         index_record_t **query_rst = 
    //         find_index_record(indexed_attr[c], int_equals, &value, idx_to_search, &out_len);
    //         numrst += out_len;
    //     }else if (search_types[c]==2) {
    //         double value = atof(search_values[c]);
    //         int out_len = 0;
    //         index_record_t **query_rst = 
    //         find_index_record(indexed_attr[c], double_equals, &value, idx_to_search, &out_len);
    //         numrst += out_len;
    //     } else {
    //         char *value = search_values[c];
    //         int out_len = 0;
    //         index_record_t **query_rst = 
    //         find_index_record(indexed_attr[c], string_equals, &value, idx_to_search, &out_len);
    //         numrst += out_len;
    //     }
    // }
    // fclose(idx_to_search);
    // timer_pause(&timer_search);
    // println("[META_SEARCH_DISK] Time for 1024 queries on %d indexes and spent %d microseconds.  %d", 
    // num_indexed_field, timer_delta_us(&timer_search), numrst);
#ifdef ENABLE_MPI
    rst = MPI_Finalize();
#endif
    return rst;
}

