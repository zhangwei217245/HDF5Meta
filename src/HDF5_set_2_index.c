#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "../lib/include/base_stdlib.h"
#include "../lib/index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"



#define INDEX_DIR_PATH "/global/cscratch1/sd/wzhang5/data/miqs/idx"
#define PATH_DELIMITER "/"
#define MDB_NAME_TEMPLATE "index_miqs_%d.mdb"
#define AOF_NAME_TEMPLATE "index_miqs_%d.aof"



void print_usage() {
    printf("Usage: ./hdf5_set_2_index /path/to/hdf5/dir topk num_indexed_fields persistence_type, /path/to/idx/dir\n");
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


int parse_single_hdf5_file(char *filepath, void *args){
    index_file_loading_param_t *pargs = (index_file_loading_param_t *)args;
    pargs->current_file_count = pargs->current_file_count+1;
    if (pargs->current_file_count % pargs->size != pargs->rank) {
        return 0;
    }
    parse_hdf5_file(filepath);
    return 0;
}

int on_file(struct dirent *f_entry, const char *parent_path, void *arg) {

    char *filepath = (char *)calloc(512, sizeof(char));

    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);
    
    parse_single_hdf5_file(filepath, arg);

    print_mem_usage(filepath);

    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *arg) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path, const int topk, void *args) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, args, NULL, NULL);
    return 0;
}

int on_mdb(struct dirent *f_entry, const char *parent_path, void *args){
    index_file_loading_param_t *param = (index_file_loading_param_t *)args;
    char *filepath = (char *)calloc(511, sizeof(char));
    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);

    if (param->is_building) {
        char *owning_idx_file_name = (char *)calloc(strlen(MDB_NAME_TEMPLATE)+11, sizeof(char));
        sprintf(owning_idx_file_name, MDB_NAME_TEMPLATE, param->rank);
        if (endsWith(filepath, owning_idx_file_name)){
            return 0;
        }
    }
    return load_mdb(filepath, param);
}

int on_aof(struct dirent *f_entry, const char *parent_path, void *args){
    index_file_loading_param_t *param = (index_file_loading_param_t *)args;
    char *filepath = (char *)calloc(strlen(parent_path)+11, sizeof(char));
    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);

    if (param->is_building) {
        char *owning_idx_file_name = (char *)calloc(strlen(AOF_NAME_TEMPLATE)+11, sizeof(char));
        sprintf(owning_idx_file_name, AOF_NAME_TEMPLATE, param->rank);
        if (endsWith(filepath, owning_idx_file_name)){
            return 0;
        }
    }
    return load_aof(filepath, param);
}

/**
 * load index from mdb files
 * return 1 for loaded if directory exists, otherwise, return 0;
 */
int load_mdb_files(char *index_dir, index_file_loading_param_t *param){
    if (dir_exists(index_dir)) {
        collect_dir(index_dir, is_mdb, alphasort, ASC, 0, on_mdb, NULL, param, NULL, NULL);
        return 1;
    } else {
        return 0;
    }
}

/**
 * load index from aof files
 * return 1 for loaded if directory exists, otherwise, return 0;
 */
int load_aof_files(char *index_dir, index_file_loading_param_t *param){
    if (dir_exists(index_dir)) {
        collect_dir(index_dir, is_aof, alphasort, ASC, 0, on_aof, NULL, param, NULL, NULL);
        return 1;
    } else {
        return 0;
    }
}

int 
main(int argc, char *argv[])
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

    char *index_dir_path = INDEX_DIR_PATH;

    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    if (argc >= 5){
        persistence_type = atoi(argv[4]);
    }

    if (argc >= 6) {
        index_dir_path = (char *)argv[5];
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

    index_file_loading_param_t *param = 
        (index_file_loading_param_t *)calloc(1, sizeof(index_file_loading_param_t));
    param->idx_anchor = idx_anchor;
    param->size = size;
    param->rank = rank;
    

    int need_to_build_from_scratch = 1;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (persistence_type > 0) {
        
        stopwatch_t disk_loading_time;
        timer_start(&disk_loading_time);
        char *persistence_type_name = "";
        // 1. try to load different index files, if return 1, means index files does not exists. 
        param->is_building = 0;
        if (persistence_type == 1) {// mdb
            need_to_build_from_scratch = (load_mdb_files(index_dir_path, param)==0);
            persistence_type_name="MDB";
        } else if (persistence_type == 2){ // aof
            need_to_build_from_scratch = (load_aof_files(index_dir_path, param)==0);
            persistence_type_name="AOF";
        }

        timer_pause(&disk_loading_time);
        if (!need_to_build_from_scratch) {
            println("[LOAD_INDEX_FROM_%s_FILE] Rank %d : Time for loading %ld kv-pairs was %ld us, %ld us on in-memory.", 
            persistence_type_name,
            rank,
            idx_anchor->total_num_kv_pairs, 
            timer_delta_us(&disk_loading_time));
        }
    } 

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (need_to_build_from_scratch==1) {
        // 1. resolve name
        char *full_file_name = (char *)calloc(strlen(index_dir_path)+strlen(MDB_NAME_TEMPLATE)+11, sizeof(char));
        strcpy(full_file_name, index_dir_path);
        if (!endsWith(full_file_name, "/")){
            strcat(full_file_name, PATH_DELIMITER);
        }
        char *file_name = (char *)calloc(strlen(MDB_NAME_TEMPLATE)+11, sizeof(char));
        if (persistence_type == 1) {
            sprintf(file_name, MDB_NAME_TEMPLATE, rank);
        } else if (persistence_type == 2) {
            sprintf(file_name, AOF_NAME_TEMPLATE, rank);
        }
        strcat(full_file_name, file_name);

        // before persistence, make sure the directory is there. 
        if (!dir_exists(index_dir_path)) {
            mkpath(index_dir_path, (S_IRWXU|S_IRWXG));
        }

        if (persistence_type == 2) {// AOF
            idx_anchor->on_disk_file_stream = fopen(full_file_name, "w");
            idx_anchor->is_readonly_index_file=0;
        }
        suseconds_t mem_indexing_time = 0;
        suseconds_t disk_indexing_time = 0;
        suseconds_t loading_other_index_time = 0;
        // build index from HDF5 files
        param->current_file_count=0;
        stopwatch_t hdf5_indexing_timer;
        timer_start(&hdf5_indexing_timer);
        int count = 0;
        if (is_regular_file(path)) {
            parse_single_hdf5_file((char *)path, param);
            rst = 0;
        } else {
            rst = parse_files_in_dir((char *)path, topk, param);
        }

        if (persistence_type == 2) {
            fclose(idx_anchor->on_disk_file_stream);
            disk_indexing_time += idx_anchor->us_to_disk_index;
        }
        
        if (persistence_type == 1) {//mdb
            // 2. dump to mdb file
            stopwatch_t mdb_indexing_time;
            timer_start(&mdb_indexing_time);

            dump_mdb_index_to_disk(full_file_name, idx_anchor);

            timer_pause(&mdb_indexing_time);
            disk_indexing_time += timer_delta_us(&mdb_indexing_time);
        }

        // 3. load index files by other processes
#ifdef ENABLE_MPI
        stopwatch_t loading_other_disk_index_time;
        timer_start(&loading_other_disk_index_time);

        MPI_Barrier(MPI_COMM_WORLD);
        param->is_building=1;
        if (persistence_type == 1) { // mdb
            need_to_build_from_scratch = load_mdb_files(index_dir_path, param);
        } else if (persistence_type ==2) { // aof
            need_to_build_from_scratch = load_aof_files(index_dir_path, param);
        }
        timer_pause(&loading_other_disk_index_time);
        loading_other_index_time = timer_delta_us(&loading_other_disk_index_time);
#endif
        timer_pause(&hdf5_indexing_timer);

        mem_cost_t *mem_usage = get_mem_cost();

        println("[LOAD_INDEX_FROM_HDF5_FILE] Rank %d : Time for loading index from %ld HDF5 files with %ld objects and %ld attributes and %ld kv-pairs was %ld us, %ld us on in-memory, %ld us on on-disk, %ld us for loading other index files. dataSize: %ld , indexSize : %ld", 
        rank,
        idx_anchor->total_num_files,
        idx_anchor->total_num_objects,
        idx_anchor->total_num_attrs,
        idx_anchor->total_num_kv_pairs,
        timer_delta_us(&hdf5_indexing_timer),
        idx_anchor->us_to_index,
        disk_indexing_time,
        loading_other_index_time,
        mem_usage->metadata_size,
        mem_usage->overall_index_size
        );
    }


#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

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
    println("[META_SEARCH_MEMO] Rank %d : Time for 1024 queries on %d indexes and spent %d microseconds.  %d", 
    rank, num_indexed_field, timer_delta_us(&timer_search), numrst);

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

