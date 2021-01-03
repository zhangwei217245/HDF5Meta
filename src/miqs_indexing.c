#include "miqs_indexing.h"
#include "utils/cbase/base_stdlib.h"
#include "h52index/hdf52index.h"
#include "utils/fs/fs_ops.h"
#include "utils/string_utils.h"
#include "utils/timer_utils.h"
#include "miqs_config.h"

#define ENABLE_MPI 

#ifdef ENABLE_MPI
#include "mpi.h"
#endif


#define PATH_DELIMITER "/"
#define INDEX_FILE_NAME_TEMPLATE "index_miqs_%d.%s"


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
    indexing_config_t *pargs = (indexing_config_t *)args;
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
    indexing_config_t *param = (indexing_config_t *)args;
    char *filepath = (char *)calloc(strlen(parent_path)+11, sizeof(char));
    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);

    if (param->is_build_from_scratch) {
        char *owning_idx_file_name = (char *)calloc(strlen(INDEX_FILE_NAME_TEMPLATE)+16, sizeof(char));
        sprintf(owning_idx_file_name, INDEX_FILE_NAME_TEMPLATE, param->rank, "mdb");
        if (endsWith(filepath, owning_idx_file_name)){
            return 0;
        }
    }
    return load_mdb(filepath, root_idx_anchor());
}

int on_aof(struct dirent *f_entry, const char *parent_path, void *args){
    indexing_config_t *param = (indexing_config_t *)args;
    char *filepath = (char *)calloc(strlen(parent_path)+11, sizeof(char));
    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);

    if (param->is_build_from_scratch) {
        char *owning_idx_file_name = (char *)calloc(strlen(INDEX_FILE_NAME_TEMPLATE)+16, sizeof(char));
        sprintf(owning_idx_file_name, INDEX_FILE_NAME_TEMPLATE, param->rank, "aof");
        if (endsWith(filepath, owning_idx_file_name)){
            return 0;
        }
    }
    return load_aof(filepath, root_idx_anchor());
}


/**
 * load index from mdb files
 * return 1 for loaded if directory exists, otherwise, return 0;
 */
int load_mdb_files(char *index_dir, indexing_config_t *param){
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
int load_aof_files(char *index_dir, indexing_config_t *param){
    if (dir_exists(index_dir)) {
        collect_dir(index_dir, is_aof, alphasort, ASC, 0, on_aof, NULL, param, NULL, NULL);
        return 1;
    } else {
        return 0;
    }
}


indexing_config_t *init_indexing_config(int parallelism, 
                                    int rank, 
                                    int size, 
                                    int topk, 
                                    int enable_mdb, 
                                    int enable_aof,
                                    char *dataset_path,
                                    char *index_dir_path){
    indexing_config_t *param = (indexing_config_t *)calloc(1, sizeof(indexing_config_t));
#ifdef MIQS_PARALLELED_INDEX
    param->parallelism = parallelism;
#else
    param->parallelism = 1;
#endif
    param->rank = rank;
    param->size = size;
    param->topk = topk;
    param->is_mdb_enabled = enable_mdb;
    param->is_aof_enabled = enable_aof;
    param->dataset_path = dataset_path;
    param->index_dir_path = index_dir_path;
    return param;
}

char *resolve_file_name(char *index_dir_path, const char* suffix, int rank){
    char *full_file_name = (char *)calloc(strlen(index_dir_path)+strlen(INDEX_FILE_NAME_TEMPLATE)+20, sizeof(char));
    strcpy(full_file_name, index_dir_path);
    if (!endsWith(full_file_name, PATH_DELIMITER)){
        strcat(full_file_name, PATH_DELIMITER);
    }
    char *file_name = (char *)calloc(strlen(INDEX_FILE_NAME_TEMPLATE)+16, sizeof(char));
    sprintf(file_name, INDEX_FILE_NAME_TEMPLATE, rank, suffix);
    strcat(full_file_name, file_name);
    return full_file_name;
}

int indexing_data_collection(indexing_config_t *param){
    int rst = 0;
    param->is_build_from_scratch = 1;
    int rank = param->rank;
    int size = param->size;
    int topk = param->topk;
    // 1. initialize in-memory index first.
    init_in_mem_index(param->parallelism);
    index_anchor_t *idx_anchor = root_idx_anchor();
    // param->_idx_anchor = idx_anchor;
    int idp_len = strlen(param->index_dir_path)+1;
    int dp_len = strlen(param->dataset_path)+1;
    char *index_dir_path = (char *)calloc(idp_len, sizeof(char));
    char *data_path = (char *)calloc(dp_len, sizeof(char));
    strncpy(index_dir_path, param->index_dir_path, strlen(index_dir_path));
    strncpy(data_path, param->dataset_path, strlen(data_path));

    // 2. resolve index file name
    char *mdb_file_name = resolve_file_name(index_dir_path, "mdb", rank);
    char *aof_file_name = resolve_file_name(index_dir_path, "aof", rank);

    // 3. before persistence, make sure the directory is there. 
    if (!dir_exists(index_dir_path)) {
        mkpath(index_dir_path, (S_IRWXU|S_IRWXG));
    }
    // 4. Open AOF file if needed.
    if (param->is_aof_enabled == 1) {// AOF
        idx_anchor->on_disk_file_stream = fopen(aof_file_name, "w");
        idx_anchor->is_readonly_index_file=0;
    }
    // suseconds_t mem_indexing_time = 0;
    suseconds_t disk_indexing_time = 0;
    suseconds_t loading_other_index_time = 0;
    // 5. build index from HDF5 files
    param->current_file_count=0;
    stopwatch_t hdf5_indexing_timer;
    timer_start(&hdf5_indexing_timer);
    int count = 0;
    if (is_regular_file(data_path)) {
        parse_single_hdf5_file((char *)data_path, param);
        rst = 0;
    } else {
        rst = parse_files_in_dir((char *)data_path, topk, param);
    }

    // 6. close AOF file, if AOF is enabled.
    if (param->is_aof_enabled == 1) {
        fclose(idx_anchor->on_disk_file_stream);
        disk_indexing_time += idx_anchor->us_to_disk_index;
    }
    
    // 7. dump in-memory index to MDB file, if MDB is enabled.
    if (param->is_mdb_enabled == 1) {//mdb
        stopwatch_t mdb_indexing_time;
        timer_start(&mdb_indexing_time);

        dump_mdb_index_to_disk(mdb_file_name, idx_anchor);

        timer_pause(&mdb_indexing_time);
        disk_indexing_time += timer_delta_us(&mdb_indexing_time);
    }

    // 8. load index files generated by other processes
#ifdef ENABLE_MPI
    stopwatch_t loading_other_disk_index_time;
    timer_start(&loading_other_disk_index_time);

    MPI_Barrier(MPI_COMM_WORLD);
    param->is_build_from_scratch=1;
    if (param->is_mdb_enabled == 1) { // mdb
        rst |= !load_mdb_files(index_dir_path, param);
    } 
    if (param->is_aof_enabled == 1) { // aof
        rst |= !load_aof_files(index_dir_path, param);
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
    return rst;
}

int recovering_index(indexing_config_t *param){
    int rst = 0;

    int idp_len = strlen(param->index_dir_path)+1;
    char *index_dir_path = (char *)calloc(idp_len, sizeof(char));
    strncpy(index_dir_path, param->index_dir_path, strlen(index_dir_path));

    // param->_idx_anchor = NULL;
    param->is_build_from_scratch=0;
    stopwatch_t disk_loading_time;
    timer_start(&disk_loading_time);
    char *persistence_type_name = "";
    // 1. try to load different index files, if return 1, means index files does not exists. 
    if (param->is_mdb_enabled == 1) {// mdb
        rst = !load_mdb_files(index_dir_path, param);
        persistence_type_name="MDB";
    }
    // param->_idx_anchor = root_idx_anchor();
    if (param->is_aof_enabled == 1){ // aof
        rst = !load_aof_files(index_dir_path, param);
        persistence_type_name="AOF";
    }

    timer_pause(&disk_loading_time);
    if (!rst) {
        println("[LOAD_INDEX_FROM_%s_FILE] Rank %d : Time for loading %ld kv-pairs was %ld us, %ld us on in-memory.", 
        persistence_type_name,
        param->rank,
        root_idx_anchor()->total_num_kv_pairs, 
        timer_delta_us(&disk_loading_time));
    }
    return rst;
}