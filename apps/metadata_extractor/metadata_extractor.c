#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "miqs_indexing.h"
#include "miqs_querying.h"
#include "utils/timer_utils.h"
#include "utils/string_utils.h"
#include "utils/fs/fs_ops.h"

#include "metadata/miqs_metadata.h"
#include "metadata/miqs_meta_collector.h"
#include "metadata/hdf5_meta_extractor.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>


#define PATH_DELIMITER "/"
#define INDEX_FILE_NAME_TEMPLATE "index_miqs_%d.%s"


typedef struct {
    int rank;                               /** < rank of the current MPI process */
    int size;                               /** < number of all MPI processes */
    int current_file_count;                 /** < The number of files current process has encountered */
    int processed_file_count;               /** < The number of files current process has processed */

    char *h5_obj_path;
    char *h5_file_path;

    long total_num_attr_names;                  /** total number of distinct attribute names */
    long total_num_attr_kv_pairs;               /** total number of distinct attribute key-value pairs*/
    long total_num_objs;                        /** total number of distinct objects*/
    
    FILE *output_file;                     /** the file handle that refers to the text file. We should append every attribute kv pair into this file. */
} extractor_config_t;



int on_obj(void *opdata, miqs_data_object_t *obj){
    extractor_config_t *ex_config = (extractor_config_t *)opdata;
    size_t path_len = strlen(obj->obj_name)+1;
    ex_config->h5_obj_path = (char *)calloc(path_len+1, sizeof(char));
    strncpy(ex_config->h5_obj_path, obj->obj_name, path_len);
    ex_config->total_num_objs+=1;
    return 1;
}


int on_attr(void *opdata, miqs_meta_attribute_t *attr){
    extractor_config_t *ex_config = (extractor_config_t *)opdata;
    attr->file_path_str = ex_config->h5_file_path;
    attr->obj_path_str = ex_config->h5_obj_path;

    ex_config->total_num_attr_names+=1;

    char *attr_name = attr->attr_name;
    int rst = 0;
    if (attr->attr_type == MIQS_AT_INTEGER) {
        int *int_value = (int *)attribute_value;
        int len = attr->attribute_value_length;
        int c = 0;
        for (c = 0; c < len; c++) {
            ex_config->total_num_attr_kv_pairs+=1;
            fprintf(ex_config->output_file, "%s INT %d %s %s\n", attr_name, int_value[c], attr->file_path_str, attr->obj_path_str);
        }
    } else if(attr->attr_type == MIQS_AT_FLOAT) {
        double *float_value = (double *)attribute_value;
        int len = attr->attribute_value_length;
        int c = 0;
        for (c = 0; c < len; c++) {
            ex_config->total_num_attr_kv_pairs+=1;
            printf("%s FLT %.8f %s %s\n", attr_name, float_value[c], attr->file_path_str, attr->obj_path_str);
        }
    } else if(attr->attr_type == MIQS_AT_STRING) {
        char **string_value = (char **)attribute_value;
        int len = attr->attribute_value_length;
        int c = 0;
        for (c = 0; c < len; c++) {
            ex_config->total_num_attr_kv_pairs+=1;
            printf("%s STR %s %s %s\n", attr_name, string_value[c], attr->file_path_str, attr->obj_path_str);
        }
    } else {
        // just ignore any unknown type for now.
    }
    return rst;
}

int scan_single_hdf5_file(char *file_path, void *args){
    extractor_config_t *pargs = (extractor_config_t *)args;
    pargs->current_file_count = pargs->current_file_count+1;
    if (pargs->current_file_count % pargs->size != pargs->rank) {
        return 0;
    }

    pargs->h5_file_path=(char *)calloc(strlen(file_path)+1, sizeof(char)); 
    strncpy(pargs->file_path, file_path, strlen(file_path));
    
    miqs_metadata_collector_t *meta_collector = (miqs_metadata_collector_t *)calloc(1, sizeof(miqs_metadata_collector_t));
    init_metadata_collector(meta_collector, 0, args, NULL, on_obj, on_attr);

    scan_hdf5(file_path, meta_collector, 0);
    pargs->processed_file_count+=1;
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
    
    scan_single_hdf5_file(filepath, arg);

    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *arg) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int scan_files_in_dir(char *path, const int topk, void *args) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, args, NULL, NULL);
    return 0;
}

void init_extractor_config_t(extractor_config_t *config){
    config->current_file_count=0;
    config->processed_file_count=0;
    config->total_num_attr_names = 0;                  /** total number of distinct attribute names */
    config->total_num_attr_kv_pairs = 0;               /** total number of distinct attribute key-value pairs*/
    config->total_num_objs = 0;                        /** total number of distinct objects*/
}


int
main (int argc, char **argv)
{
    int rank = 0, size = 1;
    int rst = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    printf("%d out of %d\n", rank, size);

    if (argc < 3) {
        printf("metadata_extractor <INPUT_DIR> <OUTPUT_DIR> <TOPK>\n");
        return 1;
    }

    char *INPUT_DIR = argv[1];
    char *OUTPUT_DIR = argv[2];

    extractor_config_t *param = (extractor_config_t *)calloc(1, sizeof(extractor_config_t));
    init_extractor_config_t(param);

    int topk = -1;
    if (argc > 3) {
        topk = atoi(argv[3]);
    }

    param->size=size;
    param->rank=rank;
    
    char *output_file_path=(char *)calloc(1000, sizeof(char));
    sprintf(output_file_path, "%s/rank%d_out.txt", OUTPUT_DIR, rank);
    param->output_file = fopen(output_file_path, "w");

    if (is_regular_file(INPUT_DIR)) {
        scan_single_hdf5_file((char *)INPUT_DIR, param);
        rst = 0;
    } else {
        rst = scan_files_in_dir((char *)INPUT_DIR, topk, param);
    }

    // generate or print the statistic
    fprintf(param->output_file, "file_count: %d obj_count: %d attr_name_count: %d attr_kv_pair_count %d\n", 
        param->processed_file_count, param->total_num_objs, param->total_num_attr_names, param->total_num_attr_kv_pairs);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return rst;
}