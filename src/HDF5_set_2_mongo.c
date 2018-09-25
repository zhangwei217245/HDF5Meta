#include "../lib/hdf52json.h"
#include "../lib/fs_ops.h"
#include "../lib/string_utils.h"

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
extern int64_t query_count(const char *query_condition);
extern int64_t query_result_count(const char *query_condition);
extern void query_result_and_print(const char *query_condition);
extern int64_t get_all_doc_count();
extern int64_t importing_json_doc_to_db(const char *json_str);
extern void random_test();

void print_usage() {
    printf("Usage: ./hdf5_reader /path/to/hdf5/file\n");
}

int parse_single_file(char *filepath) {
    char *json_str = NULL;
    parse_hdf5_meta_as_json_str(filepath, &json_str);
    printf("============= Importing %s to MongoDB =============\n", filepath);
    // printf("%s\n", json_str);
    importing_json_doc_to_db(json_str);
    return 0;
}

int is_hdf5(struct dirent *entry){

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
    parse_single_file(filepath);
    
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *args) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path) {
    collect_dir(path, is_hdf5, on_file, on_dir, NULL);
    return 0;
}

int
main(int argc, char **argv)
{
    char* path;
    int rst = 0;

    int64_t doc_count = init_db();
    printf("successfully init db, %d documents in mongodb.\n", doc_count);

    clear_everything();
    uint64_t doc_count = 1;
    while (doc_count == 0) {
        doc_count = get_all_doc_count();
    }
    printf("db cleaned!\n");

    if (argc != 2)
        print_usage();
    else {
        path = argv[1];
        if (is_regular_file(path)) {
            rst = parse_single_file(path);
        } else {
            rst = parse_files_in_dir(path);
        }
    }
    return rst;
}