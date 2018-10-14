#include "fs_ops.h"
#include "string_utils.h"

typedef struct branch_depth{
    int depth;
} branch_depth_t;

extern void collect_dir(const char *dir_path, int (*filter)(struct dirent *entry),
    int (*on_file)(struct dirent *f_entry, const char *parent_path, void *args), 
    int (*on_dir)(struct dirent *d_entry, const char *parent_path, void *args), 
    void *coll_args);

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
    branch_depth_t *b_depth = (branch_depth_t *)args;
    printf("%*s- %s/%s\n", b_depth->depth+1, " ", parent_path, f_entry->d_name);
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *args) {
    branch_depth_t *b_depth = (branch_depth_t *)args;
    b_depth->depth+=2;
    printf("%*s[%s/%s]\n", b_depth->depth, " ", parent_path, d_entry->d_name);
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("test_dir_scan <file_path>");
    }
    char *path = argv[1];
    
    branch_depth_t *d_depth = (branch_depth_t *)calloc(1, sizeof(branch_depth_t));
    d_depth->depth = 1;

    collect_dir(path, is_hdf5, on_file, on_dir, d_depth);
}