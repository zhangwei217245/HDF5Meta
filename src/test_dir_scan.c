#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"

typedef struct branch_depth{
    int depth;
} branch_depth_t;

int only_sub(const struct dirent *entry) {
    if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0) {
        return 0;
    }
    return 1;
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
    branch_depth_t *b_depth = (branch_depth_t *)args;
    printf("%*s- %s/%s\n", b_depth->depth, " ", parent_path, f_entry->d_name);
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *args) {
    branch_depth_t *b_depth = (branch_depth_t *)args;
    printf("%*s[%s/%s]\n", b_depth->depth, " ", parent_path, d_entry->d_name);
    return 1;
}

int pre_op(void *arg){
    branch_depth_t *b_depth = (branch_depth_t *)arg;
    b_depth->depth+=4;
    return 1;
}

int post_op(void *arg){
    branch_depth_t *b_depth = (branch_depth_t *)arg;
    b_depth->depth-=4;
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("test_dir_scan <file_path>");
        return 0;
    }
    int topk = 0;
    char *path = argv[1];
    if (argc == 3) {
        topk = atoi(argv[2]);
    }
    
    branch_depth_t *d_depth = (branch_depth_t *)calloc(1, sizeof(branch_depth_t));
    d_depth->depth = 1;

    collect_dir(path, only_sub, alphasort, ASC, topk, on_file, on_dir, d_depth, pre_op, post_op);
}