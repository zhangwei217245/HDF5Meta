#include "fs_ops.h"

int desc_cmp(int v, int end){
    return v >= end;
}

int asc_cmp(int v, int end) {
    return v <= end;
}

int incr(int a){
    return a+1;
}

int decr(int a){
    return a-1;
}

void collect_dir(const char *dir_path, int (*filter) (const struct dirent *), 
    int (*cmp) (const struct dirent **, const struct dirent **), 
    sorting_direction_t sd, const int topk,
    int (*on_file)(struct dirent *f_entry, const char *parent_path, void *args), 
    int (*on_dir)(struct dirent *d_entry, const char *parent_path, void *args), 
    void *coll_args){

    if (dir_path == NULL) { // if the given start_dir is not a valid struct.
        return;
    }

    struct dirent **namelist;
    int n;
    n = scandir(dir_path, &namelist, filter, cmp);
    if (n < 0) {
        perror("error occurred at scandir");
    } else {
        int v, end;
        int (*cmp_nl)(int, int);
        int (*v_act)(int);
        int count = 0;
        if (sd == DESC) {
            v = n - 1;
            end = 0;
            cmp_nl = desc_cmp;
            v_act = decr;
        } else {
            v = 0;
            end = n - 1;
            cmp_nl = asc_cmp;
            v_act = incr;
        }
        while (cmp_nl(v, end) && (topk > 0 ? count < topk : 1)) {
            struct dirent *entry = namelist[v];
            char *path = (char *)calloc(1024, sizeof(char));
            char *name = (char *)calloc(1024, sizeof(char));
            snprintf(name,1023, "%s", entry->d_name);
            snprintf(path, 1023, "%s/%s", dir_path, entry->d_name);
            if (entry->d_type == DT_DIR) {

                collect_dir(path, filter, cmp, sd, topk, on_file, on_dir, coll_args);
                if (on_dir) {
                    on_dir(entry, dir_path, coll_args);
                }
            } else {
                if (on_file) {
                    on_file(entry, dir_path, coll_args);
                }
            }
            free(path);
            free(name);
            free(namelist[v]);
            v = v_act(v);
            count++;
        }
        free(namelist);
    }
}


int is_regular_file(const char *path){
    struct stat path_stat;
    stat(path, &path_stat);
    return S_ISREG(path_stat.st_mode);
}