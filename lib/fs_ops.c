#include "fs_ops.h"

void collect_dir(const char *dir_path, int (*filter)(struct dirent *entry),
    int (*on_file)(struct dirent *f_entry, const char *parent_path, void *args), 
    int (*on_dir)(struct dirent *d_entry, const char *parent_path, void *args), 
    void *coll_args){

    if (dir_path == NULL) { // if the given start_dir is not a valid struct.
        return;
    }

    DIR *dir;
    
    if (!(dir = opendir(dir_path))) { // if the given dir entry cannot be open.
        return;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            // Not visiting current directory and parental directory
            continue;
        }

        if (filter!=NULL && filter(entry)==0) {
                continue;
        }

        char *path = (char *)calloc(1024, sizeof(char));
        char *name = (char *)calloc(1024, sizeof(char));
        snprintf(name,1023, "%s", entry->d_name);
        snprintf(path, 1023, "%s/%s", dir_path, entry->d_name);

        if (entry->d_type == DT_DIR) {
            if (on_dir) {
                on_dir(entry, dir_path, coll_args);
            }
            collect_dir(path, filter, on_file, on_dir, coll_args);
        } else {
            if (on_file) {
                on_file(entry, dir_path, coll_args);
            }
        }
        free(path);
        free(name);
    }
    closedir(dir);
}