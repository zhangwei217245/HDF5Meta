#include "../lib/fs_ops.h"
#include "../lib/string_utils.h"

extern void collect_dir(dir_entry_t *start_dir, int (*filter)(dir_entry_t *d_entry),
    int (*on_file)(dir_entry_t *f_entry), 
    int (*on_dir)(dir_entry_t *d_entry));

int is_hdf5(dir_entry_t *d_entry){
    // int ends_with_HDF5 = endsWith(d_entry->name, ".hdf5");
    
    // if (d_entry->dir_type==FILE_ENTRY && ends_with_HDF5){
    //     return 1;
    // } else {
    //     return 0;
    // }

    return 1;
}

int on_file(dir_entry_t *f_entry) {
    printf("%*s- %s\n", f_entry->depth+1, " ", f_entry->name);
    return 1;
}

int on_dir(dir_entry_t *d_entry) {
    printf("%*s[%s]\n", d_entry->depth+1, " ", d_entry->name);
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("test_dir_scan <file_path>");
    }
    char *path = argv[1];
    dir_entry_t *start_dir = (dir_entry_t *)calloc(1, sizeof(dir_entry_t));
    init_dir_entry(path, start_dir);
    collect_dir(start_dir, is_hdf5, on_file, on_dir);
}