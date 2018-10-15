#include "../include/base_stdlib.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

typedef enum {
    ASC, DESC
} sorting_direction_t;

void collect_dir(const char *dir_path, int (*selector) (const struct dirent *),
    int (*cmp) (const struct dirent **, const struct dirent **), 
    sorting_direction_t sd, int topk, 
    int (*on_file)(struct dirent *f_entry, const char *parent_path, void *args), 
    int (*on_dir)(struct dirent *d_entry, const char *parent_path, void *args), 
    void *coll_args,
    int (*pre_op)(void *coll_args),
    int (*post_op)(void *coll_args));


