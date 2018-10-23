#include "../lib/include/base_stdlib.h"
#include "../lib/hdf5/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"

index_anchor *idx_anchor;

void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/dir topk\n");
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
    parse_hdf5_file(filepath, (index_anchor *)arg);
    
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *arg) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path, const int topk, index_anchor *idx_anchor) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, idx_anchor, NULL, NULL);
    return 0;
}

int 
main(int argc, char const *argv[])
{
    if (argc < 2) {
        print_usage();
        return 0;
    }
    int rst = 0;
    int topk = 0;
    const char *path = argv[1];
    if (argc == 3) {
        topk = atoi(argv[2]);
    }

    char *indexed_attr[]={"COLLA", "DARKTIME", "BADPIXEL", "FILENAME", "EXPOSURE", "COLLB", NULL};
    char *search_values[]={"27089", "0", "badpixels-56149-b1.fits.gz", "sdR-b2-00154990.fit", "155701", "26660", NULL};

    int search_types[] = {1,1,0,0,1,1};
    
    idx_anchor = (index_anchor *)calloc(1, sizeof(index_anchor));

    if (is_regular_file(path)) {
        parse_hdf5_file((char *)path, idx_anchor);
        rst = 0;
    } else {
        rst = parse_files_in_dir((char *)path, topk, idx_anchor);
    }
    
    int i = 0;
    for (i = 0; i < 1000; i++) {
        int numrst = -1;
        stopwatch_t timer_search;
        timer_start(&timer_search);

        int c = i%6;
        if (search_types[c]) {
            int value = atoi(search_values[c]);
            search_result_t *rst = NULL;
            numrst += int_value_search(idx_anchor, indexed_attr[c], value, &rst);
        } else {
            char *value = search_values[c];
            search_result_t *rst = NULL;
            numrst += string_value_search(idx_anchor, indexed_attr[c], value, &rst);
        }

        timer_pause(&timer_search);

        println("Time for 1000 queries get %d results and spent %d microseconds.\n", numrst, timer_delta_us(&timer_search));
    }

    return rst;
}

