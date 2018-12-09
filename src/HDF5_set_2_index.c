#include "../lib/include/base_stdlib.h"
#include "../lib/hdf5/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"

index_anchor *idx_anchor;

size_t index_num;
size_t file_num;


void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/dir topk num_indexed_fields\n");
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
    index_anchor *idx_anchor = (index_anchor *)arg;

    sprintf(filepath, "%s/%s", parent_path, f_entry->d_name);
    parse_hdf5_file(filepath, idx_anchor);

    idx_anchor->total_num_files+=1;
    // print_mem_usage();
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
    int topk = 0; // number of files to be scanned.
    int num_indexed_field = 0; //number of attributes to be indexed.
    const char *path = argv[1];
    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    char *indexed_attr[]={
        "AUTHOR", 
        "BESTEXP", 
        "HELIO_RV",
        "FILENAME", 
        "DARKTIME", 
        "IOFFSTD",
        "EXPOSURE", 
        "BADPIXEL", 
        "CRVAL1",
        "LAMPLIST",
        "COLLB", 
        "M1PISTON",
        "COMMENT",
        "HIGHREJ",
        "FBADPIX2", 
        "DAQVER",
        NULL};
    char *search_values[]={
        "Scott Burles & David Schlegel",
        "103179", 
        "26.6203",
        "badpixels-56149-b1.fits.gz", 
        "0", 
        "0.0133138",
        "sdR-b2-00154990.fit", 
        "155701", 
        "3.5528",
        "lamphgcdne.dat",
        "26660", 
        "661.53",
        "sp2blue cards follow",
        "8",
        "0.231077", 
        "1.2.7",
        NULL};

    //  string value = 0, int value = 1, float value = 2
    int search_types[] = {0,1,2,0,1,2,0,1,2,0,1,2,0,1,2,0};
    
    idx_anchor = (index_anchor *)calloc(1, sizeof(index_anchor));
    idx_anchor->indexed_attr = indexed_attr;
    idx_anchor->num_indexed_field = num_indexed_field;

    if (is_regular_file(path)) {
        parse_hdf5_file((char *)path, idx_anchor);
        rst = 0;
    } else {
        rst = parse_files_in_dir((char *)path, topk, idx_anchor);
    }

    int num_queries = 16;
    if (num_indexed_field > 0) {
        num_queries = num_indexed_field;
    }

    stopwatch_t timer_search;
    timer_start(&timer_search);
    int numrst = 0;
    int i = 0;
    for (i = 0; i < 1024; i++) {
        int c = i%num_queries;
        if (search_types[c]==1) {
            int value = atoi(search_values[c]);
            search_result_t *rst = NULL;
            numrst += int_value_search(idx_anchor, indexed_attr[c], value, &rst);
        }else if (search_types[c]==2) {
            double value = atof(search_values[c]);
            search_result_t *rst = NULL;
            numrst += float_value_search(idx_anchor, indexed_attr[c], value, &rst); 
        } else {
            char *value = search_values[c];
            search_result_t *rst = NULL;
            numrst += string_value_search(idx_anchor, indexed_attr[c], value, &rst);
        }
    }

    timer_pause(&timer_search);
    println("[META_SEARCH] Time for 1024 queries on %d indexes and spent %d microseconds.", num_indexed_field, timer_delta_us(&timer_search));

    print_mem_usage();

    return rst;
}

