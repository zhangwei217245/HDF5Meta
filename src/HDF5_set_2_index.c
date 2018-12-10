#include "../lib/include/base_stdlib.h"
#include "../lib/index/hdf52index.h"
#include "../lib/fs/fs_ops.h"
#include "../lib/utils/string_utils.h"
#include "../lib/utils/timer_utils.h"
#include <sys/stat.h>
#include <unistd.h>


void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/dir topk num_indexed_fields on_disk_file\n");
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
    parse_hdf5_file(filepath);

    print_mem_usage(filepath);
    return 1;
}

int on_dir(struct dirent *d_entry, const char *parent_path, void *arg) {
    // char *dirpath = (char *)calloc(512, sizeof(char));
    // sprintf(dirpath, "%s/%s", parent_path, d_entry->d_name);
    // Nothing to do here currently.
    return 1;
}

int parse_files_in_dir(char *path, const int topk) {
    collect_dir(path, is_hdf5, alphasort, ASC, topk, on_file, on_dir, NULL, NULL, NULL);
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
    char *on_disk_index_path = "./index_miqs.idx";
    const char *path = argv[1];
    if (argc >= 3) {
        topk = atoi(argv[2]);
    }
    if (argc >= 4) {
        num_indexed_field = atoi(argv[3]);
    }

    if (argc >= 5) {
        on_disk_index_path = (char *)argv[4];
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
    
    if (init_in_mem_index()==0) {
        return 0;
    }
    index_anchor *idx_anchor = root_idx_anchor();
    idx_anchor->indexed_attr = indexed_attr;
    idx_anchor->num_indexed_field = num_indexed_field;

    // check if index file exists
    
    if (access(on_disk_index_path, F_OK)==0 && 
        access(on_disk_index_path, R_OK)==0 
        ){
        // file exists, readable. try to load index 
        idx_anchor->on_disk_file_stream = fopen(on_disk_index_path, "r");
        idx_anchor->is_readonly_index_file=1;
        fseek(idx_anchor->on_disk_file_stream, 0, SEEK_SET);
        stopwatch_t disk_indexing_time;
        timer_start(&disk_indexing_time);
        size_t count = 0;
        while (1) {
            index_record_t *ir = read_index_record(idx_anchor->on_disk_file_stream);
            if (ir == NULL) {
                break;
            }
            // convert to required parameters from IR.
            h5attribute_t attr;
            convert_index_record_to_in_mem_parameters(idx_anchor, &attr, ir);
            //insert into in-mem index.
            on_attr((void *)idx_anchor, &attr);
            count++;
        }
        fclose(idx_anchor->on_disk_file_stream);

        timer_pause(&disk_indexing_time);
        println("[LOAD_INDEX_FROM_MIQS_FILE] Time for loading %ld index records and get %ld kv-pairs was %ld s, %ld s on in-memory.", 
        count, idx_anchor->total_num_kv_pairs, timer_delta_s(&disk_indexing_time),
        idx_anchor->us_to_index);

    } else {
        // build index from HDF5 files
        idx_anchor->on_disk_file_stream = fopen(on_disk_index_path, "w");
        idx_anchor->is_readonly_index_file=0;
        // fseek(idx_anchor->on_disk_file_stream, 0, SEEK_END);
        stopwatch_t hdf5_indexing_time;
        timer_start(&hdf5_indexing_time);
        int count = 0;
        if (is_regular_file(path)) {
            parse_hdf5_file((char *)path);
            rst = 0;
        } else {
            rst = parse_files_in_dir((char *)path, topk);
        }  
        fclose(idx_anchor->on_disk_file_stream);
        timer_pause(&hdf5_indexing_time);
        println("[LOAD_INDEX_FROM_HDF5_FILE] Time for loading index from %ld HDF5 files with %ld objects and %ld attributes and %ld kv-pairs was %ld s, %ld s on in-memory, %ld s on on-disk.", 
        idx_anchor->total_num_files,
        idx_anchor->total_num_objects,
        idx_anchor->total_num_attrs,
        idx_anchor->total_num_kv_pairs,
        timer_delta_s(&hdf5_indexing_time),
        idx_anchor->us_to_index/1000000,
        idx_anchor->us_to_disk_index/1000000
        );
    }

    print_mem_usage("MEMALL");

    int num_queries = 16;
    if (num_indexed_field > 0) {
        num_queries = num_indexed_field;
    }

    stopwatch_t timer_search;
    timer_start(&timer_search);
    int numrst = 0;
    int i = 0;
    for (i = 0; i < 1024; i++) {
        int c = i % 16;
        if (search_types[c]==1) {
            int value = atoi(search_values[c]);
            search_result_t **rst = NULL;
            numrst += int_value_search(indexed_attr[c], value, &rst);
        }else if (search_types[c]==2) {
            double value = atof(search_values[c]);
            search_result_t **rst = NULL;
            numrst += float_value_search(indexed_attr[c], value, &rst); 
        } else {
            char *value = search_values[c];
            search_result_t **rst = NULL;
            numrst += string_value_search(indexed_attr[c], value, &rst);
        }
    }
    timer_pause(&timer_search);
    println("[META_SEARCH_MEMO] Time for 1024 queries on %d indexes and spent %d microseconds.", num_indexed_field, timer_delta_us(&timer_search));

    return rst;
}

