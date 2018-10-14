#include "../lib/hdf52index.h"
#include "../lib/fs_ops.h"
#include "../lib/string_utils.h"
#include "../lib/timer_utils.h"


void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/dir\n");
}


void perform_search(int seed, void *opdata_p){
    op_data_struct_t *opdata = (op_data_struct_t *)opdata_p;
    int pos = seed % opdata->num_indexed_attr;
    char *attr = opdata->indexed_attr[pos];
    art_leaf_content_t *art_leaf = (art_leaf_content_t *)art_search(global_art, attr, strlen(attr));
    if (art_leaf != NULL) {
        if (art_leaf->is_numeric) {
            int v_query = atoi(opdata->search_values[pos]) + seed;
            uint64_t *bitmap = bplus_tree_get(art_leaf->bpt, v_query);
            int num_matched_files = get_num_ones_in_bitmap(bitmap, opdata->bitmap_int64_arr_len);
            println("%d Matched files on query %s=%d.\n", num_matched_files, attr, v_query);
        } else {
            char tmp_v_query[100];
            sprintf(tmp_v_query, "%d%s", seed, opdata->search_values[pos]);
            char *v_query = tmp_v_query;
            uint64_t *bitmap = art_search(art_leaf->art, v_query, strlen(v_query));
            int num_matched_files = get_num_ones_in_bitmap(bitmap, opdata->bitmap_int64_arr_len);
            println("%d Matched files on query %s=%s.\n", num_matched_files, attr, v_query);
        }
    }
    return;
}


int parse_single_file(char *filepath) {

    stopwatch_t one_file;
    stopwatch_t parse_file;
    stopwatch_t import_one_doc;

    timer_start(&one_file);
    timer_start(&parse_file);
    
    parse_hdf5_file(filepath, &rootObj);

    timer_pause(&parse_file);
    
    timer_start(&import_one_doc);
    
    
    timer_pause(&import_one_doc);
    timer_pause(&one_file);
    suseconds_t one_file_duration = timer_delta_us(&one_file);
    suseconds_t parse_file_duration = timer_delta_us(&parse_file);
    suseconds_t import_one_doc_duration = timer_delta_us(&import_one_doc);
    println("[IMPORT_META] Finished in %ld us for %s, with %ld us for parsing and %ld us for inserting.",
        one_file_duration, basename(filepath), parse_file_duration, import_one_doc_duration);
    
    return 0;
}


int 
main(int argc, char const *argv[])
{
    if (argc < 2) {
        print_usage();
    }
    
    char *file_path = argv[1];

    char *indexed_attr[]={"COLLA", "DARKTIME", "BADPIXEL", "FILENAME", "EXPOSURE", "COLLB", NULL};
    char *search_values[]={"27089", "0", "badpixels-56149-b1.fits.gz", "sdR-b2-00154990.fit", "155701", "26660", NULL};
    int search_types[] = {1,1,0,0,1,1,0};
    

    for (i = 0; i < 1000; i++) {

        stopwatch_t timer_search;
        timer_start(&timer_search);

        perform_search(i, &opdata);

        timer_pause(&timer_search);

        println("Time to %d search is %d microseconds.\n", i, timer_delta_us(&timer_search));
    }

    return 0;
}

