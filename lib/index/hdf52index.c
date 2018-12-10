#include "hdf52index.h"
#include <libgen.h>
#include "../utils/string_utils.h"


int on_obj(void *opdata, h5object_t *obj){
    index_anchor *idx_anchor = (index_anchor *)opdata;
    idx_anchor->obj_path = (char *)ctr_calloc(strlen(obj->obj_name)+1, sizeof(char), get_index_size_ptr());
    strncpy(idx_anchor->obj_path, obj->obj_name, strlen(obj->obj_name));
    idx_anchor->object_id = obj->obj_id;
    return 1;
}

int on_attr(void *opdata, h5attribute_t *attr){
    
    index_anchor *idx_anchor = (index_anchor *)opdata;
    char **indexed_attr = idx_anchor->indexed_attr;
    int num_indexed_field = idx_anchor->num_indexed_field;

    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor -> obj_path;
    hid_t obj_id = idx_anchor->object_id;
    char *name = attr->attr_name;


    if (num_indexed_field > 0) {
        int f = 0;
        int has_specified_field = 0;
        for (f = 0; f < num_indexed_field; f++) {
            if (strcmp(name, indexed_attr[f]) == 0) {
                has_specified_field = 1;
                break;
            }
        } 
        if (has_specified_field == 0) {
            return 1;
        }
    }
    
    art_tree *global_art = idx_anchor->root_art;

    stopwatch_t one_attr;   
    timer_start(&one_attr);
    attr_tree_leaf_content_t *leaf_cnt = (attr_tree_leaf_content_t *)art_search(global_art, name, strlen(name));
    if (leaf_cnt == NULL){
        leaf_cnt = (attr_tree_leaf_content_t *)ctr_calloc(1, sizeof(attr_tree_leaf_content_t), get_index_size_ptr());
        // void *bptr = NULL;
        leaf_cnt->bpt = (void ***)ctr_calloc(1, sizeof(void **), get_index_size_ptr());
        (leaf_cnt->bpt)[0] = (void **)ctr_calloc(1, sizeof(void *), get_index_size_ptr());
        (leaf_cnt->bpt)[0][0] = NULL;
        leaf_cnt->art = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
        art_insert(global_art, name, strlen(name), leaf_cnt);
    }

    switch(attr->attr_type) {
        case H5T_INTEGER:
            indexing_numeric(attr->attr_name,(int *)attr->attribute_value, attr->attribute_value_length, 
            int_value_compare_func, file_path, obj_id, leaf_cnt);
            break;
        case H5T_FLOAT:
            indexing_numeric(attr->attr_name, (double *)attr->attribute_value, attr->attribute_value_length,
            float_value_compare_func, file_path, obj_id, leaf_cnt);
            break;
        case H5T_STRING:
            indexing_string(attr->attr_name, (char **)attr->attribute_value, attr->attribute_value_length, 
            file_path, obj_id, leaf_cnt);
            break;
        default:
            // printf("Ignore unsupported attr_type for attribute %s\n", name);
            break;
    }
    timer_pause(&one_attr);
    suseconds_t one_attr_duration = timer_delta_us(&one_attr);
    idx_anchor->us_to_index += one_attr_duration;
    
    //TODO: currently ignore any error.
    return 1;
}


void parse_hdf5_file(char *filepath, index_anchor *idx_anchor){
    
    if (idx_anchor == NULL) {
        return;
    }

    char *file_path = (char *)ctr_calloc(strlen(filepath)+1, sizeof(char), get_index_size_ptr());
    strncpy(file_path, filepath, strlen(filepath));

    if (idx_anchor->root_art == NULL) {
        idx_anchor->root_art = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
    }
    
    idx_anchor->file_path = file_path;
    idx_anchor->us_to_index = 0;
    
    metadata_collector_t *meta_collector = (metadata_collector_t *)ctr_calloc(1, sizeof(metadata_collector_t), get_index_size_ptr());
    // init_metadata_collector(meta_collector, 0, (void *)index_anchor, NULL, on_obj, on_attr);
    init_metadata_collector(meta_collector, 0, (void *)idx_anchor, NULL, on_obj, on_attr);

    stopwatch_t time_to_scan;
    timer_start(&time_to_scan);
    scan_hdf5(filepath, meta_collector, 0);
    timer_pause(&time_to_scan);
    suseconds_t scan_and_index_duration = timer_delta_us(&time_to_scan);
    suseconds_t actual_indexing = idx_anchor->us_to_index;
    suseconds_t actual_scanning = scan_and_index_duration - actual_indexing;
    
    println("[IMPORT_META] Finished in %ld us for %s, with %ld us for scanning and %ld us for indexing.",
        scan_and_index_duration, basename(filepath), actual_scanning, actual_indexing);
}

/*
 * Measures the current (and peak) resident and virtual memories
 * usage of your linux C process, in kB
 */
void getMemory(
    int* currRealMem, int* peakRealMem,
    int* currVirtMem, int* peakVirtMem) {

    // stores each word in status file
    char buffer[1024] = "";

    // linux file contains this-process info
    FILE* file = fopen("/proc/self/status", "r");

    // read the entire file
    while (fscanf(file, " %1023s", buffer) == 1) {

        if (strcmp(buffer, "VmRSS:") == 0) {
            fscanf(file, " %d", currRealMem);
        }
        if (strcmp(buffer, "VmHWM:") == 0) {
            fscanf(file, " %d", peakRealMem);
        }
        if (strcmp(buffer, "VmSize:") == 0) {
            fscanf(file, " %d", currVirtMem);
        }
        if (strcmp(buffer, "VmPeak:") == 0) {
            fscanf(file, " %d", peakVirtMem);
        }
    }
    fclose(file);
}


void print_mem_usage(){
    int VmRSS;
    int VmHWM;
    int VmSize;
    int VmPeak;
    getMemory(&VmRSS, &VmHWM, &VmSize, &VmPeak);
    // printf("VmRSS=%d, VmHWM=%d, VmSize=%d, VmPeak=%d\n", VmRSS, VmHWM, VmSize, VmPeak);

    size_t art_size = get_art_mem_size();
    // size_t btree_size = get_btree_mem_size();
    size_t btree_size = 0;
    size_t overall_index_size = get_index_size() + btree_size + art_size;
    size_t metadata_size = get_hdf5_meta_size() + overall_index_size;
    println("VmRSS=%d, VmHWM=%d, VmSize=%d, VmPeak=%d, dataSize = %d, indexSize = %d", VmRSS, VmHWM, VmSize, VmPeak, metadata_size, overall_index_size);
}