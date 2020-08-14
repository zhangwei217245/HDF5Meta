#include "hdf52index.h"
#include <libgen.h>
#include "../utils/string_utils.h"


int on_obj(void *opdata, miqs_data_object_t *obj){
    
    index_anchor *idx_anchor = (index_anchor *)opdata;
    idx_anchor->obj_path = (char *)ctr_calloc(strlen(obj->obj_name)+1, sizeof(char), get_index_size_ptr());
    strncpy(idx_anchor->obj_path, obj->obj_name, strlen(obj->obj_name));
    idx_anchor->object_id = (void *)obj->obj_id;
    idx_anchor->total_num_objects+=1;
    return 1;
}

int on_attr(void *opdata, miqs_meta_attribute_t *attr){
    
    index_anchor *idx_anchor = (index_anchor *)opdata;
    return indexing_attr(idx_anchor, attr);
}


void parse_hdf5_file(char *filepath){

    index_anchor *idx_anchor = root_idx_anchor();

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

    idx_anchor->total_num_files+=1;
    
    miqs_metadata_collector_t *meta_collector = (miqs_metadata_collector_t *)ctr_calloc(1, sizeof(miqs_metadata_collector_t), get_index_size_ptr());
    
    init_metadata_collector(meta_collector, 0, (void *)root_idx_anchor(), NULL, on_obj, on_attr);

    stopwatch_t time_to_scan;
    timer_start(&time_to_scan);
    scan_hdf5(filepath, meta_collector, 0);
    timer_pause(&time_to_scan);
    suseconds_t scan_and_index_duration = timer_delta_us(&time_to_scan);
    suseconds_t actual_indexing = idx_anchor->us_to_index;
    suseconds_t actual_persist_index = idx_anchor->us_to_disk_index;
    suseconds_t actual_scanning = scan_and_index_duration - actual_indexing;

    mem_cost_t *mem_cost = get_mem_cost();
    
    println("[IMPORT_META] Finished in %ld us for %s, with %ld us for scanning and %ld us for indexing, %ld for on-disk indexing. [MEM] dataSize: %ld , indexSize: %ld . num_obj: %ld , num_attrs: %ld , totalkv: %ld",
        scan_and_index_duration, basename(filepath), actual_scanning, actual_indexing, actual_persist_index,
        mem_cost->metadata_size, mem_cost->overall_index_size,
        idx_anchor->total_num_objects, idx_anchor->total_num_attrs, idx_anchor->total_num_kv_pairs);
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


mem_cost_t *get_mem_cost(){
    mem_cost_t *rst = (mem_cost_t *)calloc(1, sizeof(mem_cost_t));
    size_t art_size = get_mem_usage_by_all_arts();
    // size_t btree_size = get_btree_mem_size();
    size_t btree_size = get_mem_usage_by_all_rbtrees();
    size_t linklist_size = get_mem_usage_by_all_linkedlist();
    size_t overall_index_size = get_index_size() + btree_size + art_size + linklist_size;
    size_t metadata_size = get_hdf5_meta_size() + overall_index_size;
    rst->metadata_size = metadata_size;
    rst->overall_index_size = overall_index_size;
    return rst;
}

void print_mem_usage(char *prefix){
    // int VmRSS;
    // int VmHWM;
    // int VmSize;
    // int VmPeak;
    // getMemory(&VmRSS, &VmHWM, &VmSize, &VmPeak);
    // printf("VmRSS=%d, VmHWM=%d, VmSize=%d, VmPeak=%d\n", VmRSS, VmHWM, VmSize, VmPeak);
    mem_cost_t *rst = get_mem_cost();
    printf("[MEM_CONSUMPTION_%s] ", prefix);
    println("dataSize = %d, indexSize = %d", rst->metadata_size, rst->overall_index_size);
}
