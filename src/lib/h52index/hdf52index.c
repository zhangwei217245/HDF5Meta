#include "hdf52index.h"
#include <libgen.h>
#include "../utils/string_utils.h"



int is_specified_field(char *name, index_anchor *idx_anchor) {
    int rst = 0;
    if (idx_anchor->num_indexed_field > 0) {
        int f = 0;
        for (f = 0; f < idx_anchor->num_indexed_field; f++) {
            if (strcmp(name, idx_anchor->indexed_attr[f]) == 0) {
                rst = 1;
                break;
            }
        } 
    }
    return rst;
}


void create_in_mem_index_for_attr(index_anchor *idx_anchor, miqs_meta_attribute_t *attr){
    art_tree *global_art = idx_anchor->root_art;
    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor->obj_path;
    int into_art = 1;
    stopwatch_t one_attr;   
    timer_start(&one_attr);
    attr_tree_leaf_content_t *leaf_cnt = (attr_tree_leaf_content_t *)art_search(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name));
    if (leaf_cnt == NULL){
        leaf_cnt = (attr_tree_leaf_content_t *)ctr_calloc(1, sizeof(attr_tree_leaf_content_t), get_index_size_ptr());
        // void *bptr = NULL;
        // leaf_cnt->bpt = (void ***)ctr_calloc(1, sizeof(void **), get_index_size_ptr());
        // (leaf_cnt->bpt)[0] = (void **)ctr_calloc(1, sizeof(void *), get_index_size_ptr());
        // (leaf_cnt->bpt)[0][0] = NULL;
        switch((H5T_class_t)(attr->attr_type)) {
            case H5T_INTEGER:
                leaf_cnt->rbt = rbt_create(libhl_cmp_keys_int, free);
                leaf_cnt->is_float=0;
                leaf_cnt->is_numeric = 1;
                break;
            case H5T_FLOAT:
                leaf_cnt->is_float=1;
                leaf_cnt->is_numeric = 1;
                leaf_cnt->rbt = rbt_create(libhl_cmp_keys_double, free);
                break;
            case H5T_STRING:
                leaf_cnt->is_float=0;
                leaf_cnt->is_numeric = 0;
                leaf_cnt->art = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
            default:
                into_art = 0;
                break;
        }
        if (into_art == 1) {
            art_insert(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name), leaf_cnt);
        }
    }

    switch((H5T_class_t)(attr->attr_type)) {
        case H5T_INTEGER:
            indexing_numeric(attr->attr_name,(int *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case H5T_FLOAT:
            indexing_numeric(attr->attr_name, (double *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case H5T_STRING:
            indexing_string(attr->attr_name, (char **)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        default:
            // printf("Ignore unsupported attr_type for attribute %s\n", name);
            // into_art = 0;
            break;
    }
    timer_pause(&one_attr);
    suseconds_t one_attr_duration = timer_delta_us(&one_attr);
    idx_anchor->us_to_index += one_attr_duration;
}



void convert_index_record_to_in_mem_parameters(index_anchor *idx_anchor, miqs_meta_attribute_t *attr, index_record_t *ir){
    attr->attr_name = ir->name;
    switch(ir->type) {
        case 1:
            attr->attr_type = (void *)H5T_INTEGER;
            attr->attribute_value = ir->data;
            attr->attribute_value_length = 1;
            break;
        case 2:
            attr->attr_type = (void *)H5T_FLOAT;
            attr->attribute_value = ir->data;
            attr->attribute_value_length = 1;
            break;
        case 3:
            attr->attr_type = (void *)H5T_STRING;
            char *str_val = (char *)ir->data;
            attr->attribute_value = (void *)&str_val;
            attr->attribute_value_length = 1;
            break;
        default:
            // printf("Ignore unsupported attr_type for attribute %s\n", name);
            break;
    }
    idx_anchor->file_path=ir->file_path;
    idx_anchor->obj_path=ir->object_path;
}


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
    // char **indexed_attr = idx_anchor->indexed_attr;
    // int num_indexed_field = idx_anchor->num_indexed_field;

    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor->obj_path;
    hid_t obj_id = (hid_t)idx_anchor->object_id;
    char *name = attr->attr_name;
    int ir_type = 1;

    idx_anchor->total_num_attrs+=1;
    
    // Create in-memory index according to specified field list.
    int build_index = 0;
    if (idx_anchor->num_indexed_field > 0) {
        if (is_specified_field(name, idx_anchor) == 1){
            build_index = 1;
        }
    } else {
        build_index = 1;
    }

    if (build_index == 1) {
        create_in_mem_index_for_attr(idx_anchor, attr);
        idx_anchor->total_num_indexed_kv_pairs+=attr->attribute_value_length;

        // Create on-disk AOF index, if the index file is not read only.
        if (idx_anchor->on_disk_file_stream!= NULL && idx_anchor->is_readonly_index_file==0) {
            stopwatch_t one_disk_attr;   
            timer_start(&one_disk_attr);

            if ((H5T_class_t)(attr->attr_type) == H5T_INTEGER) {
                int i = 0;
                for (i = 0; i < attr->attribute_value_length; i++) {
                    int *value_arr = (int *)attr->attribute_value;
                    index_record_t *ir =
                    create_index_record(1, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
                    append_index_record(ir, idx_anchor->on_disk_file_stream);
                }
            } else if ((H5T_class_t)(attr->attr_type) == H5T_FLOAT) {
                int i = 0;
                for (i = 0; i < attr->attribute_value_length; i++) {
                    double *value_arr = (double *)attr->attribute_value;
                    index_record_t *ir =
                    create_index_record(2, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
                    append_index_record(ir, idx_anchor->on_disk_file_stream);
                }
            } else if ((H5T_class_t)(attr->attr_type) == H5T_STRING) {
                int i = 0;
                for (i = 0; i < attr->attribute_value_length; i++) {
                    char  **value_arr = (char  **)attr->attribute_value;
                    index_record_t *ir =
                    create_index_record(3, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
                    append_index_record(ir, idx_anchor->on_disk_file_stream);
                }
            }
            suseconds_t one_disk_attr_duration = timer_delta_us(&one_disk_attr);
            idx_anchor->us_to_disk_index += one_disk_attr_duration;
        }
        idx_anchor->total_num_kv_pairs+=attr->attribute_value_length;
    }
    //TODO: currently ignore any error.
    return 1;
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

int load_mdb(char *filepath, index_file_loading_param_t *param) {
    return load_mdb_file_to_index(filepath, param->idx_anchor);
}

int load_aof(char *filepath, index_file_loading_param_t *param){
    size_t count = 0;
    index_anchor *idx_anchor = (index_anchor *)param->idx_anchor;
    if (access(filepath, F_OK)==0 && 
        access(filepath, R_OK)==0 
        ){
        size_t fsize = get_file_size(filepath);
        if (fsize > 0) {
            // file exists, readable. try to load index 
            idx_anchor->on_disk_file_stream = fopen(filepath, "r");
            idx_anchor->is_readonly_index_file=1;
            fseek(idx_anchor->on_disk_file_stream, 0, SEEK_SET);    
            while (1) {
                index_record_t *ir = read_index_record(idx_anchor->on_disk_file_stream);
                if (ir == NULL) {
                    break;
                }
                // convert to required parameters from IR.
                miqs_meta_attribute_t attr;
                convert_index_record_to_in_mem_parameters(idx_anchor, &attr, ir);
                //insert into in-mem index.
                on_attr((void *)idx_anchor, &attr);
                count++;
            }
            fclose(idx_anchor->on_disk_file_stream);
        }
    }
    return count;
}