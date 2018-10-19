#include "hdf52index.h"
#include <libgen.h>

/**
 * Index structure:
 * 1. ART for attr_name
 * 2. BPT for int value
 * 3. path_bpt_leaf_cnt for 
 *  a. file_path_art
 *  b. obj_path_art
 * Using file_path_art/obj_path_art can save a lot more space if multiple 
 * k-v pairs can be mapped to one file/one object. Duplicated values will 
 * not take extra space. With art_iter, we can retrieve all unique 
 * file_paths/obj_paths. 
 */
void indexing_int(char *attr_name, int *attr_val, int attribute_value_length, char *file_path, char *obj_path, art_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 1;
    leaf_cnt->is_float = 0;
    if (leaf_cnt->bpt == NULL) {
        leaf_cnt->bpt = bplus_tree_init(attr_name, 4096);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        int k = attr_val[i];
        path_bpt_leaf_cnt_t *v = (path_bpt_leaf_cnt_t *)bplus_tree_get(leaf_cnt->bpt, k);
        if (v == NULL || (long)v == 0 || (long)v == 0xffffffffffffffff) {
            v = (path_bpt_leaf_cnt_t *)calloc(1, sizeof(path_bpt_leaf_cnt_t));
            v->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            v->obj_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(v->file_path_art);
            art_tree_init(v->obj_path_art);
            bplus_tree_put(leaf_cnt->bpt, k, (long)v);
        }
        // TODO: we store value as attr_name currently, 
        // but we can utilize this value to store some statistic info, 
        // for caching policy maybe.
        art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
        art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
    }
}

void indexing_float(char *attr_name, double *attr_val, int attribute_value_length, char *file_path, char *obj_path, art_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 1;
    leaf_cnt->is_float = 1;
    if (leaf_cnt->bpt == NULL) {
        leaf_cnt->bpt = bplus_tree_init(attr_name, 4096);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        double k = attr_val[i];
        path_bpt_leaf_cnt_t *v = (path_bpt_leaf_cnt_t *)bplus_tree_get(leaf_cnt->bpt, k);
        if (v == NULL || (long)v == 0 || (long)v == 0xffffffffffffffff) {
            v = (path_bpt_leaf_cnt_t *)calloc(1, sizeof(path_bpt_leaf_cnt_t));
            v->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            v->obj_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(v->file_path_art);
            art_tree_init(v->obj_path_art);
            bplus_tree_put(leaf_cnt->bpt, (int)k, (long)v);
        }
        // TODO: we store value as attr_name currently, 
        // but we can utilize this value to store some statistic info, 
        // for caching policy maybe.
        art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
        art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
    }
}

void indexing_string(char *attr_name, char **attr_val, int attribute_value_length, char *file_path, char *obj_path, art_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 0;
    leaf_cnt->is_float = 0;
    if (leaf_cnt->art == NULL) {
        leaf_cnt->art = (art_tree *)calloc(1, sizeof(art_tree));
        art_tree_init(leaf_cnt->art);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        char *k = attr_val[i];
        path_bpt_leaf_cnt_t *v = (path_bpt_leaf_cnt_t *)art_search(leaf_cnt->art, k, strlen(k));
        if (v == NULL || (long)v == 0 || (long)v == 0xffffffffffffffff) {
            v = (path_bpt_leaf_cnt_t *)calloc(1, sizeof(path_bpt_leaf_cnt_t));
            v->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            v->obj_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(v->file_path_art);
            art_tree_init(v->obj_path_art);
            art_insert(leaf_cnt->art, (unsigned char *)k, strlen(k), (void *)v);
        }
        // TODO: we store value as attr_name currently, 
        // but we can utilize this value to store some statistic info, 
        // for caching policy maybe.
        art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
        art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
    }
}


int on_obj(void *opdata,h5object_t *obj){
    index_anchor *idx_anchor = (index_anchor *)opdata;
    idx_anchor->obj_path = (char *)calloc(strlen(obj->obj_name)+1, sizeof(char));
    strncpy(idx_anchor->obj_path, obj->obj_name, strlen(obj->obj_name));
}

int on_attr(void *opdata, h5attribute_t *attr){
    
    index_anchor *idx_anchor = (index_anchor *)opdata;
    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor -> obj_path;
    char *name = attr->attr_name;
    art_tree *global_art = idx_anchor->root_art;


    stopwatch_t one_attr;   
    timer_start(&one_attr);
    art_leaf_content_t *leaf_cnt = (art_leaf_content_t *)art_search(global_art, name, strlen(name));
    if (leaf_cnt == NULL){
        leaf_cnt = (art_leaf_content_t *)calloc(1, sizeof(art_leaf_content_t));
        art_insert(global_art, name, strlen(name), leaf_cnt);
    }

    switch(attr->attr_type) {
        case H5T_INTEGER:
            indexing_int(attr->attr_name,(int *)attr->attribute_value, attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case H5T_FLOAT:
            indexing_float(attr->attr_name, (double *)attr->attribute_value, attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case H5T_STRING:
            indexing_string(attr->attr_name, (char **)attr->attribute_value, attr->attribute_value_length, file_path, obj_path, leaf_cnt);
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

void parse_hdf5_file(char *filepath, art_tree *artree){
    
    if (artree == NULL) {
        return;
    }

    char *file_path = (char *)calloc(strlen(filepath)+1, sizeof(char));
    strncpy(file_path, filepath, strlen(filepath));

    index_anchor *idx_anchor = (index_anchor *)calloc(1, sizeof(index_anchor));
    idx_anchor->file_path = file_path;
    idx_anchor->root_art = artree;
    idx_anchor->us_to_index = 0;
    
    metadata_collector_t *meta_collector = (metadata_collector_t *)calloc(1, sizeof(metadata_collector_t));
    // init_metadata_collector(meta_collector, 0, (void *)index_anchor, NULL, on_obj, on_attr);
    init_metadata_collector(meta_collector, 0, (void *)idx_anchor, NULL, on_obj, on_attr);

    stopwatch_t time_to_scan;
    timer_start(&time_to_scan);
    scan_hdf5(filepath, meta_collector, 0);
    timer_pause(&time_to_scan);
    suseconds_t scan_and_index_duration = timer_delta_us(&time_to_scan);
    suseconds_t actual_indexing = idx_anchor->us_to_index;
    suseconds_t actual_scanning = scan_and_index_duration - actual_indexing;
    println("[IMPORT_META] Finished in %ld us for %s, with %ld us for scanning and %ld us for inserting.",
        scan_and_index_duration, basename(filepath), actual_scanning, actual_indexing);
    

}