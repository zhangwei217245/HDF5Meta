#include "hdf52index.h"
#include <libgen.h>
#include "../utils/string_utils.h"


index_anchor *idx_anchor;
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
// void indexing_int(char *attr_name, int *attr_val, int attribute_value_length, char *file_path, char *obj_path, art_leaf_content_t *leaf_cnt){
//     leaf_cnt->is_numeric = 1;
//     leaf_cnt->is_float = 0;
//     if (leaf_cnt->bpt == NULL) {
//         leaf_cnt->bpt = bplus_tree_init(attr_name, 4096);
//     }
//     int i = 0;
//     for (i = 0; i < attribute_value_length; i++) {
//         int k = attr_val[i];
//         path_bpt_leaf_cnt_t *v = (path_bpt_leaf_cnt_t *)bplus_tree_get(leaf_cnt->bpt, k);
//         if (v == NULL || (long)v == 0 || (long)v == 0xffffffffffffffff) {
//             v = (path_bpt_leaf_cnt_t *)calloc(1, sizeof(path_bpt_leaf_cnt_t));
//             v->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
//             v->obj_path_art = (art_tree *)calloc(1, sizeof(art_tree));
//             art_tree_init(v->file_path_art);
//             art_tree_init(v->obj_path_art);
//             bplus_tree_put(leaf_cnt->bpt, k, (long)v);
//         }
//         // TODO: we store value as attr_name currently, 
//         // but we can utilize this value to store some statistic info, 
//         // for caching policy maybe.
//         art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
//         art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
//     }
// }

// void indexing_float(char *attr_name, double *attr_val, int attribute_value_length, char *file_path, char *obj_path, art_leaf_content_t *leaf_cnt){
//     leaf_cnt->is_numeric = 1;
//     leaf_cnt->is_float = 1;
//     if (leaf_cnt->bpt == NULL) {
//         leaf_cnt->bpt = bplus_tree_init(attr_name, 4096);
//     }
//     int i = 0;
//     for (i = 0; i < attribute_value_length; i++) {
//         double k = attr_val[i];
//         path_bpt_leaf_cnt_t *v = (path_bpt_leaf_cnt_t *)bplus_tree_get(leaf_cnt->bpt, k);
//         if (v == NULL || (long)v == 0 || (long)v == 0xffffffffffffffff) {
//             v = (path_bpt_leaf_cnt_t *)calloc(1, sizeof(path_bpt_leaf_cnt_t));
//             v->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
//             v->obj_path_art = (art_tree *)calloc(1, sizeof(art_tree));
//             art_tree_init(v->file_path_art);
//             art_tree_init(v->obj_path_art);
//             bplus_tree_put(leaf_cnt->bpt, (int)k, (long)v);
//         }
//         // TODO: we store value as attr_name currently, 
//         // but we can utilize this value to store some statistic info, 
//         // for caching policy maybe.
//         art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
//         art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
//     }
// }



int int_value_compare_func(const void *l, const void *r){
    const value_tree_leaf_content_t *el = (const value_tree_leaf_content_t *)l;
    const value_tree_leaf_content_t *er = (const value_tree_leaf_content_t *)r;
    const int *il = (const int *)el->k;
    const int *ir = (const int *)er->k;
    if ((*il) < (*ir)) {
        return -1;
    }
    if ((*il) > (*ir)) {
        return -1;
    }
    return 0;
}

int float_value_compare_func(const void *l, const void *r){
    const value_tree_leaf_content_t *el = (const value_tree_leaf_content_t *)l;
    const value_tree_leaf_content_t *er = (const value_tree_leaf_content_t *)r;
    const double *il = (const double *)el->k;
    const double *ir = (const double *)er->k;
    if ((*il) < (*ir)) {
        return -1;
    }
    if ((*il) > (*ir)) {
        return -1;
    }
    return 0;
}

void indexing_numeric(char *attr_name, void *attr_val, int attribute_value_length,
int (*compare_func)(const void *a, const void *b), 
char *file_path, hid_t obj_id, attr_tree_leaf_content_t *leaf_cnt){
    void *retval = 0;
    leaf_cnt->is_numeric = 1;
    leaf_cnt->is_float = (compare_func==float_value_compare_func);
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        value_tree_leaf_content_t *entry = (value_tree_leaf_content_t *)calloc(1, sizeof(value_tree_leaf_content_t));
        if (compare_func == int_value_compare_func) {
            int k = ((int *)attr_val)[i];
            entry->k = (int *)calloc(1,sizeof(int));
            *((int *)(entry->k)) = k;
        } else if (compare_func == float_value_compare_func){
            double k = ((double *)attr_val)[i];
            entry->k = (double *)calloc(1,sizeof(double));
            *((double *)(entry->k)) = k;
        }
        retval = tsearch(entry, leaf_cnt->bpt, compare_func);
        if (retval == 0) {
            println("Fail ENOMEM");
        } else {
            value_tree_leaf_content_t *test_ent = 0;
            test_ent = *(value_tree_leaf_content_t **)retval;
            if (test_ent != entry) {
                // value found, please append data here.
                free(entry->k);
                free(entry);
            } else {
                //value added.
                test_ent->file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
                art_tree_init(test_ent->file_path_art);
            }
            hashset_t obj_id_set = (hashset_t)art_search(test_ent->file_path_art, 
            (const unsigned char *)file_path, strlen(file_path));
            if (obj_id_set == NULL) {
                obj_id_set = hashset_create();
                art_insert(test_ent->file_path_art, file_path, strlen(file_path), (void *)obj_id_set);
            }
            hashset_add(obj_id_set, (void *)obj_id);
        }
        // TODO: we store value as attr_name currently, 
        // but we can utilize this value to store some statistic info, 
        // for caching policy maybe.
        // art_insert(v->file_path_art, file_path, strlen(file_path), attr_name);
        // art_insert(v->obj_path_art, obj_path, strlen(obj_path), attr_name);
    }
}

void indexing_string(char *attr_name, char **attr_val, int attribute_value_length, 
char *file_path, hid_t obj_id, attr_tree_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 0;
    leaf_cnt->is_float = 0;

    if (leaf_cnt->art == NULL) {
        leaf_cnt->art = (art_tree *)calloc(1, sizeof(art_tree));
        art_tree_init(leaf_cnt->art);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        char *k = attr_val[i];
        art_tree *file_path_art = (art_tree *)art_search(leaf_cnt->art, k, strlen(k));
        if (file_path_art == NULL) {
            file_path_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(file_path_art);
            art_insert(leaf_cnt->art, (unsigned char *)k, strlen(k), (void *)file_path_art);
        }
        hashset_t obj_id_set = (hashset_t)art_search(file_path_art, 
        (const unsigned char *)file_path, strlen(file_path));
        if (obj_id_set == NULL) {
            obj_id_set = hashset_create();
            art_insert(file_path_art, file_path, strlen(file_path), (void *)obj_id_set);
        }
        hashset_add(obj_id_set, (void *)obj_id);
        // TODO: we store value as attr_name currently, 
        // but we can utilize this value to store some statistic info, 
        // for caching policy maybe.
    }
}


int on_obj(void *opdata, h5object_t *obj){
    index_anchor *idx_anchor = (index_anchor *)opdata;
    idx_anchor->obj_path = (char *)calloc(strlen(obj->obj_name)+1, sizeof(char));
    strncpy(idx_anchor->obj_path, obj->obj_name, strlen(obj->obj_name));
    idx_anchor->object_id = obj->obj_id;
    return 1;
}

int on_attr(void *opdata, h5attribute_t *attr){
    
    index_anchor *idx_anchor = (index_anchor *)opdata;
    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor -> obj_path;
    hid_t obj_id = idx_anchor->object_id;
    char *name = attr->attr_name;
    art_tree *global_art = idx_anchor->root_art;

    stopwatch_t one_attr;   
    timer_start(&one_attr);
    attr_tree_leaf_content_t *leaf_cnt = (attr_tree_leaf_content_t *)art_search(global_art, name, strlen(name));
    if (leaf_cnt == NULL){
        leaf_cnt = (attr_tree_leaf_content_t *)calloc(1, sizeof(attr_tree_leaf_content_t));
        void *bptr = NULL;
        leaf_cnt->bpt = &bptr;
        leaf_cnt->art = (art_tree *)calloc(1, size);
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

int collect_result(void *data, const unsigned char *key, uint32_t key_len, void *value) {
    power_search_rst_t *prst = (power_search_rst_t *)data;
    search_result_t *rst = &(prst->rst_arr[prst->num_files]);
    rst->file_path = strdup(key);
    //TODO: get object_ids;

    hashset_t obj_id_set = (hashset_t)value;
    rst->num_objs = (int)hashset_num_items(obj_id_set);
    rst->obj_ids = (hid_t *)calloc(rst->num_objs, sizeof(hid_t));
    hashset_itr_t hs_itr = hashset_iterator(obj_id_set);
    hs_itr->index = 0;//FIXME: confirm.
    int i = 0;
    while (hashset_iterator_has_next(hs_itr)) {
        hid_t obj_id = (hid_t)hashset_iterator_value(hs_itr);
        rst->obj_ids[i] = obj_id;
        hashset_iterator_next(hs_itr);
        i++;
    }
    prst->num_files+=1;
}

/**
 * This is key-value exact search
 * 
 */ 
int int_value_search(index_anchor *idx_anchor, char *attr_name, int value, search_result_t **rst) {
    
    int numrst = -1;
    if (rst == NULL) {
        return numrst;
    }
    
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));

    value_tree_leaf_content_t *entry = (value_tree_leaf_content_t *)calloc(1, sizeof(value_tree_leaf_content_t));
    entry->k = (int *)calloc(1,sizeof(int));
    *((int *)(entry->k)) = value;
    
    value_tree_leaf_content_t *retval = tfind(entry, leaf_cnt->bpt, int_value_compare_func);
    if (retval == NULL) {
        return numrst;
    } else {
        power_search_rst_t *prst = (power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
        prst->num_files=0;
        prst->rst_arr = (search_result_t *)calloc(art_size(retval->file_path_art), sizeof(search_result_t));
        art_iter(retval->file_path_art, collect_result, prst);
        numrst = prst->num_files;
        search_result_t *_rst = prst->rst_arr;
        *rst = _rst;
        free(prst);
    }
    free(entry->k);
    free(entry);
    return numrst;
}

int float_value_search(index_anchor *idx_anchor, char *attr_name, double value, search_result_t **rst) {
    int numrst = -1;
    if (rst == NULL) {
        return numrst;
    }
    
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));

    value_tree_leaf_content_t *entry = (value_tree_leaf_content_t *)calloc(1, sizeof(value_tree_leaf_content_t));
    entry->k = (double *)calloc(1,sizeof(double));
    *((double *)(entry->k)) = value;
    
    value_tree_leaf_content_t *retval = tfind(entry, leaf_cnt->bpt, int_value_compare_func);
    if (retval == NULL) {
        return numrst;
    } else {
        power_search_rst_t *prst = (power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
        prst->num_files=0;
        prst->rst_arr = (search_result_t *)calloc(art_size(retval->file_path_art), sizeof(search_result_t));
        art_iter(retval->file_path_art, collect_result, prst);
        numrst = prst->num_files;
        search_result_t *_rst = prst->rst_arr;
        *rst = _rst;
        free(prst);
    }
    free(entry->k);
    free(entry);
    return numrst;
}

int string_value_search(index_anchor *idx_anchor, char *attr_name, char *value, search_result_t **rst) {
    int numrst = -1;
    if (rst == NULL) {
        return numrst;
    }
    
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));

    art_tree *file_path_art = (art_tree *)art_search(leaf_cnt->art, (const unsigned char *)value, strlen(value));

    if (file_path_art == NULL) {
        return numrst;
    } else {
        power_search_rst_t *prst = (power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
        prst->num_files=0;
        prst->rst_arr = (search_result_t *)calloc(art_size(file_path_art), sizeof(search_result_t));
        art_iter(file_path_art, collect_result, prst);
        numrst = prst->num_files;
        search_result_t *_rst = prst->rst_arr;
        *rst = _rst;
        free(prst);
    }
    return numrst;
}

void parse_hdf5_file(char *filepath, index_anchor *idx_anchor){
    
    if (idx_anchor == NULL) {
        return;
    }

    char *file_path = (char *)calloc(strlen(filepath)+1, sizeof(char));
    strncpy(file_path, filepath, strlen(filepath));

    if (idx_anchor->root_art == NULL) {
        idx_anchor->root_art = (art_tree *)calloc(1, sizeof(art_tree));
    }
    
    idx_anchor->file_path = file_path;
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
    println("[IMPORT_META] Finished in %ld us for %s, with %ld us for scanning and %ld us for indexing.",
        scan_and_index_duration, basename(filepath), actual_scanning, actual_indexing);
}