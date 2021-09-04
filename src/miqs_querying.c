#include "miqs_querying.h"
#include "utils/cbase/base_stdlib.h"
#include "pthread.h"
#include "index/libhl/linklist.h"
#include "h52index/hdf52index.h"
#include "utils/fs/fs_ops.h"
#include "utils/string_utils.h"
#include "utils/timer_utils.h"

/** new impl with linklist **/
int collect_result_from_list(void *item, size_t idx, void *user){
    power_search_rst_t *rst = (power_search_rst_t *)user;
    file_obj_pair_t *fo_pair = (file_obj_pair_t *)item;
    size_t parallel_index_pos = fo_pair->parallel_index_pos;
    char *file_path = list_pick_tagged_value(root_idx_anchor()->file_paths_list[parallel_index_pos], fo_pair->file_list_pos)->tag;
    char *obj_path = list_pick_tagged_value(root_idx_anchor()->object_paths_list[parallel_index_pos], fo_pair->obj_list_pos)->tag;
    search_rst_entry_t *entry = (search_rst_entry_t *)calloc(1, sizeof(search_rst_entry_t));
    entry->file_path = file_path;
    entry->obj_path = obj_path;
    rst->rst_arr[idx] = *entry;
    return 1;
}


value_tree_leaf_content_t *attribute_value_search(char *attr_name, void *value_p, size_t value_size, miqs_attr_value_type_t attr_value_type){
    value_tree_leaf_content_t *rst_node = NULL;
    index_anchor_t *idx_anchor = root_idx_anchor();

    unsigned long attr_name_hval = djb2_hash((unsigned char *)attr_name) % idx_anchor->parallelism;

    if (idx_anchor== NULL) {
        return rst_node;
    }

    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art_array[attr_name_hval], 
    (const unsigned char *)attr_name, strlen(attr_name));

    if (leaf_cnt == NULL || leaf_cnt->rbt == NULL) {
        return rst_node;
    }

    if (attr_value_type == MIQS_ATV_STRING) {
        rst_node = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, (const unsigned char *)value_p, strlen(value_p));
    } else if (attr_value_type == MIQS_ATV_INTEGER || attr_value_type == MIQS_ATV_FLOAT) {
        void *entry;
        int rbt_found = rbt_find(leaf_cnt->rbt, value_p, value_size, &entry);
        if (rbt_found!=0){//Not found
            return rst_node;
        } else {
            rst_node = (value_tree_leaf_content_t *)entry;
        }
    }
    return rst_node;
}


power_search_rst_t *exact_metadata_search(char *attr_name, void *attribute_value, miqs_attr_value_type_t attr_value_type){
    index_anchor_t *idx_anchor = root_idx_anchor();
    power_search_rst_t *rst = NULL;
    value_tree_leaf_content_t *rst_node = NULL;
    unsigned long attr_name_hval = djb2_hash((unsigned char *)attr_name) % idx_anchor->parallelism;
    pthread_rwlock_rdlock(&(idx_anchor->GLOBAL_INDEX_LOCK[attr_name_hval]));

    if (attr_value_type == MIQS_ATV_INTEGER) {
        int *value = (int *)attribute_value;
        rst_node = attribute_value_search(attr_name, value, sizeof(int), attr_value_type);
    } else if (attr_value_type == MIQS_ATV_FLOAT) {
        double *value = (double *)attribute_value;
        rst_node = attribute_value_search(attr_name, value, sizeof(double), attr_value_type);
    } else {
        char *value = (char *)attribute_value;
        rst_node = attribute_value_search(attr_name, value, sizeof(char *), attr_value_type);
    }

    // Prepare result
    rst = calloc(1, sizeof(power_search_rst_t));
    rst->size = list_count(rst_node->file_obj_pair_list);
    rst->rst_arr = calloc(rst->size, sizeof(search_rst_entry_t));
    list_foreach_value(rst_node->file_obj_pair_list, collect_result_from_list, rst);

    pthread_rwlock_unlock(&(idx_anchor->GLOBAL_INDEX_LOCK[attr_name_hval]));
    return rst;
}


/**
 * ----------------------------------------------------------------------
 * The code below implemented on-disk search is currently ignored. 
 * ----------------------------------------------------------------------
 */

// search on disk
// timer_start(&timer_search);
// numrst = 0;
// i = 0;
// // open file
// FILE *idx_to_search = fopen(on_disk_index_path, "r");
// for (i = 0; i < 1024; i++) {
    
//     int c = i % 16;
//     if (search_types[c]==1) {
//         int value = atoi(search_values[c]);
//         int out_len = 0;
//         index_record_t **query_rst = 
//         find_index_record(indexed_attr[c], int_equals, &value, idx_to_search, &out_len);
//         numrst += out_len;
//     }else if (search_types[c]==2) {
//         double value = atof(search_values[c]);
//         int out_len = 0;
//         index_record_t **query_rst = 
//         find_index_record(indexed_attr[c], double_equals, &value, idx_to_search, &out_len);
//         numrst += out_len;
//     } else {
//         char *value = search_values[c];
//         int out_len = 0;
//         index_record_t **query_rst = 
//         find_index_record(indexed_attr[c], string_equals, &value, idx_to_search, &out_len);
//         numrst += out_len;
//     }
// }
// fclose(idx_to_search);
// timer_pause(&timer_search);
// println("[META_SEARCH_DISK] Time for 1024 queries on %d indexes and spent %d microseconds.  %d", 
// num_indexed_field, timer_delta_us(&timer_search), numrst);

/**
 * ----------------------------------------------------------------------
 * The code above implemented on-disk search is currently ignored. 
 * ----------------------------------------------------------------------


/**
 * 1. name="PID*"/"*ID*"/"*.gz"
 *      func(char *attr_name, char *value_prefix)
 *      func("name", "PID")
 * "miqs.txt" "txt.sqim" "miqs.txt" "iqs.txt", "qs.txt", "s.txt"... "t"
 *  func(char *attr_name, char *value_infix)
 * func(char *attr_name, char *value_suffix)
 * func("name", ".gz")
 * 
 * 2. batch_num=5-7 <NULL=infinity>
 * func(char *attr_name, void *left_bound, void *right_bound)
 * [8,9)
 * func(char *attr_name, char *range_exp)
 * 
 * 3. "n*"/"*e"/"*m*"="PID*"/"*ID*"/"*.gz"/5-7
 * 
 */