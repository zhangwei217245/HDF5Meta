#include "in_mem_index.h"


index_anchor *idx_anchor;

size_t index_mem_size;

index_anchor *root_idx_anchor(){
    return idx_anchor;
}

size_t *get_index_size_ptr(){
    return &index_mem_size;
}

size_t get_index_size(){
    return index_mem_size;
}

size_t insert_tagged_value(linked_list_t *list, char *tag){
    tagged_value_t *tgv = list_get_tagged_value(list, tag);
    if (tgv) {
        size_t *pos_ptr = (size_t *)tgv->value;
        return *pos_ptr;
    } else {
        size_t last_pos = list_count(list);
        tgv = list_create_tagged_value(tag, &last_pos, sizeof(size_t));
        list_insert_tagged_value(list, tgv,
            last_pos);
        return last_pos;
    }
}

/** new impl with linklist **/
int collect_result_from_list(void *item, size_t idx, void *user){
    power_search_rst_t *rst = (power_search_rst_t *)user;
    file_obj_pair_t *fo_pair = (file_obj_pair_t *)item;
    char *file_path = list_pick_tagged_value(root_idx_anchor()->file_paths_list, fo_pair->file_list_pos)->tag;
    char *obj_path = list_pick_tagged_value(root_idx_anchor()->object_paths_list, fo_pair->obj_list_pos)->tag;
    search_rst_entry_t *entry = (search_rst_entry_t *)calloc(1, sizeof(search_rst_entry_t));
    entry->file_path = file_path;
    entry->obj_path = obj_path;
    list_push_value(rst->rst_arr, (void *)entry);
}


int init_in_mem_index(){
    idx_anchor = (index_anchor *)ctr_calloc(1, sizeof(index_anchor), get_index_size_ptr());
    idx_anchor->root_art = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
    idx_anchor->file_path=NULL;
    idx_anchor->obj_path=NULL;
    idx_anchor->file_paths_list=list_create();
    idx_anchor->object_paths_list=list_create();
    idx_anchor->indexed_attr=NULL;
    idx_anchor->num_indexed_field=0;
    idx_anchor->on_disk_file_stream=NULL;
    idx_anchor->is_readonly_index_file=1;
    idx_anchor->total_num_files=0;
    idx_anchor->total_num_objects=0;
    idx_anchor->total_num_attrs=0;
    idx_anchor->total_num_indexed_kv_pairs=0;
    idx_anchor->total_num_kv_pairs=0;
    idx_anchor->us_to_index=0;
    idx_anchor->us_to_disk_index=0;
    return 1;
}

/**
 * This is a function for indexing numeric fields in the HDF5 metadata. 
 * We deal with two different data types, one is integer, the other one is double. 
 * 1. Go through attribute_name art_tree, and we get the pointer for value B-tree
 * 2. After transforming the value to int/double value, we go through value B-tree(tsearch)
 * 3. At the leaf nodes of B-tree, we go through the ART tree again to collect all file paths
 *    which contains the result. 
 */ 
void indexing_numeric(char *attr_name, void *attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt){
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        // A node with k as the value to compare and search.
        void *entry;
        size_t k_size;
        void *key;
        if (leaf_cnt->is_float==0) {
            k_size = sizeof(int);
            int *_attr_value = (int *)attr_val;
            key = &(_attr_value[i]);
        } else {
            k_size = sizeof(double);
            double *_attr_value = (double *)attr_val;
            key = &(_attr_value[i]);
        }
        int rbt_found = rbt_find(leaf_cnt->rbt, key, k_size, &entry);
        if (rbt_found != 0) {// not found
            entry = (value_tree_leaf_content_t *)ctr_calloc(1, sizeof(value_tree_leaf_content_t), &index_mem_size);
            ((value_tree_leaf_content_t *)entry)->file_obj_pair_list = list_create();
            rbt_add(leaf_cnt->rbt, key, k_size, entry);
        }
        size_t file_pos = insert_tagged_value(root_idx_anchor()->file_paths_list, file_path);
        size_t obj_pos = insert_tagged_value(root_idx_anchor()->object_paths_list, obj_path);
        
        file_obj_pair_t *file_obj_pair = (file_obj_pair_t *)calloc(1, sizeof(file_obj_pair_t));
        file_obj_pair->file_list_pos = file_pos;
        file_obj_pair->obj_list_pos = obj_pos;

        list_push_value(((value_tree_leaf_content_t *)entry)->file_obj_pair_list, (void *)file_obj_pair);
    }
}

void indexing_string(char *attr_name, char **attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 0;
    leaf_cnt->is_float = 0;

    if (leaf_cnt->art == NULL) {
        leaf_cnt->art = (art_tree *)ctr_calloc(1, sizeof(art_tree), &index_mem_size);
        art_tree_init(leaf_cnt->art);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        char *k = attr_val[i];
        value_tree_leaf_content_t *test_cnt = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, k, strlen(k));
        if (test_cnt == NULL){
            test_cnt = (value_tree_leaf_content_t *)ctr_calloc(1, sizeof(value_tree_leaf_content_t) , &index_mem_size);
            art_insert(leaf_cnt->art, (unsigned char *)k, strlen(k), (void *)test_cnt);
            test_cnt->file_obj_pair_list = list_create();
        }
        size_t file_pos = insert_tagged_value(root_idx_anchor()->file_paths_list, file_path);
        size_t obj_pos = insert_tagged_value(root_idx_anchor()->object_paths_list, obj_path);
        
        file_obj_pair_t *file_obj_pair = (file_obj_pair_t *)calloc(1, sizeof(file_obj_pair_t));
        file_obj_pair->file_list_pos = file_pos;
        file_obj_pair->obj_list_pos = obj_pos;

        list_push_value(test_cnt->file_obj_pair_list, (void *)file_obj_pair);
    }
}


power_search_rst_t *numeric_value_search(char *attr_name, void *value_p, size_t value_size){
    power_search_rst_t *prst =(power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
    prst->num_files=0;

    index_anchor *idx_anchor = root_idx_anchor();

    if (idx_anchor== NULL) {
        return prst;
    }
    
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));

    if (leaf_cnt == NULL || leaf_cnt->rbt == NULL) {
        return prst;
    }

    void *entry;
    int rbt_found = rbt_find(leaf_cnt->rbt, value_p, value_size, &entry);
    if (rbt_found!=0){//Not found
        return prst;
    } else {
        value_tree_leaf_content_t *node = (value_tree_leaf_content_t *)entry;
        prst->rst_arr = list_create();
        list_foreach_value(node->file_obj_pair_list, collect_result_from_list, prst);
        prst->num_files = list_count(prst->rst_arr);
    }
    return prst;
}

/**
 * This is key-value exact search
 * 
 */ 
power_search_rst_t *int_value_search(char *attr_name, int value) {
    return numeric_value_search(attr_name, &value, sizeof(int));
}

power_search_rst_t *float_value_search(char *attr_name, double value) {
    return numeric_value_search(attr_name, &value, sizeof(double));
}

power_search_rst_t *string_value_search(char *attr_name, char *value) {
    power_search_rst_t *prst =(power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
    prst->num_files=0;

    
    index_anchor *idx_anchor = root_idx_anchor();
    if (idx_anchor== NULL) {
        return prst;
    }
    
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));

    if (leaf_cnt == NULL || leaf_cnt->art == NULL) {
        return prst;
    }

    value_tree_leaf_content_t *test_cnt = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, (const unsigned char *)value, strlen(value));

    if (test_cnt == NULL) {
        return prst;
    } else {
        if (test_cnt->file_obj_pair_list == NULL) {
            return prst;
        }
        prst->rst_arr = list_create();
        list_foreach_value(test_cnt->file_obj_pair_list, collect_result_from_list, prst);
    }
    return prst;
}

/**
 * Dump in-memory index to disk
 * |file_list_region|object_list_region|attribute region|
 * return 1 on success;
 */
int dump_mdb_index_to_disk(char *filename, index_anchor *idx_anchor){
    FILE *disk_idx_stream = fopen(filename, "w");
    //1. Append all file_paths 
    linked_list_t *file_list = idx_anchor->file_paths_list;
    append_path_list(file_list, disk_idx_stream);
    //2. Append all object_paths
    linked_list_t *object_list = idx_anchor->object_paths_list;
    append_path_list(object_list, disk_idx_stream);
    //3. append attribute region
    art_tree *name_art = idx_anchor->root_art;
    append_attr_root_tree(name_art, disk_idx_stream);
    fclose(disk_idx_stream);
    return 1;
}

int load_mdb_file_to_index(char *filename, index_anchor *idx_anchor){
    int rst = 0;
    if (access(filename, F_OK)==0 && 
        access(filename, R_OK)==0 
        ){
        size_t fsize = get_file_size(filename);
        if (fsize > 0) {
            FILE *disk_idx_stream = fopen(filename, "r");
            fseek(disk_idx_stream, 0, SEEK_SET);
            idx_anchor->file_paths_list = list_create();
            int rst = read_into_path_list(idx_anchor->file_paths_list, disk_idx_stream);

            if (rst != 1){
                return 0;
            }

            idx_anchor->object_paths_list = list_create();
            rst = read_into_path_list(idx_anchor->object_paths_list, disk_idx_stream);

            if (rst != 1){
                return 0;
            }

            idx_anchor->root_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(idx_anchor->root_art);
            rst = read_into_attr_root_tree(idx_anchor->root_art, disk_idx_stream);
            fclose(disk_idx_stream);
        }
    }
    return rst;
}



