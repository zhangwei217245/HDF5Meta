#include "in_mem_index.h"
#include <pthread.h>


index_anchor *idx_anchor;

size_t index_mem_size;

// #define GETTER_SETTER_NAME(_GS, _G_FD_NAME) _GS##_G_FD_NAME
// #define GETTER_SETTER(_type, G_FD_NAME) _type GETTER_SETTER_NAME(get, G_FD_NAME) (index_anchor *idx_anchor){\
//     return idx_anchor->G_FD_NAME;\
// }\
// void GETTER_SETTER_NAME(set, G_FD_NAME)(index_anchor *idx_anchor, _type G_FD_NAME){\
//     idx_anchor->G_FD_NAME=G_FD_NAME;\
// }


// GETTER_SETTER(char         *,file_path                 );
// GETTER_SETTER(char         *,obj_path                  );
// GETTER_SETTER(hid_t        ,object_id                 );
// GETTER_SETTER(art_tree     *,root_art                  );
// GETTER_SETTER(linked_list_t *,file_paths_list           );
// GETTER_SETTER(linked_list_t *,object_paths_list         );
// GETTER_SETTER(char         **,indexed_attr              );
// GETTER_SETTER(int          ,num_indexed_field         );
// GETTER_SETTER(FILE         *,on_disk_file_stream       );
// GETTER_SETTER(int          ,is_readonly_index_file    );

// GETTER_SETTER(size_t, total_num_files)
// GETTER_SETTER(size_t, total_num_objects)
// GETTER_SETTER(size_t, total_num_attrs)
// GETTER_SETTER(size_t, total_num_indexed_kv_pairs)
// GETTER_SETTER(size_t, total_num_kv_pairs)
// GETTER_SETTER(suseconds_t, us_to_index)
// GETTER_SETTER(suseconds_t, us_to_disk_index)

// size_t get_total_num_files              (index_anchor *idx_anchor){
//     return idx_anchor->total_num_files;
// }
// size_t get_total_num_objects            (index_anchor *idx_anchor){
//     return idx_anchor->total_num_objects;
// }
// size_t get_total_num_attrs              (index_anchor *idx_anchor){
//     return idx_anchor->total_num_attrs;
// }
// size_t get_total_num_indexed_kv_pairs   (index_anchor *idx_anchor){
//     return idx_anchor->total_num_indexed_kv_pairs;
// }
// size_t get_total_num_kv_pairs           (index_anchor *idx_anchor){
//     return idx_anchor->total_num_kv_pairs;
// }

// suseconds_t get_us_to_index         (index_anchor *idx_anchor){
//     return idx_anchor->us_to_index;
// }
// suseconds_t get_us_to_disk_index    (index_anchor *idx_anchor){
//     return idx_anchor->us_to_disk_index;
// }

index_anchor *root_idx_anchor(){
    return idx_anchor;
}

size_t *get_index_size_ptr(){
    return &index_mem_size;
}

size_t get_index_size(){
    return index_mem_size;
}

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
    return 1;
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

    #if MIQS_INDEX_CONCURRENT_LEVEL==1
        pthread_rwlock_init(&(idx_anchor->GLOBAL_INDEX_LOCK), NULL);
        pthread_rwlock_t GLOBAL_INDEX_LOCK;
    #elif MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_init(&(idx_anchor->TOP_ART_LOCK), NULL);
    #else
        /* nothing here for tree-node protection */
    #endif

    return 1;
}

void create_in_mem_index_for_attr(index_anchor *idx_anchor, miqs_meta_attribute_t *attr){
    art_tree *global_art = idx_anchor->root_art;
    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor->obj_path;
    int into_art = 1;
    stopwatch_t one_attr;   
    timer_start(&one_attr);

#if MIQS_INDEX_CONCURRENT_LEVEL==1
    pthread_rwlock_wrlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#elif MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_rdlock(&(idx_anchor->TOP_ART_LOCK));
#else
    /* nothing here for tree-node protection */
#endif

    attr_tree_leaf_content_t *leaf_cnt = (attr_tree_leaf_content_t *)art_search(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name));
    
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_unlock(&(idx_anchor->TOP_ART_LOCK));
#endif

    if (leaf_cnt == NULL){
        leaf_cnt = (attr_tree_leaf_content_t *)ctr_calloc(1, sizeof(attr_tree_leaf_content_t), get_index_size_ptr());
        // void *bptr = NULL;
        // leaf_cnt->bpt = (void ***)ctr_calloc(1, sizeof(void **), get_index_size_ptr());
        // (leaf_cnt->bpt)[0] = (void **)ctr_calloc(1, sizeof(void *), get_index_size_ptr());
        // (leaf_cnt->bpt)[0][0] = NULL;
        switch(attr->attr_type) {
            case MIQS_AT_INTEGER:
                leaf_cnt->rbt = rbt_create(libhl_cmp_keys_int, free);
                leaf_cnt->is_float=0;
                leaf_cnt->is_numeric = 1;
                break;
            case MIQS_AT_FLOAT:
                leaf_cnt->is_float=1;
                leaf_cnt->is_numeric = 1;
                leaf_cnt->rbt = rbt_create(libhl_cmp_keys_double, free);
                break;
            case MIQS_AT_STRING:
                leaf_cnt->is_float=0;
                leaf_cnt->is_numeric = 0;
                leaf_cnt->art = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
            default:
                into_art = 0;
                break;
        }
        if (into_art == 1) {

#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_wrlock(&(idx_anchor->TOP_ART_LOCK));
#endif
            art_insert(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name), leaf_cnt);

#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_unlock(&(idx_anchor->TOP_ART_LOCK));
#endif

        }
    }

    switch(attr->attr_type) {
        case MIQS_AT_INTEGER:
            indexing_numeric(attr->attr_name,(int *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case MIQS_AT_FLOAT:
            indexing_numeric(attr->attr_name, (double *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case MIQS_AT_STRING:
            indexing_string(attr->attr_name, (char **)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        default:
            // printf("Ignore unsupported attr_type for attribute %s\n", name);
            // into_art = 0;
            break;
    }

#if MIQS_INDEX_CONCURRENT_LEVEL==1
            pthread_rwlock_unlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#endif

    timer_pause(&one_attr);
    suseconds_t one_attr_duration = timer_delta_us(&one_attr);
    idx_anchor->us_to_index += one_attr_duration;
}


int indexing_attr(index_anchor *idx_anchor, miqs_meta_attribute_t *attr) {
    char *file_path = idx_anchor->file_path;
    char *obj_path = idx_anchor->obj_path;
    // hid_t obj_id = (hid_t)idx_anchor->object_id;
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
        idx_anchor->total_num_kv_pairs+=attr->attribute_value_length;
        // Create on-disk AOF index, if the index file is not read only.
        if (idx_anchor->on_disk_file_stream!= NULL && idx_anchor->is_readonly_index_file==0) {
            stopwatch_t one_disk_attr;   
            timer_start(&one_disk_attr);
            write_aof(idx_anchor->on_disk_file_stream, attr, file_path, obj_path);
            suseconds_t one_disk_attr_duration = timer_delta_us(&one_disk_attr);
            idx_anchor->us_to_disk_index += one_disk_attr_duration;
        }
        create_in_mem_index_for_attr(idx_anchor, attr);
        idx_anchor->total_num_indexed_kv_pairs+=attr->attribute_value_length;
    }
    //TODO: currently ignore any error.
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

#if MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_rdlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif

        int rbt_found = rbt_find(leaf_cnt->rbt, key, k_size, &entry);

#if MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_unlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif

        if (rbt_found != 0) {// not found
            entry = (value_tree_leaf_content_t *)ctr_calloc(1, sizeof(value_tree_leaf_content_t), &index_mem_size);
            ((value_tree_leaf_content_t *)entry)->file_obj_pair_list = list_create();
#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_wrlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
            rbt_add(leaf_cnt->rbt, key, k_size, entry);
#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_unlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
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
#if MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_rdlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
        value_tree_leaf_content_t *test_cnt = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, (const unsigned char *)k, strlen(k));
#if MIQS_INDEX_CONCURRENT_LEVEL==2
        pthread_rwlock_unlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
        if (test_cnt == NULL){
            test_cnt = (value_tree_leaf_content_t *)ctr_calloc(1, sizeof(value_tree_leaf_content_t) , &index_mem_size);
#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_wrlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
            art_insert(leaf_cnt->art, (unsigned char *)k, strlen(k), (void *)test_cnt);
#if MIQS_INDEX_CONCURRENT_LEVEL==2
            pthread_rwlock_rdlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif
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
#if MIQS_INDEX_CONCURRENT_LEVEL==1
    pthread_rwlock_rdlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#endif
    power_search_rst_t *prst =(power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
    prst->num_files=0;

    index_anchor *idx_anchor = root_idx_anchor();

    if (idx_anchor== NULL) {
        return prst;
    }
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_rdlock(&(idx_anchor->TOP_ART_LOCK));
#endif
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_unlock(&(idx_anchor->TOP_ART_LOCK));
#endif
    if (leaf_cnt == NULL || leaf_cnt->rbt == NULL) {
        return prst;
    }

    void *entry;
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_rdlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif 
    int rbt_found = rbt_find(leaf_cnt->rbt, value_p, value_size, &entry);
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_unlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif 
    if (rbt_found!=0){//Not found
        return prst;
    } else {
        value_tree_leaf_content_t *node = (value_tree_leaf_content_t *)entry;
        prst->rst_arr = list_create();
        list_foreach_value(node->file_obj_pair_list, collect_result_from_list, prst);
        prst->num_files = list_count(prst->rst_arr);
    }
#if MIQS_INDEX_CONCURRENT_LEVEL==1
    pthread_rwlock_unlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#endif
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
#if MIQS_INDEX_CONCURRENT_LEVEL==1
    pthread_rwlock_rdlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#endif
    power_search_rst_t *prst =(power_search_rst_t *)calloc(1, sizeof(power_search_rst_t));
    prst->num_files=0;

    
    index_anchor *idx_anchor = root_idx_anchor();
    if (idx_anchor== NULL) {
        return prst;
    }
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_rdlock(&(idx_anchor->TOP_ART_LOCK));
#endif
    attr_tree_leaf_content_t *leaf_cnt =
    (attr_tree_leaf_content_t *)art_search(idx_anchor->root_art, 
    (const unsigned char *)attr_name, strlen(attr_name));
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_unlock(&(idx_anchor->TOP_ART_LOCK));
#endif
    if (leaf_cnt == NULL || leaf_cnt->art == NULL) {
        return prst;
    }
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_rdlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif   
    value_tree_leaf_content_t *test_cnt = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, (const unsigned char *)value, strlen(value));
#if MIQS_INDEX_CONCURRENT_LEVEL==2
    pthread_rwlock_unlock(&(leaf_cnt->VALUE_TREE_LOCK));
#endif 
    if (test_cnt == NULL) {
        return prst;
    } else {
        if (test_cnt->file_obj_pair_list == NULL) {
            return prst;
        }
        prst->rst_arr = list_create();
        list_foreach_value(test_cnt->file_obj_pair_list, collect_result_from_list, prst);
    }
#if MIQS_INDEX_CONCURRENT_LEVEL==1
    pthread_rwlock_unlock(&(idx_anchor->GLOBAL_INDEX_LOCK));
#endif
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
            idx_anchor->total_num_files += list_count(idx_anchor->file_paths_list);
            if (rst != 1){
                return 0;
            }

            idx_anchor->object_paths_list = list_create();
            rst = read_into_path_list(idx_anchor->object_paths_list, disk_idx_stream);
            idx_anchor->total_num_objects += list_count(idx_anchor->object_paths_list);
            if (rst != 1){
                return 0;
            }

            idx_anchor->root_art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(idx_anchor->root_art);
            rst = read_into_attr_root_tree(idx_anchor->root_art, disk_idx_stream);
            fclose(disk_idx_stream);
            idx_anchor->total_num_kv_pairs += get_num_kv_pairs_loaded_mdb();
            idx_anchor->total_num_attrs += get_num_attrs_loaded_mdb();
        }
    }
    return rst;
}

void convert_index_record_to_in_mem_parameters(index_anchor *idx_anchor, miqs_meta_attribute_t *attr, index_record_t *ir){
    attr->attr_name = ir->name;
    attr->attr_type = ir->type;
    if (ir->type == MIQS_AT_STRING) {
        char *str_val = (char *)ir->data;
        attr->attribute_value = (void *)&str_val;
        attr->attribute_value_length = 1;
    } else {
        attr->attribute_value = ir->data;
        attr->attribute_value_length = 1;
    }
    idx_anchor->file_path=ir->file_path;
    idx_anchor->obj_path=ir->object_path;
}

int load_mdb(char *filepath, index_file_loading_param_t *param) {
    return load_mdb_file_to_index(filepath, param->idx_anchor);
}

void write_aof(FILE *stream, miqs_meta_attribute_t *attr, char *file_path, char *obj_path){
    if (attr->attr_type == MIQS_AT_INTEGER) {
        int i = 0;
        for (i = 0; i < attr->attribute_value_length; i++) {
            int *value_arr = (int *)attr->attribute_value;
            index_record_t *ir =
            create_index_record((int)MIQS_AT_INTEGER, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
            append_index_record(ir, stream);
        }
    } else if (attr->attr_type == MIQS_AT_FLOAT) {
        int i = 0;
        for (i = 0; i < attr->attribute_value_length; i++) {
            double *value_arr = (double *)attr->attribute_value;
            index_record_t *ir =
            create_index_record((int)MIQS_AT_FLOAT, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
            append_index_record(ir, stream);
        }
    } else if (attr->attr_type == MIQS_AT_STRING) {
        int i = 0;
        for (i = 0; i < attr->attribute_value_length; i++) {
            char  **value_arr = (char  **)attr->attribute_value;
            index_record_t *ir =
            create_index_record((int)MIQS_AT_STRING, attr->attr_name, (void *)(&(value_arr[i])), file_path, obj_path);
            append_index_record(ir, stream);
        }
    }
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
                indexing_attr(idx_anchor, &attr);
                count++;
            }
            fclose(idx_anchor->on_disk_file_stream);
        }
    }
    return count;
}

