#include "in_mem_index.h"
#include <string.h>
#include <pthread.h>


index_anchor_t *root_index_anchor=NULL;

size_t index_mem_size;



index_anchor_t *root_idx_anchor(){
    return root_index_anchor;
}

size_t *get_index_size_ptr(){
    return &index_mem_size;
}

size_t get_index_size(){
    return index_mem_size;
}

int is_specified_field(char *name, index_anchor_t *idx_anchor) {
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



int init_in_mem_index(int _parallelism){
    root_index_anchor = (index_anchor_t *)ctr_calloc(1, sizeof(index_anchor_t), get_index_size_ptr());
    root_index_anchor->parallelism = _parallelism;
    root_index_anchor->root_art_array = (art_tree **)ctr_calloc(root_index_anchor->parallelism, sizeof(art_tree *), get_index_size_ptr());
    root_index_anchor->file_paths_list= (linked_list_t **)ctr_calloc(root_index_anchor->parallelism, sizeof(linked_list_t *), get_index_size_ptr());
    root_index_anchor->object_paths_list=(linked_list_t **)ctr_calloc(root_index_anchor->parallelism, sizeof(linked_list_t *), get_index_size_ptr());;
    root_index_anchor->GLOBAL_INDEX_LOCK = (pthread_rwlock_t *)ctr_calloc(root_index_anchor->parallelism, sizeof(pthread_rwlock_t), get_index_size_ptr());

    int i = 0;
    for (i = 0; i < root_index_anchor->parallelism ; i++) {
        // initialize art
        root_index_anchor->root_art_array[i] = (art_tree *)ctr_calloc(1, sizeof(art_tree), get_index_size_ptr());
        art_tree_init(root_index_anchor->root_art_array[i]);
        // initialize file path list
        root_index_anchor->file_paths_list[i]=list_create();
        // initialize file path list
        root_index_anchor->object_paths_list[i]=list_create();
        // initialize read write lock
        root_index_anchor->GLOBAL_INDEX_LOCK[i] = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;
        pthread_rwlock_init(&(root_index_anchor->GLOBAL_INDEX_LOCK[i]), NULL);
    }
    root_index_anchor->file_path=NULL;
    root_index_anchor->obj_path=NULL;
    root_index_anchor->indexed_attr=NULL;
    root_index_anchor->num_indexed_field=0;
    root_index_anchor->on_disk_file_stream=NULL;
    root_index_anchor->is_readonly_index_file=1;
    root_index_anchor->total_num_files=0;
    root_index_anchor->total_num_objects=0;
    root_index_anchor->total_num_attrs=0;
    root_index_anchor->total_num_indexed_kv_pairs=0;
    root_index_anchor->total_num_kv_pairs=0;
    root_index_anchor->us_to_index=0;
    root_index_anchor->us_to_disk_index=0;
    return 1;
}

/**
 * Put a single metadata attribute key-value pair into in-memory index.
 */
void create_in_mem_index_for_attr(index_anchor_t *idx_anchor, miqs_meta_attribute_t *attr){
    unsigned long attr_name_hval = djb2_hash((unsigned char *)attr->attr_name) % idx_anchor->parallelism;
    pthread_rwlock_wrlock(&(idx_anchor->GLOBAL_INDEX_LOCK[attr_name_hval]));
    art_tree *global_art = idx_anchor->root_art_array[attr_name_hval];
    char *file_path = attr->file_path_str;
    char *obj_path = attr->obj_path_str;
    int into_art = 1;
    stopwatch_t one_attr;   
    timer_start(&one_attr);

    attr_tree_leaf_content_t *leaf_cnt = (attr_tree_leaf_content_t *)art_search(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name));

    if (leaf_cnt == NULL){
        leaf_cnt = (attr_tree_leaf_content_t *)ctr_calloc(1, sizeof(attr_tree_leaf_content_t), get_index_size_ptr());
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
            art_insert(global_art, (const unsigned char *)attr->attr_name, strlen(attr->attr_name), leaf_cnt);
        }
    }

    switch(attr->attr_type) {
        case MIQS_AT_INTEGER:
            indexing_numeric(attr_name_hval,(int *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case MIQS_AT_FLOAT:
            indexing_numeric(attr_name_hval, (double *)attr->attribute_value, 
            attr->attribute_value_length, file_path, obj_path, leaf_cnt);
            break;
        case MIQS_AT_STRING:
            indexing_string(attr_name_hval, (char **)attr->attribute_value, 
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

    pthread_rwlock_unlock(&(idx_anchor->GLOBAL_INDEX_LOCK[attr_name_hval]));

}

/**
 * Indexing a single metadata attribute (in the form of key-value pair) 
 */
int indexing_attr(index_anchor_t *idx_anchor, miqs_meta_attribute_t *attr) {
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
void indexing_numeric(size_t parallel_index_pos, void *attr_val, int attribute_value_length, 
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
        size_t file_pos = insert_tagged_value(root_idx_anchor()->file_paths_list[parallel_index_pos], file_path);
        size_t obj_pos = insert_tagged_value(root_idx_anchor()->object_paths_list[parallel_index_pos], obj_path);
        
        file_obj_pair_t *file_obj_pair = (file_obj_pair_t *)calloc(1, sizeof(file_obj_pair_t));
        file_obj_pair->parallel_index_pos = parallel_index_pos;
        file_obj_pair->file_list_pos = file_pos;
        file_obj_pair->obj_list_pos = obj_pos;

        list_push_value(((value_tree_leaf_content_t *)entry)->file_obj_pair_list, (void *)file_obj_pair);
    }
}

void indexing_string(size_t parallel_index_pos, char **attr_val, int attribute_value_length, 
char *file_path, char *obj_path, attr_tree_leaf_content_t *leaf_cnt){
    leaf_cnt->is_numeric = 0;
    leaf_cnt->is_float = 0;

    if (leaf_cnt->art == NULL) {
        leaf_cnt->art = (art_tree *)ctr_calloc(1, sizeof(art_tree), &index_mem_size);
        art_tree_init(leaf_cnt->art);
    }
    int i = 0;
    for (i = 0; i < attribute_value_length; i++) {
        value_tree_leaf_content_t *test_cnt = (value_tree_leaf_content_t *)art_search(leaf_cnt->art, 
            (const unsigned char *)attr_val[i], strlen(attr_val[i]));

        if (test_cnt == NULL){
            test_cnt = (value_tree_leaf_content_t *)ctr_calloc(1, sizeof(value_tree_leaf_content_t) , &index_mem_size);
            test_cnt->file_obj_pair_list = list_create();
            art_insert(leaf_cnt->art, (unsigned char *)attr_val[i], strlen(attr_val[i]), (void *)test_cnt);
        }
        size_t file_pos = insert_tagged_value(root_idx_anchor()->file_paths_list[parallel_index_pos], file_path);
        size_t obj_pos = insert_tagged_value(root_idx_anchor()->object_paths_list[parallel_index_pos], obj_path);
        
        file_obj_pair_t *file_obj_pair = (file_obj_pair_t *)calloc(1, sizeof(file_obj_pair_t));
        file_obj_pair->parallel_index_pos = parallel_index_pos;
        file_obj_pair->file_list_pos = file_pos;
        file_obj_pair->obj_list_pos = obj_pos;

        list_push_value(test_cnt->file_obj_pair_list, (void *)file_obj_pair);
    }
}

/**
 * Dump in-memory index to disk
 * |parallism|file_list_region|object_list_region|attribute region|...<repeat>|
 * return 1 on success;
 */
int dump_mdb_index_to_disk(char *filename, index_anchor_t *idx_anchor){
    FILE *disk_idx_stream = fopen(filename, "w");
    size_t parallelism = (size_t)idx_anchor->parallelism;
    miqs_append_size_t(parallelism, disk_idx_stream);
    int i = 0;
    for (i = 0; i < parallelism; i++) {
        //1. Append all file_paths 
        linked_list_t *file_list = idx_anchor->file_paths_list[i];
        append_path_list(file_list, disk_idx_stream);
        //2. Append all object_paths
        linked_list_t *object_list = idx_anchor->object_paths_list[i];
        append_path_list(object_list, disk_idx_stream);
        //3. append attribute region
        int i = 0;
        for (i = 0; i < idx_anchor->parallelism; i++) {
            art_tree *name_art = idx_anchor->root_art_array[i];
            append_attr_root_tree(name_art, disk_idx_stream);
        }
    }
    fclose(disk_idx_stream);
    return 1;
}


int load_mdb_file_to_index(char *filename, index_anchor_t *_idx_anchor){
    int rst = 0;
    if (access(filename, F_OK)==0 && 
        access(filename, R_OK)==0 
        ){
        size_t fsize = get_file_size(filename);
        if (fsize > 0) {
            FILE *disk_idx_stream = fopen(filename, "r");
            fseek(disk_idx_stream, 0, SEEK_SET);
            // read parallelism
            size_t *parallelism = miqs_read_size_t(disk_idx_stream);
            // test if it is in loading procedure: if _idx_anchor is null, it is in loading procedure
            index_anchor_t *idx_anchor = _idx_anchor;
            if (_idx_anchor == NULL) {
                init_in_mem_index(*parallelism);
                idx_anchor = root_idx_anchor();
            }
            // initialize in-memory art trees according to parallelism parameter recoreded in the mdb file.
            int i = 0;
            // Repeat the index loading procedure for p times (p = parallelism)
            for (i = 0; i < idx_anchor->parallelism; i++){
                int rst = read_into_path_list(idx_anchor->file_paths_list[i], disk_idx_stream);
                idx_anchor->total_num_files += list_count(idx_anchor->file_paths_list[i]);
                if (rst != 1){
                    return 0;
                }

                rst = read_into_path_list(idx_anchor->object_paths_list[i], disk_idx_stream);
                idx_anchor->total_num_objects += list_count(idx_anchor->object_paths_list[i]);
                if (rst != 1){
                    return 0;
                }

                rst = read_into_attr_root_tree(idx_anchor->root_art_array, idx_anchor->parallelism, disk_idx_stream);
            }
            fclose(disk_idx_stream);
            idx_anchor->total_num_kv_pairs += get_num_kv_pairs_loaded_mdb();
            idx_anchor->total_num_attrs += get_num_attrs_loaded_mdb();
        }
    }
    return rst;
}

int load_mdb(char *filepath, index_anchor_t *idx_ancr) {
    return load_mdb_file_to_index(filepath, idx_ancr);
}

void convert_index_record_to_in_mem_parameters(index_anchor_t *idx_anchor, miqs_meta_attribute_t *attr, index_record_t *ir){
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

int load_aof(char *filepath, index_anchor_t *idx_ancr){
    size_t count = 0;
    index_anchor_t *idx_anchor = idx_ancr;
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

