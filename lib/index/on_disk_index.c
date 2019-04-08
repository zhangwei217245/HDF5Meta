#include "on_disk_index.h"

/**
 * New index file format.
 * 
 * Attrname|Attrvalue1|FilePath+ObjPath1|FilePath+ObjPath2|
 * 
 */

/**
 * Old Index format
 * 
 */

/**
 * Create index record in memory.
 * 
 */
index_record_t *create_index_record(int type, char *name, void *data, char *file_path, char *object_path){
    index_record_t *rst = (index_record_t *)calloc(1, sizeof(index_record_t));
    rst->type = type;
    rst->name = name;
    rst->data = data;
    rst->file_path = file_path;
    rst->object_path = object_path;
    return rst;
}



/**
 *  append index_record to the current position of the stream
 *  users can call fseek to adjust the position of the file pointer to the end of the file. 
 */
void append_index_record(index_record_t *ir, FILE *stream){

    if (ir == NULL) return;

    miqs_append_type(ir->type, stream );

    miqs_append_string(ir->name, stream);


    if (ir->type == 1) {
        int *valp = (int *)ir->data;
        miqs_append_int(*valp, stream);
    } else if (ir->type == 2) {
        double *valp = (double *)ir->data;
        miqs_append_double(*valp, stream);
    } else if (ir->type == 3) {
        char *strv = (char *)ir->data;
        miqs_append_string(strv, stream);
    }

    miqs_append_string(ir->file_path, stream);
    miqs_append_string(ir->object_path, stream);

}

/**
 * Read one index record from current position of the file.
 */
index_record_t *read_index_record(FILE *stream){
    index_record_t *rst = (index_record_t *)calloc(1, sizeof(index_record_t));
    int *ptr = miqs_read_int(stream);
    if (ptr == NULL){
        return NULL;
    }
    rst->type = *ptr;

    rst->name = miqs_read_string(stream);
    if (rst->type == 1){
        rst->data = (void *)miqs_read_int(stream);
    } else if (rst->type == 2) {
        rst->data = (void *)miqs_read_double(stream);
    } else if (rst->type == 3) {
        rst->data = (void *)miqs_read_string(stream);
    }
    rst->file_path = miqs_read_string(stream);
    rst->object_path = miqs_read_string(stream);
    return rst;
}

/**
 * Print the index record just for visual verification.
 * 
 */
void print_index_record(index_record_t ir){
    char *template = "%s : %s -> %s : %s\n";
    char *pattern = (char *)calloc(1024, sizeof(char));
    char *data_ptn = "%s";
    if (ir.type == 1) {
        data_ptn = "%d";
        int *v = (int *)ir.data;
        sprintf(pattern, template, ir.name, data_ptn, ir.file_path, ir.object_path);
        printf(pattern, *v);
    } else if (ir.type == 2){
        data_ptn = "%.2f";
        double *v = (double *)ir.data;
        sprintf(pattern, template, ir.name, data_ptn, ir.file_path, ir.object_path);
        printf(pattern, *v);
    } else if (ir.type == 3) {
        data_ptn = "%s";
        char *str = (char *)ir.data;
        sprintf(pattern, template, ir.name, data_ptn, ir.file_path, ir.object_path);
        printf(pattern, str);
    } else {

        printf("unknown record\n");
    }
    fflush(stdin);
}


/**
 * Search over disk, from the beginning of a file, and collect result matching given compare function
 * 
 */
index_record_t **find_index_record(char *name,
        int (*compare_value)(const void *data, const void *criterion), void *criterion,
        FILE *stream, int *out_len){

    int rst_num = 0;
    if (out_len == NULL||stream ==NULL) {
        return NULL;
    }
    rewind(stream);
    if (*out_len<=0) {
        *out_len = 10;
    }
    int capacity = *out_len;
    int curr_pos = 0;
    index_record_t **rst = (index_record_t **)calloc(capacity, sizeof(index_record_t *));

    while (1) {

        int *ptr = miqs_read_int(stream);
        if (ptr == NULL){
            break;
        }

        long int skip_offset = 0;

        int type = *ptr;
        void *data;

        char *name_from_disk = miqs_read_string(stream);
        if (strcmp(name, name_from_disk)==0) {
            if (type == 1){
                data = (void *)miqs_read_int(stream);
            } else if (type == 2) {
                data = (void *)miqs_read_double(stream);
            } else if (type == 3) {
                data = (void *)miqs_read_string(stream);
            }

            if (compare_value(data, criterion)==1) {
                char *file_path = miqs_read_string(stream);
                char *object_path = miqs_read_string(stream);
                index_record_t *new_record = create_index_record(type, name, data, file_path, object_path);

                if (curr_pos == capacity) {
                    int new_capacity = capacity * 2;
                    index_record_t **new_rst = (index_record_t **)realloc(rst, new_capacity*sizeof(index_record_t));
                    if (new_rst == NULL){
                        fprintf(stderr, "[ERROR] Memory reallocation failed!\n");
                        return rst;
                    } else {
                        rst = new_rst;
                        capacity = new_capacity;
                    }
                }
                rst[curr_pos++] = new_record;
                rst_num++;
            }else {
                // skip two strings
                miqs_skip_field(stream);
                miqs_skip_field(stream);
            }
        } else {
            //skip value, two paths
            miqs_skip_field(stream);
            miqs_skip_field(stream);
            miqs_skip_field(stream);
        }
    }
    *out_len = capacity;
    int i = capacity-1;
    for (i = capacity -1 ; i >= rst_num; i--){
        free(rst[i]);
        *out_len=(*out_len)-1;
    }

    return rst;
}

/******* Functions below are test functions *****/

int int_equals(const void *data, const void *criterion){
    int *_data = (int *)data;
    int *_criterion = (int *)criterion;
    return _data[0] == _criterion[0];
}

int double_equals(const void *data, const void *criterion){
    double *_data = (double *)data;
    double *_criterion = (double *)criterion;
    return _data[0] == _criterion[0];
}


int string_equals(const void *data, const void *criterion){
    char *_data = (char *)data;
    char *_criterion = (char *)criterion;
    return strcmp(_data, _criterion)==0;
}

/**
 * This is a file object pair
 * |file_list_pos|object_list_pos|
 */
int append_file_obj_pair(void *item, size_t idx, void *fh){
    file_obj_pair_t *pair = (file_obj_pair_t *)item;
    FILE *stream = (FILE *)fh;
    miqs_append_size_t(pair->file_list_pos, stream);
    miqs_append_size_t(pair->obj_list_pos, stream);
    return 1;// return 1 for list iteration to continue;
}

/**
 * This is a file object pair list 
 * |list length = n|pair 1|...|pair n|
 */
void append_file_obj_pair_linked_list(linked_list_t *list, FILE *stream){
    size_t list_len = list_count(list);
    miqs_append_size_t(list_len, stream);
    list_foreach_value(list, append_file_obj_pair, stream);
}

/**
 * This is a string value node
 * |str_val|file_obj_pair_list|
 */
int append_string_value_node(void *fh, const unsigned char *key, 
    uint32_t key_len, void *value){
    FILE *stream = (FILE *)fh;
    // 1. append string value
    miqs_append_string_with_len((char *)key, (size_t)key_len, stream);
    // 2. append file obj pair list
    value_tree_leaf_content_t *leaf_cnt = (value_tree_leaf_content_t *)value;
    append_file_obj_pair_linked_list(leaf_cnt->file_obj_pair_list, stream);
    return 0;// return 0 for art iteration to continue;
}

/**
 * This is a int value node
 * |int value|file_obj_pair_list|
 */
rbt_walk_return_code_t append_int_value_node(rbt_t *rbt,
                                                    void *key,
                                                    size_t klen,
                                                    void *value,
                                                    void *priv){
    FILE *stream = (FILE *)priv;
    int *vk = (int *)key;
    // 1. append int value
    miqs_append_int(*vk, stream);
    // 2. append file obj pair list
    value_tree_leaf_content_t *leaf_cnt = (value_tree_leaf_content_t *)value;
    append_file_obj_pair_linked_list(leaf_cnt->file_obj_pair_list, stream);
    return RBT_WALK_CONTINUE;
}

/**
 * This is a float value node
 * |double value|file_obj_pair_list|
 */
rbt_walk_return_code_t append_float_value_node(rbt_t *rbt,
                                                    void *key,
                                                    size_t klen,
                                                    void *value,
                                                    void *priv){
    FILE *stream = (FILE *)priv;
    double *vk = (double *)key;
    // 1. append double value
    miqs_append_double(*vk, stream);
    // 2. append file obj pair list
    value_tree_leaf_content_t *leaf_cnt = (value_tree_leaf_content_t *)value;
    append_file_obj_pair_linked_list(leaf_cnt->file_obj_pair_list, stream);
    return RBT_WALK_CONTINUE;
}

/**
 * This is the string value region
 * |type = 3|number of values = n|value_node_1|...|value_node_n|
 * 
 * return number of strings in the string value tree
 */ 
int append_string_value_tree(art_tree *art, FILE *stream){
    // 1. type
    miqs_append_type(3, stream);
    // 2. number of values
    uint64_t num_str_value = art_size(art);
    miqs_append_uint64(num_str_value, stream);
    // 3. value nodes
    int rst = art_iter(art, append_string_value_node, stream);
    return rst==0?num_str_value:0;
}

/**
 * This is a numerical value region
 * return the number of visited nodes in rbt
 * |type = 1(int)/2(float)|number of values = n|value_node_1|...|value_node_n|
 */
int append_numeric_value_tree(rbt_t *rbt, int is_float, FILE *stream){
    // 1. type
    int type = 1;
    if (is_float == 0) {
        type = 1;
    } else if (is_float==1) {
        type = 2;
    }
    miqs_append_type(type, stream);
    // 2. number of values
    uint64_t num_num_value = rbt_size(rbt);
    miqs_append_uint64(num_num_value, stream);
    // 3. value nodes
    int rst = 0;
    if (is_float){
        rst = rbt_walk(rbt, append_int_value_node, stream);
    } else {
        rst = rbt_walk(rbt, append_float_value_node, stream);
    }
    return rst;
}

/**
 * return number of attribute values
 * This is an attribute node
 * |attr_name|attr_value_region|
 */
int append_attr_name_node(void *fh, const unsigned char *key, 
    uint32_t key_len, void *value){
    int rst = 0;
    FILE *stream = (FILE *)fh;
    //1. attr name
    char *attr_name = (char *)key;
    miqs_append_string_with_len(attr_name, (size_t)key_len, stream);
    // 2. attr value region
    attr_tree_leaf_content_t *attr_val_node = (attr_tree_leaf_content_t *)value;
    if (attr_val_node->is_numeric) {
        rst = append_numeric_value_tree(attr_val_node->rbt, 
        attr_val_node->is_float, stream);
    } else {
        rst = append_string_value_tree(attr_val_node->art, stream);
    }
    // printf("number of attribute values = %d\n", rst);
    return 0;// return 0 for art iteration to continue;
}

/**
 * This is the "attribute region"
 * |number of attributes = n|attr_node 1|...|attr_node n|
 */
int append_attr_root_tree(art_tree *art, FILE *stream){
    uint64_t num_str_value = art_size(art);
    miqs_append_uint64(num_str_value, stream);
    return art_iter(art, append_attr_name_node, stream);
}

/**
 * This is to persist a path. namely, a tagged value
 * |char *tag|size_t pos|
 */
int append_path(void *item, size_t idx, void *fh){
    tagged_value_t *tagv = (tagged_value_t *)item;
    FILE *stream = (FILE *)fh;
    miqs_append_string(tagv->tag, stream);
    size_t *pos_p = (size_t *)tagv->value;
    miqs_append_size_t(*pos_p, stream);
    return 1;// return 1 for list iteration to continue;
}

/**
 * This is for file/obj lookup table
 * |num of paths|path 1|...|path n|
 */
int append_path_list(linked_list_t *list, FILE *stream){
    size_t list_len = list_count(list);
    miqs_append_size_t(list_len, stream);
    return list_foreach_value(list, append_path, stream);
}

/*****************************************/
/**
 * return 1 on success;
 * 
 */
int read_into_path_list(linked_list_t *list, FILE *stream){
    size_t *list_len = miqs_read_size_t(stream);
    size_t i = 0;
    int rst = 0;
    for (i = 0; i < *list_len; i++){
        char *tag = miqs_read_string(stream);
        size_t *pos = miqs_read_size_t(stream);
        tagged_value_t *tagv = list_create_tagged_value(tag, pos, sizeof(size_t));
        rst = rst | list_insert_tagged_value(list, tagv, *pos);
    }
    return rst==0;
}

int read_into_file_obj_list(linked_list_t *list, FILE *stream){
    size_t *file_pos = miqs_read_size_t(stream);
    size_t *obj_pos = miqs_read_size_t(stream);
    if (list == NULL) {
        return 0;
    }
    file_obj_pair_t *pair = (file_obj_pair_t *)calloc(1, sizeof(file_obj_pair_t));
    pair->file_list_pos = *file_pos;
    pair->obj_list_pos = *obj_pos;
    return list_push_value(list, pair);
}

int read_file_obj_path_pair_list(value_tree_leaf_content_t *vtree_leaf, FILE *stream){
    int rst = 0;
    size_t *list_len = miqs_read_size_t(stream);

    if (vtree_leaf==NULL) {
        size_t i = 0;
        for (i = 0; i < *list_len; i++){
            rst = rst | read_into_file_obj_list(NULL, stream);
        }
        return rst;
    }
    vtree_leaf->file_obj_pair_list=list_create();
    size_t i = 0;
    for (i = 0; i < *list_len; i++){
        rst = rst | read_into_file_obj_list(vtree_leaf->file_obj_pair_list, stream);
    }
    return rst;
}

int read_attr_value_node(attr_tree_leaf_content_t *attr_val_node, int type, FILE *stream){
    value_tree_leaf_content_t *val_leaf = (value_tree_leaf_content_t *)
                    calloc(1, sizeof(value_tree_leaf_content_t));
    int rst = 0;
    void *_value = NULL;
    if (type == 3){
        _value = miqs_read_string(stream);
        if (_value) {
            char *val = (char *)_value;
            art_insert(attr_val_node->art, (const unsigned char *)val, strlen(val), val_leaf);
            rst = 0;
        }
    } else if (type == 2){
        _value = miqs_read_double(stream);
        if (_value) {
            double *val = (double *)_value;
            rst = rbt_add(attr_val_node->rbt, val, sizeof(double), val_leaf);
        }
    } else if (type == 1) {
        _value = miqs_read_int(stream);
        if (_value) {
            int *val = (int *)_value;
            rst = rbt_add(attr_val_node->rbt, val, sizeof(int), val_leaf);
        }
    }
    if (_value) {
        rst = rst | read_file_obj_path_pair_list(val_leaf, stream);
    } else {
        rst = 1;
    }
    return rst;
}

int read_attr_values(attr_tree_leaf_content_t *attr_val_node, FILE *stream){
    int rst = 0;
    int *type = miqs_read_int(stream);
    if (*type == 3) { //string
        attr_val_node->is_numeric=0;
        attr_val_node->is_float=0;
        attr_val_node->art = (art_tree *)calloc(1, sizeof(art_tree));
        art_tree_init(attr_val_node->art);
    } else {
        attr_val_node->is_numeric=1;
        if (*type == 1){ // int
            attr_val_node->is_float=0;
            attr_val_node->rbt = rbt_create(libhl_cmp_keys_int, free);
        } else if (*type == 2){// double
            attr_val_node->is_float=1;
            attr_val_node->rbt = rbt_create(libhl_cmp_keys_double, free);
        }
    }
    uint64_t *num_values = miqs_read_uint64(stream);
    uint64_t i = 0;
    for (i = 0; i < *num_values; i++){
        rst = rst | read_attr_value_node(attr_val_node, *type, stream);
    }
    return rst;
}

int read_attr_name_node(art_tree *art, FILE *stream){
    char *attr_name = miqs_read_string(stream);
    attr_tree_leaf_content_t *attr_val_node = 
        (attr_tree_leaf_content_t *)calloc(1, sizeof(attr_tree_leaf_content_t));
    art_insert(art, (const unsigned char *)attr_name, strlen(attr_name), attr_val_node);
    return read_attr_values(attr_val_node, stream);
}
/**
 * return 1 on success;
 */
int read_into_attr_root_tree(art_tree *art, FILE *stream){
    uint64_t *num_attrs = miqs_read_uint64(stream);
    uint64_t i = 0;
    int rst = 0;
    for (i = 0; i < *num_attrs; i++){
        rst = rst | read_attr_name_node(art, stream);
    }
    return rst == 0;
}
/*****************************************/

int test(int argc, char **argv){

    if (argc < 2) {
        printf("please give a filename");
    };

    char *filename = argv[1];

    FILE *file = fopen(filename, "w");

    int temp = 50;
    double height = 50.5;
    char *descript = "test data";


    index_record_t *a = create_index_record(1, "temp", (void *)&temp, "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *b = create_index_record(2, "height", (void *)&height, "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *c = create_index_record(3, "desc", (void *)"test data", "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *d = create_index_record(3, "desc", (void *)"test ", "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *e = create_index_record(3, "desc", (void *)"test data", "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *f = create_index_record(2, "height", (void *)&height, "/a/b/c/d.h5", "/g1/g2/d1");
    index_record_t *g = create_index_record(1, "temp", (void *)&temp, "/a/b/c/d.h5", "/g1/g2/d1");

    append_index_record(a, file);
    append_index_record(c, file);
    append_index_record(b, file);

    append_index_record(d, file);
    append_index_record(f, file);
    append_index_record(e, file);
    append_index_record(g, file);

    fclose(file);


    FILE *filei = fopen(filename, "r");

    while(1){
        index_record_t *x = read_index_record(filei);
        if (x == NULL){
            break;
        }
        print_index_record(*x);

    }

    int out_len = 1;

    index_record_t **query_rst = find_index_record("desc", string_equals, "test data", filei, &out_len);

    if (query_rst) {
        int i = 0;
        for (i= 0; i < out_len; i++){
            if (query_rst[i]) {
                print_index_record(*query_rst[i]);
            }
        }
    }
    

    fclose(filei);
    return 0;
}


int is_mdb(const struct dirent *entry){
    if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0) {
        return 0;
    }
    if (entry->d_type == DT_DIR){
        return 1;
    }
    if(endsWith(entry->d_name, ".mdb")) {
        return 1;
    }
    return 0;
}

int is_aof(const struct dirent *entry){
    if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0) {
        return 0;
    }
    if (entry->d_type == DT_DIR){
        return 1;
    }
    if(endsWith(entry->d_name, ".aof")) {
        return 1;
    }
    return 0;
}

