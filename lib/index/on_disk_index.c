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
 * datatype : 1. int, 2 float.
 */
int append_numeric_value_tree(const void **rootp, int numeric_data_type, FILE *stream){
}
int append_float_value_tree(const void **rootp, FILE *stream);
int append_string_art(art_tree *art, FILE *stream);
int append_string_linked_list(linked_list_t *list, FILE *stream);

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
}




