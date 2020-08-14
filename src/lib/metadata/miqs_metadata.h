#ifndef MIQS_METADATA_H
#define MIQS_METADATA_H

#include "../include/base_stdlib.h"
#include "../include/c99_stdlib.h"

typedef enum miqs_attr_type {MIQS_AT_UNKNOWN=0, MIQS_AT_INTEGER = 1, MIQS_AT_FLOAT = 2, MIQS_AT_STRING = 3} miqs_attr_type_t;

typedef enum miqs_object_type {MIQS_OT_UNKNOWN = 0, MIQS_OT_GROUP = 1, MIQS_OT_DATASET = 2} miqs_object_type_t;


typedef struct miqs_meta_attribute {
    char *attr_name;
    miqs_attr_type_t attr_type; //H5T_class_t
    void *attribute_value;
    int attribute_value_length;

    struct miqs_meta_attribute *next;
    struct miqs_meta_attribute *head; // each node maintains the head pointer. 
    struct miqs_meta_attribute *tail; // Only head maintain tail pointer. 
} miqs_meta_attribute_t;

typedef struct miqs_data_object{
    void *opdata;

    void *obj_id; //hid_t
    char *obj_name;
    void **obj_info; //H5O_info_t *
    miqs_object_type_t obj_type;

    struct miqs_data_object *sub_obj_list;
    int num_sub_objs;
    int (*on_sub_obj)(void *opdata, struct miqs_data_object *obj);

    int num_attrs;
    miqs_meta_attribute_t *attr_linked_list; 
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr);

    struct miqs_data_object *next;
    struct miqs_data_object *head; // each node maintains the head pointer. 
    struct miqs_data_object *tail; // Only head maintain tail pointer. 
} miqs_data_object_t;


miqs_attr_type_t get_miqs_type_from_int(int number);

#endif /* !MIQS_METADATA_H */
