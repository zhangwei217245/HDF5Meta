
/* File foo.  */
#ifndef MIQS_NEW_HDF5_META_H
#define MIQS_NEW_HDF5_META_H

#include "../include/base_stdlib.h"
#include "../include/c95_stdlib.h"
#include "../include/c99_stdlib.h"
#include "../utils/query_utils.h"
#include "../utils/timer_utils.h"
#include "../utils/string_utils.h"
#include "../ds/vector.h"
#include "hdf5.h"


typedef struct h5attribute {
    char *attr_name;
    H5T_class_t attr_type;
    void *attribute_value;
    int attribute_value_length;

    struct h5attribute *next;
    struct h5attribute *head; // each node maintains the head pointer. 
    struct h5attribute *tail; // Only head maintain tail pointer. 
} h5attribute_t;

typedef struct h5object{
    void *opdata;

    hid_t obj_id;
    char *obj_name;
    H5O_info_t *obj_info;

    struct h5object *sub_obj_list;
    int num_sub_objs;
    int (*on_sub_obj)(void *opdata, struct h5object *obj);

    int num_attrs;
    h5attribute_t *attr_linked_list; 
    int (*on_attr)(void *opdata, h5attribute_t *attr);

    struct h5object *next;
    struct h5object *head; // each node maintains the head pointer. 
    struct h5object *tail; // Only head maintain tail pointer. 
} h5object_t;

typedef struct metadata_collector {
    h5object_t *object_linked_list;
    int num_objs;
    int total_num_attrs;
    void *opdata;
    int (*on_obj)(void *opdata, h5object_t *obj);
    int (*on_attr)(void *opdata, h5attribute_t *attr);
} metadata_collector_t;

/**
 * This function recursively go through all objects in a single HDF5 file
 * And calling action function in meta_collector, passing its data field as parameter.
 * 
 * @file_path           : the path of HDF5 file
 * @meta_collector      : the metadata collector 'object' including data field and action method. 
 */
int scan_hdf5(char *file_path, metadata_collector_t *meta_collector, int is_visit_link);



/**
 * Auxiliary function that translate H5O_type_t enumeration into string
 */
void get_obj_type_str(H5O_type_t obj_type, char **out);

void init_metadata_collector(metadata_collector_t *meta_coll,
    int num_objs, void *opdata, h5object_t *obj_list_head,
    int (*on_obj)(void *opdata, h5object_t *obj),
    int (*on_attr)(void *opdata, h5attribute_t *attr)
    );

void de_init_metadata_collector(metadata_collector_t *meta_coll) ;

void init_h5object(h5object_t *h5object,
    void *opdata, hid_t obj_id, char *obj_name, H5O_info_t *obj_info,
    int num_attrs, h5attribute_t *attr_list_head, 
    int (*on_attr)(void *opdata, h5attribute_t *attr),
    int (*on_sub_obj)(void *opdata, h5object_t *obj));

void pretty_print_metadata(metadata_collector_t *meta_coll);

size_t get_hdf5_meta_size();


#endif /* !MIQS_NEW_HDF5_META_H */