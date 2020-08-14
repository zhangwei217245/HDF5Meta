#include "miqs_meta_collector.h"

void init_metadata_collector(miqs_metadata_collector_t *meta_coll,
    int num_objs, void *opdata, miqs_data_object_t *obj_list_head,
    int (*on_obj)(void *opdata, miqs_data_object_t *obj),
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr)
    ){
    meta_coll->num_objs=num_objs;
    meta_coll->opdata = opdata;
    meta_coll->object_linked_list = obj_list_head;
    meta_coll->on_obj = on_obj;
    meta_coll->on_attr = on_attr;
}

void de_init_metadata_collector(miqs_metadata_collector_t *meta_coll) {
    miqs_data_object_t *curr_obj = meta_coll->object_linked_list;
    while (curr_obj) {
        miqs_data_object_t *_tempobj = curr_obj;
        free(curr_obj->obj_name);
        free(curr_obj->obj_info);
        miqs_meta_attribute_t *curr_attr = curr_obj->attr_linked_list;
        while (curr_attr) {
            free(curr_attr->attribute_value);
            miqs_meta_attribute_t *_temp_attr = curr_attr;
            curr_attr = curr_attr->next;
            free(_temp_attr);
        }
        curr_obj = curr_obj->next;
        free(_tempobj);
    }
}

void init_data_object(miqs_data_object_t *data_obj,
    void *opdata, void *obj_id, char *obj_name, void **obj_info,
    int num_attrs, miqs_meta_attribute_t *attr_list_head, 
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr),
    int (*on_sub_obj)(void *opdata, miqs_data_object_t *obj)){

    data_obj->opdata = opdata;
    data_obj->obj_id = obj_id;
    data_obj->obj_name = obj_name;
    data_obj->obj_info = obj_info;
    data_obj->obj_type = MIQS_OT_UNKNOWN;
    data_obj->num_attrs = num_attrs;
    data_obj->attr_linked_list = attr_list_head;
    data_obj->on_attr = on_attr;

    data_obj->sub_obj_list = NULL;
    data_obj->num_sub_objs=0;
    data_obj->on_sub_obj = on_sub_obj;

    data_obj->next = NULL;
    data_obj->head = NULL;
    data_obj->tail = NULL;
}

// void pretty_print_metadata(miqs_metadata_collector_t *meta_coll){
//     printf("Unsupported implementation!\n");
// }



