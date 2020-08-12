
/* File foo.  */
#ifndef MIQS_META_EXTRACTOR_H
#define MIQS_META_EXTRACTOR_H


#include "miqs_metadata.h"


typedef struct miqs_metadata_collector {
    miqs_data_object_t *object_linked_list;
    int num_objs;
    int total_num_attrs;
    void *opdata;
    int (*on_obj)(void *opdata, miqs_data_object_t *obj);
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr);
} miqs_metadata_collector_t;



void init_metadata_collector(miqs_metadata_collector_t *meta_coll,
    int num_objs, void *opdata, miqs_data_object_t *obj_list_head,
    int (*on_obj)(void *opdata, miqs_data_object_t *obj),
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr)
    );

void de_init_metadata_collector(miqs_metadata_collector_t *meta_coll) ;

void init_data_object(miqs_data_object_t *h5object,
    void *opdata, void *obj_id, char *obj_name, void **obj_info,
    int num_attrs, miqs_meta_attribute_t *attr_list_head, 
    int (*on_attr)(void *opdata, miqs_meta_attribute_t *attr),
    int (*on_sub_obj)(void *opdata, miqs_data_object_t *obj));

// void pretty_print_metadata(miqs_metadata_collector_t *meta_coll);




#endif /* !MIQS_META_EXTRACTOR_H */