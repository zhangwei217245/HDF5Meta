#include "hdf52json.h"


void parse_hdf5_file(char *filepath, json_object **out){
    int i = 0;
    json_object *json_root_object = json_object_new_object();

    json_object_object_add(json_root_object, "file", json_object_new_string(filepath));

    json_object *json_root_array = json_object_new_array();

    metadata_collector_t *meta_collector = (metadata_collector_t *)calloc(1, sizeof(metadata_collector_t));
    init_metadata_collector(meta_collector, 0, NULL, NULL, NULL, NULL);

    scan_hdf5(filepath, meta_collector, 0);

    // println("num_objs = %d", meta_collector->num_objs);

    h5object_t *curr_obj = meta_collector->object_linked_list;
    while (curr_obj) {
        json_object *curr_json_obj = json_object_new_object();

        char atime_buff[20];
        char mtime_buff[20];
        char ctime_buff[20];
        char btime_buff[20];
        char *obj_type_buff;
        
        get_obj_type_str(curr_obj->obj_info->type, &obj_type_buff);

        //FIXME: this can be ugly if you don't need to split the entire JSON object.
        json_object_object_add(curr_json_obj, "hdf5_filename", json_object_new_string(basename(filepath)));

        json_object_object_add(curr_json_obj, "name", json_object_new_string(curr_obj->obj_name));
        json_object_object_add(curr_json_obj, "type", json_object_new_string(obj_type_buff));


        strftime(atime_buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&(curr_obj->obj_info->atime)));
        strftime(mtime_buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&(curr_obj->obj_info->mtime)));
        strftime(ctime_buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&(curr_obj->obj_info->ctime)));
        //FIXME : a SIGSEGV here. Checkout what is going on. 
        strftime(btime_buff, 20, "%Y-%m-%d %H:%M:%S", localtime(&(curr_obj->obj_info->btime)));
        json_object_object_add(curr_json_obj, "atime", json_object_new_string(atime_buff));
        json_object_object_add(curr_json_obj, "mtime", json_object_new_string(mtime_buff));
        json_object_object_add(curr_json_obj, "ctime", json_object_new_string(ctime_buff));
        json_object_object_add(curr_json_obj, "btime", json_object_new_string(btime_buff));
        
        
        h5attribute_t *curr_attr = curr_obj->attr_linked_list;
        json_object *curr_json_attr_coll = json_object_new_object();
        while (curr_attr) {
            // println("attr_name:%s, attr_type:%d", curr_attr->attr_name, curr_attr->attr_type);
            if (curr_attr->attr_type == H5T_INTEGER) {
                int *int_value = (int *)curr_attr->attribute_value;
                if (curr_attr->attribute_value_length > 1) {
                    json_object *json_int_array = json_object_new_array();
                    int j = 0;
                    for (j = 0; j < curr_attr->attribute_value_length; j++) {
                        json_object_array_add(json_int_array, json_object_new_int(int_value[j]));
                    }
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_int_array);
                } else {
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_object_new_int(int_value[0]));
                }
            } else if (curr_attr->attr_type ==  H5T_FLOAT ){
                double *double_value = (double *)curr_attr->attribute_value;
                if (curr_attr->attribute_value_length > 1) {
                    json_object *json_double_array = json_object_new_array();
                    int j = 0;
                    for (j = 0; j < curr_attr->attribute_value_length; j++) {
                        json_object_array_add(json_double_array, json_object_new_double(double_value[j]));
                    }
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_double_array);
                } else {
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_object_new_double(double_value[0]));
                }
                    
            } else if (curr_attr->attr_type == H5T_STRING){
                char **string_value = (char **)curr_attr->attribute_value;
                if (curr_attr->attribute_value_length > 1) {
                    json_object *json_string_array = json_object_new_array();
                    int j = 0;
                    for (j = 0; j < curr_attr->attribute_value_length; j++) {
                        char *asv=(string_value[j]==NULL)?"":string_value[j];
                        json_object_array_add(json_string_array, json_object_new_string(asv));
                    }
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_string_array);
                } else {
                    char *asv=(string_value[0]==NULL)?"":string_value[0];
                    json_object_object_add(curr_json_attr_coll, 
        curr_attr->attr_name, json_object_new_string(asv));
                }
            } else {
                curr_attr = curr_attr->next;
                continue;
            } 
            curr_attr = curr_attr->next;
        }
        json_object_object_add(curr_json_obj, "attributes", curr_json_attr_coll);
        json_object_array_add(json_root_array, curr_json_obj);
        // println("obj = %s", curr_obj->obj_name);
        curr_obj = curr_obj->next;
    }

    json_object_object_add(json_root_object, "sub_objects", json_root_array);
    
    if (out != NULL) {
        *out = json_root_object;
    }
    de_init_metadata_collector(meta_collector);
}

void parse_hdf5_meta_as_json_str(char *filepath, char **result){
    json_object *rootObj;
    parse_hdf5_file(filepath, &rootObj);
    char *json_str = json_object_to_json_string(rootObj);
    if (result != NULL) {
        *result = json_str;
    }
}