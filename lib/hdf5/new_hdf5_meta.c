#include "new_hdf5_meta.h"

/**
 * Operator function to be called by H5Ovisit.
 */
static herr_t op_func (hid_t loc_id, const char *name, const H5O_info_t *info,
            void *operator_data);

/**
 * Operator function to be called by H5Lvisit.
 */
static herr_t op_func_L (hid_t loc_id, const char *name, const H5L_info_t *info,
            void *operator_data);

/**
 * Operator function to be called by H5Aiterate2.
 */
static herr_t attr_info(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata);

static herr_t read_int_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr);

static herr_t read_float_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr);

static herr_t read_string_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr);

// h5object_t get_h5obj_from_meta_coll(metadata_collector_t *meta_coll, int index){
//     if (index >= meta_coll->num_objs || index < 0) {
//         printf("Index %d out of bound for meta_coll of size %d \n", index, meta_coll->num_objs);
//         exit(1);
//     }
//     return meta_coll->h5obj_data[index];
// }

int scan_hdf5(char *file_path, metadata_collector_t *meta_collector, int is_visit_link){
    hid_t           file;           /* Handle */
    herr_t          status;

    if (meta_collector == NULL) {
        perror("The metadata collector is NULL. \n");
        return -1;
    }

    /**
     * Open file
     */
    file = H5Fopen (file_path, H5F_ACC_RDONLY, H5P_DEFAULT);

    /**
     * Begin iteration using H5Ovisit
     */
    // printf("Objects in the file:\n");
    status = H5Ovisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func, meta_collector);

    if (is_visit_link) {
        /**
         * Repeat the same process using H5Lvisit
         */
        // printf("Links in the file:\n");
        status = H5Lvisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func_L, meta_collector);
    }

    /**
     * Close and release resources.
     */
    status = H5Fclose (file);
}

/************************************************************

  Operator function for H5Ovisit.  This function prints the
  name and type of the object passed to it.

 ************************************************************/
static herr_t op_func (hid_t loc_id, const char *name, const H5O_info_t *info,
            void *operator)
{
    metadata_collector_t *meta_coll = (metadata_collector_t *)operator;

    h5object_t *object = (h5object_t *)calloc(1,sizeof(h5object_t));

    object->obj_id = H5Oopen(loc_id, name, H5P_DEFAULT);

    object->obj_name = (char *)calloc(strlen(name)+1, sizeof(char));
    sprintf(object->obj_name, "%s", name);

    H5O_info_t *obj_info = (H5O_info_t *)calloc(1, sizeof(H5O_info_t));
    H5Oget_info(object->obj_id, obj_info);
    // TODO: try H5Oget_info2
    object->obj_info = obj_info;
    object->num_attrs = H5Aget_num_attrs(object->obj_id);
    // println("obj : %s", object->obj_name);

    if (meta_coll->on_attr != NULL) {
        object->on_attr = meta_coll->on_attr;
        object->opdata = meta_coll->opdata;
    }

    if (object->num_attrs > 0) {
        H5Aiterate(object->obj_id, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, NULL, attr_info, object);
    }

    h5object_t *h5object_list_head = meta_coll->object_linked_list;

    if (h5object_list_head == NULL) {
        h5object_list_head = object;
    } else {
        h5object_t *h5object_list_tail = h5object_list_head->tail;
        if (h5object_list_tail==NULL) {
            // Only one head element; We append new element to the next pointer of head, and let tail equals to its next;
            h5object_list_head->next = object;
            h5object_list_head->tail = object;
        } else {
            h5object_list_tail->next = object;
            h5object_list_head->tail = object;
        }
        object->head = h5object_list_head;
    }
    meta_coll->object_linked_list = h5object_list_head;
    meta_coll->num_objs+=1;

    // calling on_obj function of metadata collector. The on_obj function is a user-defined operation.
    if (meta_coll -> on_obj != NULL) {
        int udf_rst = meta_coll->on_obj(meta_coll->opdata, object);
        if (udf_rst < 0) {
            fprintf(stderr, "user defined function got problem when collecting object %s\n", name);
            fflush(stderr);
        }
    }

    H5Oclose(object->obj_id);
    return 0;
}

/************************************************************

  Operator function for H5Lvisit.  This function simply
  retrieves the info for the object the current link points
  to, and calls the operator function for H5Ovisit.

 ************************************************************/
static herr_t op_func_L (hid_t loc_id, const char *name, const H5L_info_t *info,
            void *operator_data)
{
    herr_t          status;
    H5O_info_t      infobuf;

    /*
     * Get type of the object and display its name and type.
     * The name of the object is passed to this function by
     * the Library.
     */
    status = H5Oget_info_by_name (loc_id, name, &infobuf, H5P_DEFAULT);
    return op_func (loc_id, name, &infobuf, operator_data);
}

/*
 * Operator function.
 */
static herr_t 
attr_info(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *h5obj)
{
    hid_t attr, atype, aspace, str_type;  /* Attribute, datatype, dataspace, string_datatype identifiers */
    herr_t ret;
    H5S_class_t  class;
    size_t npoints;            
    
    h5attribute_t *curr_attr = (h5attribute_t *)calloc(1,sizeof(h5attribute_t));

    /*  Open the attribute using its name.  */    
    attr = H5Aopen_name(loc_id, name);
    atype  = H5Aget_type(attr);
    aspace = H5Aget_space(attr);
    npoints = H5Sget_simple_extent_npoints(aspace);

    H5T_class_t attr_type = H5Tget_class(atype);

    curr_attr->attr_name = (char *)calloc(strlen(name)+1, sizeof(char));
    curr_attr->next = NULL;
    sprintf(curr_attr->attr_name, "%s", name);

    curr_attr->attr_type = attr_type;

    switch(attr_type) {
        case H5T_INTEGER:
            read_int_attr(npoints, attr, atype, curr_attr);
            break;
        case H5T_FLOAT:
            read_float_attr(npoints, attr, atype, curr_attr);
            break;
        case H5T_STRING:
            read_string_attr(npoints, attr, atype, curr_attr);
            break;
        default:
            // printf("Ignore unsupported attr_type for attribute %s\n", name);
            break;
    }
    // println("attr: %s", name);
    // Append attribute element to object attribute list.
    h5object_t *h5object = (h5object_t *)h5obj;

    h5attribute_t *h5attr_list_head = h5object->attr_linked_list;

    if (h5attr_list_head == NULL) {
        h5attr_list_head = curr_attr;
    } else {
        h5attribute_t *h5attr_list_tail = h5attr_list_head->tail;
        if (h5attr_list_tail==NULL) {
            // Only one head element; We append new element to the next pointer of head, and let tail equals to its next;
            h5attr_list_head->next = curr_attr;
            h5attr_list_head->tail = curr_attr;
        } else {
            h5attr_list_tail->next = curr_attr;
            h5attr_list_head->tail = curr_attr;
        }
        curr_attr->head = h5attr_list_head;
    }
    h5object->attr_linked_list = h5attr_list_head;
    h5object->num_attrs+=1;

    // Call user defined function on attribute:
    if (h5object -> on_attr != NULL) {
        int udf_rst = h5object->on_attr(h5object->opdata, curr_attr);
        if (udf_rst < 0) {
            fprintf(stderr, "user defined function got problem when iterating %s->%s\n", h5object->obj_name, curr_attr->attr_name);
            fflush(stderr);
        }
    }

    ret = H5Tclose(atype);
    ret = H5Sclose(aspace);
    ret = H5Aclose(attr);

    return 0;
}

static herr_t read_int_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr) {
    int *out = (int *)calloc(npoints, sizeof(int));
    herr_t ret = H5Aread(attr, atype, out);
    curr_attr->attribute_value = out;
    curr_attr->attribute_value_length = npoints;
    return ret;
}

static herr_t read_float_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr) {
    double *out = (double *)calloc(npoints, sizeof(double)); 
    herr_t ret = H5Aread(attr, atype, out);
    curr_attr->attribute_value = out;
    curr_attr->attribute_value_length = npoints;
    return ret;
}

static herr_t read_string_attr(int npoints, hid_t attr, hid_t atype, h5attribute_t *curr_attr){

    herr_t ret;
    char  **string_out;
    char  **char_out;

    size_t size = H5Tget_size (atype);
    size_t totsize = size*npoints;

    hid_t str_type = atype;

    if(H5Tis_variable_str(str_type) == 1) {
        str_type = H5Tget_native_type(atype, H5T_DIR_ASCEND);
        char *tempout[100];
        ret = H5Aread(attr, str_type, &tempout);
        string_out = (char **)calloc(npoints, sizeof(char *));//string_out;
        int i  = 0;
        for (i = 0; i < npoints; i++) {
            string_out[i] = tempout[i];
        }
        curr_attr->attribute_value = string_out;
        curr_attr->attribute_value_length = npoints;
    } else {
        char *tempout = (char *)calloc(totsize+1, sizeof(char));
        ret = H5Aread(attr, str_type, tempout);
        char_out = (char **)calloc(1, sizeof(char *));
        char_out[0] = tempout;
        curr_attr->attribute_value = char_out;
        curr_attr->attribute_value_length=1;
    }
    return ret;
}

void get_obj_type_str(H5O_type_t obj_type, char **out){
    *out = (char *)calloc(20, sizeof(char));
    switch (obj_type) {
        case H5O_TYPE_GROUP:
            sprintf (*out, "GROUP");
            break;
        case H5O_TYPE_DATASET:
            sprintf (*out, "DATASET");
            break;
        case H5O_TYPE_NAMED_DATATYPE:
            sprintf (*out, "NAMED_DATATYPE");
            break;
        default:
            sprintf (*out, "UNKNOWN");
    }
}

void init_metadata_collector(metadata_collector_t *meta_coll,
    int num_objs, void *opdata, h5object_t *obj_list_head,
    int (*on_obj)(void *opdata, h5object_t *obj),
    int (*on_attr)(void *opdata, h5attribute_t *attr)
    ){
    meta_coll->num_objs=num_objs;
    meta_coll->opdata = opdata;
    meta_coll->object_linked_list = obj_list_head;
    meta_coll->on_obj = on_obj;
    meta_coll->on_attr = on_attr;
}

void de_init_metadata_collector(metadata_collector_t *meta_coll) {
    h5object_t *curr_obj = meta_coll->object_linked_list;
    while (curr_obj) {
        h5object_t *_tempobj = curr_obj;
        free(curr_obj->obj_name);
        free(curr_obj->obj_info);
        h5attribute_t *curr_attr = curr_obj->attr_linked_list;
        while (curr_attr) {
            free(curr_attr->attribute_value);
            h5attribute_t *_temp_attr = curr_attr;
            curr_attr = curr_attr->next;
            free(_temp_attr);
        }
        curr_obj = curr_obj->next;
        free(_tempobj);
    }
}

void init_h5object(h5object_t *h5object,
    void *opdata, hid_t obj_id, char *obj_name, H5O_info_t *obj_info,
    int num_attrs, h5attribute_t *attr_list_head, 
    int (*on_attr)(void *opdata, h5attribute_t *attr),
    int (*on_sub_obj)(void *opdata, h5object_t *obj)){

    h5object->opdata = opdata;
    h5object->obj_id = obj_id;
    h5object->obj_name = obj_name;
    h5object->obj_info = obj_info;
    h5object->num_attrs = num_attrs;
    h5object->attr_linked_list = attr_list_head;
    h5object->on_attr = on_attr;

    h5object->sub_obj_list = NULL;
    h5object->num_sub_objs=0;
    h5object->on_sub_obj = on_sub_obj;

    h5object->next = NULL;
    h5object->head = NULL;
    h5object->tail = NULL;
}

void pretty_print_metadata(metadata_collector_t *meta_coll){
    printf("Unsupported implementation!\n");
}

