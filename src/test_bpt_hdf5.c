#include "../lib/hdf5_meta.h"
#include "../lib/art.h"
#include "../lib/bplustree.h"
#include <math.h>


typedef struct op_data_struct {
    char **indexed_attr;
    char **search_values;
    int *search_types;
    int num_indexed_attr;
    int file_id;
    int bitmap_int64_arr_len; // floor(total number of files/64)
} op_data_struct_t;

typedef struct {
    int is_numeric;
    void *bpt;
    void *art;
}art_leaf_content_t;

void print_usage() {
    printf("Usage: ./test_bpt_hdf5 /path/to/hdf5/file <num_files> <num_indexed_attr>\n");
}

/*
 * Operator function to be called by H5Ovisit.
 */
herr_t op_func (hid_t loc_id, const char *name, const H5O_info_t *info,
            void *operator_data);

/*
 * Operator function to be called by H5Lvisit.
 */
herr_t op_func_L (hid_t loc_id, const char *name, const H5L_info_t *info,
            void *operator_data);

static herr_t attr_info(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata);
                                     
int scan_hdf5(char *file_path, void *opdata);

void perform_search(int seed, void *opdata);

int get_bitmap_int64_arr_len(int num_files) {
    int bitmap_int64_arr_len = (int)floor((double)num_files/(double)(sizeof(uint64_t)*8));
    return bitmap_int64_arr_len;
}

int get_bitmap_seg_index_by_file_id(int file_id) {
    return file_id/(sizeof(uint64_t)*8);
}
uint64_t get_file_mask(int file_id) {
    uint64_t bit_shift = (uint64_t)(file_id % (sizeof(uint64_t)*8));
    uint64_t bit_mask = 1;
    bit_mask = bit_mask << bit_shift;
    return bit_mask;
}

void set_file_bit(uint64_t *bitmap, int file_id){
    int seg_idx = get_bitmap_seg_index_by_file_id(file_id);
    uint64_t seg = bitmap[seg_idx];
    bitmap[seg_idx] = seg ^ get_file_mask(file_id);
}

int get_num_ones_in_bitmap(uint64_t *bitmap, int bitmap_int64_arr_len){
    int i = 0;
    int rst = 0;
    for (i = 0; i < bitmap_int64_arr_len; i++) {
        rst+=__builtin_popcount(bitmap[i]);
    }
    return rst;
}

struct bplus_tree *new_bplus_tree(char *filename, int block_size) {
    struct bplus_tree * newbpt = bplus_tree_init(filename, block_size);
    return newbpt;
}

art_tree *global_art;



int 
main(int argc, char const *argv[])
{
    if (argc < 4) {
        print_usage();
    }
    
    char *file_path = argv[1];
    char *_str_num_file = argv[2];

    char *indexed_attr[]={"COLLA", "DARKTIME", "BADPIXEL", "FILENAME", "EXPOSURE", "COLLB", NULL};
    char *search_values[]={"27089", "0", "badpixels-56149-b1.fits.gz", "sdR-b2-00154990.fit", "155701", "26660", NULL};
    int search_types[] = {1,1,0,0,1,1,0};
    
    op_data_struct_t opdata;
    opdata.indexed_attr = indexed_attr;
    opdata.search_values = search_values;
    opdata.search_types = search_types;
    opdata.num_indexed_attr = 1;
    
    int num_file = 2500;

    if (_str_num_file != NULL) {
        num_file = atoi(_str_num_file);
    }
    if (argv[3] != NULL) {
        opdata.num_indexed_attr = atoi(argv[3]);
        if (opdata.num_indexed_attr > 6) opdata.num_indexed_attr = 6;
    }

    opdata.bitmap_int64_arr_len = get_bitmap_int64_arr_len(num_file);
    //INIT ART;

    global_art = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(global_art);

    int i = 0;
    // Create array of multiple files, for locating purpose
    char **file_path_arr = (char **)calloc(num_file+1, sizeof(char *));

    for (i = 0; i < num_file; i++) {
        stopwatch_t timer_create;
        timer_start(&timer_create);

        //simulate scan over all files, recording all file names
        file_path_arr[i] = (char *)calloc(256, sizeof(char));
        sprintf(file_path_arr[i], "%s%d", file_path, i);

        opdata.file_id = i;

        // scan file and build index
        scan_hdf5(file_path, &opdata);

        timer_pause(&timer_create);

        printf("Time to index file %d is %d microseconds.\n", i, timer_delta_us(&timer_create));
    }

    for (i = 0; i < 1000; i++) {

        stopwatch_t timer_search;
        timer_start(&timer_search);

        perform_search(i, &opdata);

        timer_pause(&timer_search);

        printf("Time to %d search is %d microseconds.\n", i, timer_delta_us(&timer_search));
    }

    return 0;
}

int scan_hdf5(char *file_path, void *opdata) {
    hid_t           file;           /* Handle */
    herr_t          status;

    /*
     * Open file
     */
    file = H5Fopen (file_path, H5F_ACC_RDONLY, H5P_DEFAULT);
    
    /*
     * Begin iteration using H5Ovisit
     */
    printf ("Objects in the file:\n");
    status = H5Ovisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func, opdata);

    /*
     * Repeat the same process using H5Lvisit
     */
    printf ("\nLinks in the file:\n");
    status = H5Lvisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func_L, opdata);

    /*
     * Close and release resources.
     */
    status = H5Fclose (file);
}

/************************************************************

  Operator function for H5Ovisit.  This function prints the
  name and type of the object passed to it.

 ************************************************************/
herr_t op_func (hid_t loc_id, const char *name, const H5O_info_t *info,
            void *operator_data)
{
    char obj_type[20];

    H5O_info_t object_info;
    hid_t curr_obj_id = H5Oopen(loc_id, name, H5P_DEFAULT);

    switch (info->type) {
        case H5O_TYPE_GROUP:
            sprintf (obj_type, "GROUP");
            break;
        case H5O_TYPE_DATASET:
            sprintf (obj_type, "DATASET");
            break;
        case H5O_TYPE_NAMED_DATATYPE:
            sprintf (obj_type, "NAMED_DATATYPE");
            break;
        default:
            sprintf (obj_type, "UNKNOWN");
    }

    // printf ("| %s : /", obj_type);               /* Print root group in object path */

    /*
     * Check if the current object is the root group, and if not print
     * the full path name and type.
     */
    if (name[0] != '.'){
        // printf("%s", name);
    } 
    // printf(" | \n");     

    int na = H5Aget_num_attrs(curr_obj_id);
    if (na > 0) {
        // printf ("\n%d Attributes are:\n", na);
        H5Aiterate(curr_obj_id, H5_INDEX_CRT_ORDER, H5_ITER_NATIVE, NULL, attr_info, operator_data);
    }

    H5Oclose(curr_obj_id);
    return 0;
}


/************************************************************

  Operator function for H5Lvisit.  This function simply
  retrieves the info for the object the current link points
  to, and calls the operator function for H5Ovisit.

 ************************************************************/
herr_t op_func_L (hid_t loc_id, const char *name, const H5L_info_t *info,
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
attr_info(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata_p)
{
    hid_t attr, atype, aspace, str_type;  /* Attribute, datatype, dataspace, string_datatype identifiers */
    char  *string_out[100];
    char  *char_out;
    int   rank;
    hsize_t *sdim; 
    herr_t ret;
    int i, j ;
    size_t size, totsize;
    size_t npoints;             /* Number of elements in the array attribute. */ 
    int *point_out;    
    float *float_array;         /* Pointer to the array attribute. */
    H5S_class_t  class;

    /* avoid warnings */
    op_data_struct_t *opdata = (op_data_struct_t *)opdata_p;
    char *indexed_attr_name = NULL;

    // printf("file_id = %d\n", opdata->file_id);
    int file_id = opdata->file_id;
    int bitmap_int64_arr_len = opdata->bitmap_int64_arr_len;

    for (i = 0; i < opdata->num_indexed_attr; i++) {
        indexed_attr_name = opdata->indexed_attr[i];
        if (indexed_attr_name != NULL && strcmp(indexed_attr_name, name)==0) {
            // printf("Attr to be indexed : %s\n", indexed_attr_name);
            break;
        } else {
            return 0;
        }
    }

    art_leaf_content_t *leaf_cnt = (art_leaf_content_t *)art_search(global_art, name, strlen(name));
    if (leaf_cnt == NULL){
        leaf_cnt = (art_leaf_content_t *)calloc(1, sizeof(art_leaf_content_t));
        art_insert(global_art, name, strlen(name), leaf_cnt);
    }

    // printf("%p", string_out);

    /*  Open the attribute using its name.  */    
    attr = H5Aopen_name(loc_id, name);

    /*  Display attribute name.  */
    // printf("\t| %-*s", 12, name);

    /* Get attribute datatype, dataspace, rank, and dimensions.  */
    atype  = H5Aget_type(attr);
    aspace = H5Aget_space(attr);
    rank = H5Sget_simple_extent_ndims(aspace);
    /* Display rank and dimension sizes for the array attribute.  */
    if (rank > 0) {
        sdim = (hsize_t *)calloc(rank, sizeof(hsize_t));
        ret = H5Sget_simple_extent_dims(aspace, sdim, NULL);
        // printf("Rank : %d \n", rank); 
        // printf("Dimension sizes : ");
        // for (i=0; i< rank; i++) printf("%d ", (int)sdim[i]);
        // printf("\n");
    }
    
    /* Get dataspace type */
    class = H5Sget_simple_extent_type (aspace);
    // printf ("H5Sget_simple_extent_type (aspace) returns: %i\n", class);
    // printf(" | Class: %i", class);
    npoints = H5Sget_simple_extent_npoints(aspace);
    // printf(" | npoints: %d", npoints);

    if (H5T_INTEGER == H5Tget_class(atype)) {
    //    printf(" | %-*s| ", 10, "<INTEGER>");
       point_out = (int *)calloc(npoints, sizeof(int));
       ret  = H5Aread(attr, atype, point_out);
        
        leaf_cnt->is_numeric = 1;
        if (leaf_cnt->bpt == NULL) {
            leaf_cnt->bpt = new_bplus_tree(name, 4096);
        }

       for (i = 0; i < npoints; i++) {
            // printf("%d\t",point_out[i]);
            int k = point_out[i] + file_id;
            uint64_t *v = (uint64_t *)bplus_tree_get(leaf_cnt->bpt, k);
            if (v == NULL || v == 0 || v == 0xffffffffffffffff ) {
                v = (uint64_t *)calloc(bitmap_int64_arr_len, sizeof(uint64_t));
                //TODO: need to change the b+tree implementation so that value is a generic pointer.
                bplus_tree_put(leaf_cnt->bpt, k, (long)v);
            }
            set_file_bit(v, file_id);
        }
       free(point_out);
    }

    if (H5T_FLOAT == H5Tget_class(atype)) {
    //    printf(" | %-*s| ", 10, "<FLOAT>");
       float_array = (float *)malloc(sizeof(float)*(int)npoints); 
       ret = H5Aread(attr, atype, float_array);

        leaf_cnt->is_numeric = 1;
        if (leaf_cnt->bpt == NULL) {
            leaf_cnt->bpt = new_bplus_tree(name, 4096);
        }

       for( i = 0; i < (int)npoints; i++) {
        //    printf("%f\t", float_array[i]);
           int k = (int)float_array[i];
           k+=file_id;
            uint64_t *v = (uint64_t *)bplus_tree_get(leaf_cnt->bpt, k);
            if (v == NULL || v == 0 || v == 0xffffffffffffffff) {
                v = (uint64_t *)calloc(bitmap_int64_arr_len, sizeof(uint64_t));
                //TODO: need to change the b+tree implementation so that value is a generic pointer.
                bplus_tree_put(leaf_cnt->bpt, k, (long)v);
            }
            set_file_bit(v, file_id);
        }
       free(float_array);
    }

    if (H5T_STRING == H5Tget_class (atype)) {
        size = H5Tget_size (atype);
        
        totsize = size*npoints;

        str_type = atype;

        leaf_cnt->is_numeric = 0;
        if (leaf_cnt->art == NULL) {
            leaf_cnt->art = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)leaf_cnt->art);
        }

        if(H5Tis_variable_str(atype) == 1) {
            // printf(" | %-*s| ", 10, "<VARCHAR>");
            str_type = H5Tget_native_type(atype, H5T_DIR_ASCEND);
            ret = H5Aread(attr, str_type, &string_out);
            for (i=0; i<npoints; i++) {
                // printf ("%s ", string_out[i]);
                char tmp_key[100];
                sprintf(tmp_key,"%d%s", file_id, string_out[i]);
                char *key = tmp_key;
                uint64_t *v = (uint64_t *)art_search(leaf_cnt->art, key, strlen(key));
                if (v == NULL || v == 0) {
                    v = (uint64_t *)calloc(bitmap_int64_arr_len, sizeof(uint64_t));
                    art_insert(leaf_cnt->art, key, strlen(key), v);
                }
                set_file_bit(v, file_id);
                // free(string_out[i]);
            }
        } else {
            // char tmp[20];
            // sprintf(tmp, "<CHAR(%d)>", totsize);
            // printf ("| %-*s |\t", 10, tmp);
            char_out = calloc(totsize+1, sizeof(char));
            ret = H5Aread(attr, str_type, char_out);
            // printf("%s", char_out);

            char tmp_key[100];
            sprintf(tmp_key,"%d%s", file_id, char_out);
            char *key = tmp_key;
            uint64_t *v = (uint64_t *)art_search(leaf_cnt->art, key, strlen(key));
            if (v == NULL || v == 0) {
                v = (uint64_t *)calloc(bitmap_int64_arr_len, sizeof(uint64_t));
                art_insert(leaf_cnt->art, key, strlen(key), v);
            }
            set_file_bit(v, file_id);
        }
    }

    ret = H5Tclose(atype);
    ret = H5Sclose(aspace);
    ret = H5Aclose(attr);
    // printf("|\n");

    return 0;
}


void perform_search(int seed, void *opdata_p){
    op_data_struct_t *opdata = (op_data_struct_t *)opdata_p;
    int pos = seed % opdata->num_indexed_attr;
    char *attr = opdata->indexed_attr[pos];
    art_leaf_content_t *art_leaf = (art_leaf_content_t *)art_search(global_art, attr, strlen(attr));
    if (art_leaf != NULL) {
        if (art_leaf->is_numeric) {
            int v_query = atoi(opdata->search_values[pos]) + seed;
            uint64_t *bitmap = bplus_tree_get(art_leaf->bpt, v_query);
            int num_matched_files = get_num_ones_in_bitmap(bitmap, opdata->bitmap_int64_arr_len);
            printf("%d Matched files on query %s=%d.\n", num_matched_files, attr, v_query);
        } else {
            char tmp_v_query[100];
            sprintf(tmp_v_query, "%d%s", seed, opdata->search_values[pos]);
            char *v_query = tmp_v_query;
            uint64_t *bitmap = art_search(art_leaf->art, v_query, strlen(v_query));
            int num_matched_files = get_num_ones_in_bitmap(bitmap, opdata->bitmap_int64_arr_len);
            printf("%d Matched files on query %s=%s.\n", num_matched_files, attr, v_query);
        }
    }
    return;
}