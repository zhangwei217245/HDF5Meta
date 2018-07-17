#include "query_utils.h"
#include "hdf5.h"


void print_usage() {
    printf("Usage: ./test_unindexed_hdf5 /path/to/hdf5/file <num_files>\n");
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


int scan_hdf5(char *file_path) {
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
    status = H5Ovisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func, NULL);

    /*
     * Repeat the same process using H5Lvisit
     */
    printf ("\nLinks in the file:\n");
    status = H5Lvisit (file, H5_INDEX_NAME, H5_ITER_NATIVE, op_func_L, NULL);

    /*
     * Close and release resources.
     */
    status = H5Fclose (file);
}

int 
main(int argc, char const *argv[])
{

    if (argc < 3) {
        print_usage();
    }
    
    char *file_path = argv[1];
    char *_str_num_file = argv[2];

    int num_file = atoi(_str_num_file);

    int i = 0;

    for (i = 0; i < num_file; i++) {
        scan_hdf5(file_path);
    }

    return 0;
}


/************************************************************

  Operator function for H5Ovisit.  This function prints the
  name and type of the object passed to it.

 ************************************************************/
herr_t op_func (hid_t loc_id, const char *name, const H5O_info_t *info,
            void *operator_data)
{
    printf ("/");               /* Print root group in object path */

    /*
     * Check if the current object is the root group, and if not print
     * the full path name and type.
     */
    if (name[0] == '.')         /* Root group, do not print '.' */
        printf ("  (Group)\n");
    else
        switch (info->type) {
            case H5O_TYPE_GROUP:
                printf ("%s  (Group)\n", name);
                break;
            case H5O_TYPE_DATASET:
                printf ("%s  (Dataset)\n", name);
                break;
            case H5O_TYPE_NAMED_DATATYPE:
                printf ("%s  (Datatype)\n", name);
                break;
            default:
                printf ("%s  (Unknown)\n", name);
        }

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

