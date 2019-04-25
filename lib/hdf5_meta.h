#ifndef HDF5_META_H
#define HDF5_META_H

#include "utils/query_utils.h"
#include "utils/timer_utils.h"
#include "hdf5.h"
#include "json-c/json.h"

/***************************************************************************/
/* This one follows the old HDF5 metadata reader and translate HDF5        */
/* metadata into JSON format. However, with such hierarchical access path, */
/* Maintaining the relationship between HDF5 parent and child is difficult */
/***************************************************************************/


#define MAX_NAME 1024
#define MAX_TAG_LEN 16384

void parse_hdf5_meta_as_json_str(char *filepath, char **result);

void parse_hdf5_file(char *filepath, json_object *rootobj);

/*
 * Process a group and all it's members
 *
 *   This can be used as a model to implement different actions and
 *   searches.
 */

void
scan_group(hid_t gid, json_object *group_obj);


/*
 *  Retrieve information about a dataset.
 *
 *  Many other possible actions.
 *
 *  This example does not read the data of the dataset.
 */
void
do_dset(hid_t did, char *name, json_object *current_object);


/*
 *  Analyze a data type description
 */
void
do_dtype(hid_t tid, hid_t oid, int is_compound, char *key_name, json_object *jsonobj);

/*
 *  Analyze a symbolic link
 *
 * The main thing you can do with a link is find out
 * what it points to.
 */
void
do_link(hid_t gid, char *name, json_object *current_object);


/*
 *  Run through all the attributes of a dataset or group.
 *  This is similar to iterating through a group.
 */
void
scan_attrs(hid_t oid, json_object *attributes_obj);

/*
 *  Process one attribute.
 *  This is similar to the information about a dataset.
 */
void do_attr(hid_t aid, json_object *attributes_obj);

/*
 *   Example of information that can be read from a Dataset Creation
 *   Property List.
 *
 *   There are many other possibilities, and there are other property
 *   lists.
 */
void
do_plist(hid_t pid);

#endif //HDF5_META_H