

#include "query_utils.h"
#include "hdf5.h"

#define MAX_NAME 1024
#define MAX_TAG_LEN 16384

void do_dtype(hid_t, hid_t, int);
void do_dset(hid_t did, char *name);
void do_link(hid_t, char *);
void scan_group(hid_t);
void do_attr(hid_t);
void scan_attrs(hid_t);
void do_plist(hid_t);