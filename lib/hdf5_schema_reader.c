#include "query_utils.h"
#include "hdf5.h"

void
scan_group(hid_t gid) {
    int i;
    ssize_t len;
    hsize_t nobj;
    herr_t err;
    int otype;
    hid_t grpid, typeid, dsid;
    char group_name[MAX_NAME];
    char memb_name[MAX_NAME];

        /*
         *          * Information about the group:
         *                   *  Name and attributes
         *                            *
         *                                     *  Other info., not shown here: number of links, object id
         *                                              */
    len = H5Iget_name (gid, group_name, MAX_NAME);

    /* printf("Group Name: %s\n",group_name); */

    /*
     *   *  process the attributes of the group, if any.
     *       */
    scan_attrs(gid);

        /*
         *          *  Get all the members of the groups, one at a time.
         *                   */
    err = H5Gget_num_objs(gid, &nobj);
    for (i = 0; i < nobj; i++) {
                    /*
                     *                  *  For each object in the group, get the name and
                     *                                   *   what type of object it is.
                     *                                                    */
            /* printf("  Member: #%d ",i);fflush(stdout); */
            len = H5Gget_objname_by_idx(gid, (hsize_t)i,
                                memb_name, (size_t)MAX_NAME );
            /* printf("   len=%d ",len);fflush(stdout); */
            /* printf("  Member: \"%s\" ",memb_name);fflush(stdou
             * t); */
            otype =  H5Gget_objtype_by_idx(gid, (size_t)i );
    
                    /*
                     *                  * process each object according to its type
                     *                                   */
            switch(otype) {
                        case H5G_LINK:
                            /* printf(" SYM_LINK:\n"); */
                            do_link(gid,memb_name);
                            break;
                        case H5G_GROUP:
                            /* printf(" GROUP:\n"); */
                            grpid = H5Gopen(gid,memb_name, H5P_DE
                                    FAULT);
                            scan_group(grpid);
                            H5Gclose(grpid);
                            break;
                        case H5G_DATASET:
                            /* printf(" DATASET:\n"); */
                            dsid = H5Dopen(gid,memb_name, H5P_DEF
                                    AULT);
                            do_dset(dsid, memb_name);
                            H5Dclose(dsid);
                            break;
                        case H5G_TYPE:
                            /* printf(" DATA TYPE:\n"); */
                            typeid = H5Topen(gid,memb_name, H5P_D
                                    EFAULT);
                            do_dtype(typeid, gid, 0);
                            H5Tclose(typeid);
                            break;
                        default:
                            printf(" Unknown Object Type!\n");
                            break;
                        }
    
            }
}


