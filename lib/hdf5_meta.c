#include "hdf5_meta.h"




void parse_hdf5_file(char *filepath, json_object *rootobj){
    hid_t    file;
    hid_t    grp;
    herr_t   status;

    file = H5Fopen(filepath, H5F_ACC_RDWR, H5P_DEFAULT);
    
    json_object_object_add(rootobj, "object_path", json_object_new_string(filepath));
    json_object_object_add(rootobj, "type", json_object_new_string("file"));
    json_object *group_array = json_object_new_array();

    grp = H5Gopen(file,"/", H5P_DEFAULT);

    json_object *root_group = json_object_new_object();
    
    json_object_object_add(root_group, "object_path", json_object_new_string("/"));

    scan_group(grp, root_group);
    json_object_array_add(group_array, root_group);
    json_object_object_add(rootobj, "sub_objects", group_array);

    status = H5Fclose(file);
    printf("final json = %s\n", json_object_to_json_string(rootobj));
}

/*
 * Process a group and all it's members
 *
 *   This can be used as a model to implement different actions and
 *   searches.
 */

void
scan_group(hid_t gid, json_object *group_obj) {
    int i;
    ssize_t len;
    hsize_t nobj;
    herr_t err;
    int otype;
    hid_t grpid, typeid, dsid;
    char group_name[MAX_NAME];
    char memb_name[MAX_NAME];
    json_object_object_add(group_obj, "type", json_object_new_string("group"));
    /*
     * Information about the group:
     *  Name and attributes
     *
     *  Other info., not shown here: number of links, object id
     */
    len = H5Iget_name (gid, group_name, MAX_NAME);

    json_object_object_add(group_obj, "group_name", json_object_new_string(group_name));
    
    /* printf("Group Name: %s\n",group_name); */
    json_object *attributes = json_object_new_object();
    /*
     *  process the attributes of the group, if any.
     */
    scan_attrs(gid, attributes);

    json_object_object_add(group_obj, "attributes", attributes);

    json_object *objects_in_group = json_object_new_array();

    json_object_object_add(group_obj, "sub_objects", objects_in_group);
    /*
     *  Get all the members of the groups, one at a time.
     */
    err = H5Gget_num_objs(gid, &nobj);
    for (i = 0; i < nobj; i++) {
        /*
         *  For each object in the group, get the name and
         *   what type of object it is.
         */
        /* printf("  Member: #%d ",i);fflush(stdout); */
        len = H5Gget_objname_by_idx(gid, (hsize_t)i,
                                    memb_name, (size_t)MAX_NAME );
        /* printf("   len=%d ",len);fflush(stdout); */
        /* printf("  Member: \"%s\" ",memb_name);fflush(stdout); */
        otype =  H5Gget_objtype_by_idx(gid, (size_t)i );

        json_object *subobject = json_object_new_object();
        json_object_object_add(subobject, "object_path", json_object_new_string(memb_name));
        /*
         * process each object according to its type
         */
        switch(otype) {
        case H5G_LINK:
            /* printf(" SYM_LINK:\n"); */
            do_link(gid,memb_name, subobject);
            break;
        case H5G_GROUP:
            /* printf(" GROUP:\n"); */
            grpid = H5Gopen(gid,memb_name, H5P_DEFAULT);
            scan_group(grpid,subobject);
            H5Gclose(grpid);
            break;
        case H5G_DATASET:
            /* printf(" DATASET:\n"); */
            dsid = H5Dopen(gid,memb_name, H5P_DEFAULT);
            do_dset(dsid, memb_name, subobject);
            H5Dclose(dsid);
            break;
        case H5G_TYPE:
            /* printf(" DATA TYPE:\n"); */
            typeid = H5Topen(gid,memb_name, H5P_DEFAULT);
            do_dtype(typeid, gid, 0, memb_name, subobject);
            H5Tclose(typeid);
            break;
        default:
            printf(" Unknown Object Type!\n");
            break;
        }
        json_object_array_add(objects_in_group, subobject);
    }
}


/*
 *  Retrieve information about a dataset.
 *
 *  Many other possible actions.
 *
 *  This example does not read the data of the dataset.
 */
void
do_dset(hid_t did, char *name, json_object *current_object)
{
    hid_t tid;
    hid_t pid;
    hid_t sid;
    hsize_t size;
    char ds_name[MAX_NAME];
    char *obj_name;
    int name_len, i;
    json_object_object_add(current_object, "type", json_object_new_string("dataset"));
    // TODO: prepare tag space
    // tag_size_g = 0;
    // memset(tags_g, 0, sizeof(char)*MAX_TAG_LEN);
    // tags_ptr_g = tags_g;

    /*
     * Information about the group:
     *  Name and attributes
     *
     *  Other info., not shown here: number of links, object id
     */
    H5Iget_name(did, ds_name, MAX_NAME  );
    json_object_object_add(current_object, "dataset_name", json_object_new_string(ds_name));
    println("%s",ds_name);


    name_len = strlen(ds_name);
    for (i = name_len; i >= 0; i--) {
        if (ds_name[i] == '/') {
            obj_name = &ds_name[i+1];
            break;
        }
    }
    /* printf("[%s] {\n", obj_name); */

    // fprintf(summary_fp_g, "%s, ", ds_name);

    /*
     * Get dataset information: dataspace, data type
     */
    sid = H5Dget_space(did); /* the dimensions of the dataset (not shown) */
    tid = H5Dget_type(did);
    /* printf(" DATA TYPE:\n"); */
    println("%s",",DT:");

    do_dtype(tid, did, 0, ds_name, current_object);

    println("%s",",");

    // ndset ++;
    /*
     *  process the attributes of the dataset, if any.
     */

    json_object *attributes = json_object_new_object();
    
    scan_attrs(did, attributes);

    json_object_object_add(current_object, "attributes", attributes);
    

    /*
     * Retrieve and analyse the dataset properties
     */
    pid = H5Dget_create_plist(did); /* get creation property list */
    do_plist(pid);
    size = H5Dget_storage_size(did);
    // printf("Total space currently written in file: %d\n",(int)size);

    /*
     * The datatype and dataspace can be used to read all or
     * part of the data.  (Not shown in this example.)
     */

    /* ... read data with H5Dread, write with H5Dwrite, etc. */

    H5Pclose(pid);
    H5Tclose(tid);
    H5Sclose(sid);


    /* printf("} [%s] tag_size %d  \n========================\n%s\n========================\n\n\n", */
    /*         obj_name, tag_size_g, tags_g); */
    /* printf("size %d\n%s\n\n", tag_size_g, tags_g); */
    // fprintf(summary_fp_g, "%d\n", tag_size_g);
    // if (tag_size_g > max_tag_size_g) {
    //     max_tag_size_g = tag_size_g;
    // }
    // tag_size_g = 0;
}


/*
 *  Analyze a data type description
 */
void
do_dtype(hid_t tid, hid_t oid, int is_compound, char *key_name, json_object *jsonobj) {

    herr_t      status;
    int compound_nmember, i;
    hsize_t dims[8], ndim;
    char *mem_name;
    char attr_string[100], new_string[MAX_TAG_LEN], tmp_str[MAX_TAG_LEN];
    hsize_t size, attr_len;
    hid_t mem_type;
    hid_t atype, aspace, naive_type;
    H5T_class_t t_class, compound_class;
    t_class = H5Tget_class(tid);
    if(t_class < 0) {
        /* puts("   Invalid datatype.\n"); */
    } else {
        size = H5Tget_size (tid);
        /* printf("    Datasize %3d, type", size); */
        /*
         * Each class has specific properties that can be
         * retrieved, e.g., size, byte order, exponent, etc.
         */
        if(t_class == H5T_INTEGER) {
            /* puts(" 'H5T_INTEGER'."); */

            if (1 == is_compound) {
                sprintf(tmp_str, "I%lu,", size);
                println("%s",tmp_str);
                json_object_object_add(jsonobj, key_name, json_object_new_string(tmp_str));
            }
            else {
                int attr_int;
                status = H5Aread (oid, tid, &attr_int);
                if (status != 0) {
                    printf("==Error with H5Aread! int \n");
                    // printf("==[%s]\n", tags_g);
                }
                /* H5Aread (oid, H5T_NATIVE_INT, &attr_int); */
                sprintf(tmp_str,"%d,", attr_int);
                println("%s",tmp_str);
                json_object_object_add(jsonobj, key_name, json_object_new_int(attr_int));
            }
            /* display size, signed, endianess, etc. */
        } else if(t_class == H5T_FLOAT) {
            /* puts(" 'H5T_FLOAT'."); */
            if (1 == is_compound) {
                sprintf(tmp_str, "F%lu,", size);
                println("%s",tmp_str);
                json_object_object_add(jsonobj, key_name, json_object_new_string(tmp_str));
            }
            else {
                double attr_float;
                status = H5Aread (oid, tid, &attr_float);
                if (status != 0) {
                    printf("==Error with H5Aread! float \n");
                    // printf("==[%s]\n", tags_g);
                }
                /* H5Aread (oid, H5T_NATIVE_DOUBLE, &attr_float); */
                if (attr_float == 0) {
                    println("%s","0");
                    json_object_object_add(jsonobj, key_name, json_object_new_double(0));
                }
                else {
                    sprintf(tmp_str,"%.2f,", attr_float);
                    // Remove the trailing 0s to save space
                    for (i = strlen(tmp_str) - 2; i > 0; i--) {
                        if (tmp_str[i] == '0') {
                            tmp_str[i] = ',';
                            tmp_str[i+1] = 0;
                        }
                        else if (tmp_str[i] == '.') {
                            tmp_str[i] = ',';
                            tmp_str[i+1] = 0;
                            break;
                        }
                        else
                            break;
                    }

                    if (strlen(tmp_str) > 8) {
                        sprintf(tmp_str,"%.2E,", attr_float);
                    }
                    println("%s",tmp_str);
                    json_object_object_add(jsonobj, key_name, json_object_new_double_s(attr_float));
                }
            }
            /* display size, endianess, exponennt, etc. */
        } else if(t_class == H5T_STRING) {
            /* puts(" 'H5T_STRING'."); */

            // Only include the string in tag if it is an attribute,
            // not any strings in compound datatype
            if (is_compound == 0) {
                hsize_t totsize;
                aspace = H5Aget_space(oid);
                atype  = H5Aget_type(oid);
                ndim = H5Sget_simple_extent_ndims(aspace);
                H5Sget_simple_extent_dims(aspace, dims, NULL);
                // Deal with variable-length string
                memset(attr_string, 0, 100);
                if(H5Tis_variable_str(atype) != 1) {
                    H5Aread(oid, atype, attr_string);
                }
                else {
                    naive_type = H5Tget_native_type(atype, H5T_DIR_ASCEND);
                    H5Aread(oid, naive_type, attr_string);
                }

                println("%s",attr_string);
                println("%s",",");
                json_object_object_add(jsonobj, key_name, json_object_new_string(attr_string));
            } // End if is_compound == 0
            else {
                sprintf(tmp_str, "S%lu,", size);
                println("%s",tmp_str);
                json_object_object_add(jsonobj, key_name, json_object_new_string(attr_string));
            }

            /* display size, padding, termination, etc. */
            /* } else if(t_class == H5T_BITFIELD) { */
            /*       puts(" 'H5T_BITFIELD'."); */
            /* 	/1* display size, label, etc. *1/ */
            /* } else if(t_class == H5T_OPAQUE) { */
            /*       puts(" 'H5T_OPAQUE'."); */
            /* 	/1* display size, etc. *1/ */
        } else if(t_class == H5T_COMPOUND) {
            // For compound type, the size would be calculated by its sub-types
            /* puts(" 'H5T_COMPOUND' {"); */
            println("%s","[");
            /* recursively display each member: field name, type  */
            compound_nmember = H5Tget_nmembers(tid);
            json_object *compound_obj = json_object_new_object();

            for (i = 0; i < compound_nmember; i++) {
                mem_name = H5Tget_member_name(tid, i);
                /* printf("        Compound member [%20s]  ", mem_name); */
                println("%s",mem_name);
                println("%s","=");
                mem_type = H5Tget_member_type(tid, i);
                do_dtype(mem_type, oid, 1, mem_name, compound_obj);
            }
            /* puts("    } End 'H5T_COMPOUND'.\n"); */
            json_object_object_add(jsonobj, key_name, compound_obj);
            println("%s","]");

        } else if(t_class == H5T_ARRAY) {
            if (is_compound == 0) {
                // tag_size_g += size;
            }
            // json_object *subarray = json_object_new_array();
            ndim = H5Tget_array_ndims(tid);
            H5Tget_array_dims2(tid, dims);
            /* printf(" 'H5T_ARRAY', ndim=%d:  ", ndim); */
            // FIXME: to confirm what is the structure of this array. Should we include 'ndim' in the array?
            sprintf(tmp_str, "A%d", ndim);
            println("%s",tmp_str);
            for (i = 0; i < ndim; i++) {
                /* printf("%d, ", dims[i]); */
                sprintf(tmp_str, "_%d", dims[i]);
                println("%s",tmp_str);
                // json_object_array_add(subarray, json_object_new_int(dims[i]));
            }
            /* printf("\n                                                "); */
            //FIXME: Currently don't know what to do with this line below.
            // do_dtype(H5Tget_super(tid), oid, 1);
            
            // json_object_object_add(jsonobj, key_name, subarray);
            /* display  dimensions, base type  */
        } else if(t_class == H5T_ENUM) {
            /* puts(" 'H5T_ENUM'."); */
            sprintf(tmp_str, "E,");
            println("%s",tmp_str);
            json_object_object_add(jsonobj, key_name, json_object_new_string(tmp_str));
            /* display elements: name, value   */
        } else  {
            /* puts(" 'Other'."); */
            sprintf(tmp_str, "!OTHER!,");
            println("%s",tmp_str);
            json_object_object_add(jsonobj, key_name, json_object_new_string(tmp_str));
            /* eg. Object Reference, ...and so on ... */
        }
    }
}

/*
 *  Analyze a symbolic link
 *
 * The main thing you can do with a link is find out
 * what it points to.
 */
void
do_link(hid_t gid, char *name, json_object *current_object) {
    herr_t status;
    char target[MAX_NAME];

    json_object_object_add(current_object, "type", json_object_new_string("link"));
    json_object_object_add(current_object, "target", json_object_new_string(target));
    
    status = H5Gget_linkval(gid, name, MAX_NAME, target  ) ;
    /* printf("Symlink: %s points to: %s\n", name, target); */
}


/*
 *  Run through all the attributes of a dataset or group.
 *  This is similar to iterating through a group.
 */
void
scan_attrs(hid_t oid, json_object *attributes_obj) {
    int na;
    hid_t aid;
    int i;

    na = H5Aget_num_attrs(oid);

    for (i = 0; i < na; i++) {
        aid =	H5Aopen_idx(oid, (unsigned int)i );
        do_attr(aid, attributes_obj);
        H5Aclose(aid);
    }
}

/*
 *  Process one attribute.
 *  This is similar to the information about a dataset.
 */
void do_attr(hid_t aid, json_object *attributes_obj) {
    ssize_t len;
    hid_t atype;
    hid_t aspace;
    char buf[MAX_NAME] = {0};

    /*
     * Get the name of the attribute.
     */
    len = H5Aget_name(aid, MAX_NAME, buf );
    /* printf("    Attribute Name : %s\n",buf); */

    // Skip the COMMENT attribute
    if (strcmp("COMMENT", buf) == 0 || strcmp("comments", buf) == 0)
        return;

    println("%s",buf);
    println("%s","=");
    /*
     * Get attribute information: dataspace, data type
     */
    aspace = H5Aget_space(aid); /* the dimensions of the attribute data */

    atype  = H5Aget_type(aid);
    do_dtype(atype, aid, 0, buf, attributes_obj);

    /*
     * The datatype and dataspace can be used to read all or
     * part of the data.  (Not shown in this example.)
     */

    /* ... read data with H5Aread, write with H5Awrite, etc. */

    H5Tclose(atype);
    H5Sclose(aspace);
}

/*
 *   Example of information that can be read from a Dataset Creation
 *   Property List.
 *
 *   There are many other possibilities, and there are other property
 *   lists.
 */
void
do_plist(hid_t pid) {
    hsize_t chunk_dims_out[2];
    int  rank_chunk;
    int nfilters;
    H5Z_filter_t  filtn;
    int i;
    unsigned int   filt_flags, filt_conf;
    size_t cd_nelmts;
    unsigned int cd_values[32] ;
    char f_name[MAX_NAME];
    H5D_fill_time_t ft;
    H5D_alloc_time_t at;
    H5D_fill_value_t fvstatus;
    unsigned int szip_options_mask;
    unsigned int szip_pixels_per_block;

    /* zillions of things might be on the plist */
    /*  here are a few... */

    /*
     * get chunking information: rank and dimensions.
     *
     *  For other layouts, would get the relevant information.
     */
    /* if(H5D_CHUNKED == H5Pget_layout(pid)){ */
    /* 	rank_chunk = H5Pget_chunk(pid, 2, chunk_dims_out); */
    /* 	printf("chunk rank %d, dimensions %lu x %lu\n", rank_chunk, */
    /* 	   (unsigned long)(chunk_dims_out[0]), */
    /* 	   (unsigned long)(chunk_dims_out[1])); */
    /* } /1* else if contiguous, etc. *1/ */

    /*
     *  Get optional filters, if any.
     *
     *  This include optional checksum and compression methods.
     */

    nfilters = H5Pget_nfilters(pid);
    for (i = 0; i < nfilters; i++)
    {
        /* For each filter, get
         *   filter ID
         *   filter specific parameters
         */
        cd_nelmts = 32;
        filtn = H5Pget_filter(pid, (unsigned)i,
                              &filt_flags, &cd_nelmts, cd_values,
                              (size_t)MAX_NAME, f_name, &filt_conf);
        /*
         *  These are the predefined filters
         */
        switch (filtn) {
        case H5Z_FILTER_DEFLATE:  /* AKA GZIP compression */
            /* printf("DEFLATE level = %d\n", cd_values[0]); */
            break;
        case H5Z_FILTER_SHUFFLE:
            /* printf("SHUFFLE\n"); /1* no parms *1/ */
            break;
        case H5Z_FILTER_FLETCHER32:
            /* printf("FLETCHER32\n"); /1* Error Detection Code *1/ */
            break;
        case H5Z_FILTER_SZIP:
            szip_options_mask=cd_values[0];;
            szip_pixels_per_block=cd_values[1];

            /* printf("SZIP COMPRESSION: "); */
            /* printf("PIXELS_PER_BLOCK %d\n", */
            /* szip_pixels_per_block); */
            /* print SZIP options mask, etc. */
            break;
        default:
            /* printf("UNKNOWN_FILTER\n" ); */
            break;
        }
    }

    /*
     *  Get the fill value information:
     *    - when to allocate space on disk
     *    - when to fill on disk
     *    - value to fill, if any
     */

    /* printf("ALLOC_TIME "); */
    H5Pget_alloc_time(pid, &at);

    switch (at)
    {
    case H5D_ALLOC_TIME_EARLY:
        /* printf("EARLY\n"); */
        break;
    case H5D_ALLOC_TIME_INCR:
        /* printf("INCR\n"); */
        break;
    case H5D_ALLOC_TIME_LATE:
        /* printf("LATE\n"); */
        break;
    default:
        /* printf("unknown allocation policy"); */
        break;
    }

    /* printf("FILL_TIME: "); */
    H5Pget_fill_time(pid, &ft);
    switch ( ft )
    {
    case H5D_FILL_TIME_ALLOC:
        /* printf("ALLOC\n"); */
        break;
    case H5D_FILL_TIME_NEVER:
        /* printf("NEVER\n"); */
        break;
    case H5D_FILL_TIME_IFSET:
        /* printf("IFSET\n"); */
        break;
    default:
        printf("?\n");
        break;
    }


    H5Pfill_value_defined(pid, &fvstatus);

    if (fvstatus == H5D_FILL_VALUE_UNDEFINED)
    {
        /* printf("No fill value defined, will use default\n"); */
    } else {
        /* Read  the fill value with H5Pget_fill_value.
         * Fill value is the same data type as the dataset.
         * (details not shown)
         **/
    }

    /* ... and so on for other dataset properties ... */
}