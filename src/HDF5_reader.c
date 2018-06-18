#include "../lib/hdf5_meta.h"

char tags_g[MAX_TAG_LEN];
char *tags_ptr_g;
hsize_t tag_size_g;
int  ndset = 0;
FILE *summary_fp_g;
int max_tag_size_g = 0;

void print_usage() {
    printf("Usage: srun -n ./h5boss_v2_import /path/to/h5boss_file\n");
}

int
main(int argc, char **argv)
{

    hid_t    file;
    hid_t    grp;
    herr_t   status;


    char* filename;

    if (argc != 2)
        print_usage();
    else {
        filename = argv[1];

        /*
         *  Example: open a file, open the root, scan the whole file.
         */
        file = H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT);

        grp = H5Gopen(file,"/", H5P_DEFAULT);
        scan_group(grp);

        status = H5Fclose(file);

        //printf("%s, %d\n", filename, max_tag_size_g);
        /* printf("\n\n======================\nNumber of datasets: %d\n", ndset); */
    }
    return 0;
}