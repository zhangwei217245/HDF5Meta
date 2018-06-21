#include "../lib/hdf5_meta.h"

void print_usage() {
    printf("Usage: ./hdf5_reader /path/to/hdf5/file\n");
}

int
main(int argc, char **argv)
{
    char* filename;

    if (argc != 2)
        print_usage();
    else {
        filename = argv[1];
        json_object *rootObj = json_object_new_object();
        parse_hdf5_file(filename, rootObj);
    }
    return 0;
}