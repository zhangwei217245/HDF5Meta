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
        char *json_str = NULL;
        parse_hdf5_meta_as_json_str(filename, &json_str);
        printf("%s\n", json_str);
    }
    return 0;
}