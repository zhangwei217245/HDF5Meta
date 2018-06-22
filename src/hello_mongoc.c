//#include <mongoc.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "../lib/hdf5_meta.h"

extern void random_test();
extern int64_t init_db();
extern int32_t import_json_to_mongo(const char *json_str);


void print_usage() {
    printf("Usage: ./hdf5_reader /path/to/hdf5/file\n");
}

int
main (int argc, char *argv[])
{
    int64_t doc_count = init_db();
    printf("successfully init db, %d documents in mongodb.\n", doc_count);
    
    random_test();

    char* filename;

    if (argc != 2)
        print_usage();
    else {
        filename = argv[1];
        char *json_str = NULL;
        parse_hdf5_meta_as_json_str(filename, &json_str);
        printf("%s\n", json_str);
        
        int32_t import_rst = importing_json_doc_to_db(json_str);
        printf("import_rst = %d\n");
    }

    
    return 0;
}
