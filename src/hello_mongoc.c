//#include <mongoc.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "../lib/hdf5_meta.h"


extern int64_t init_db();
extern int64_t clear_all_docs();
extern void clear_all_indexes();
extern void create_index(const char *index_key);
extern int64_t query_count(const char *query_condition);
extern int64_t query_result_count(const char *query_condition);
extern void query_result_and_print(const char *query_condition);
extern int64_t get_all_doc_count();
extern int64_t importing_json_doc_to_db(const char *json_str);
extern void random_test();


void clear_everything(){
    clear_all_docs();
    clear_all_indexes();
}

void print_usage() {
    printf("Usage: ./hdf5_reader /path/to/hdf5/file\n");
}

/**
 * This is the comment for bench mark scenarios:
 * 
 * A. Solely inserting documents without indexes
 *      Without creating any index, solely inserting documents. 
 *      Time for each insertion operation.
 *      Time for each query. on 'type' and on 'sub_objects.sub_objects.sub_objects.type'
 */
void test_inserting_query_no_index (const char *json_str) {
    clear_everything();
    importing_json_doc_to_db(json_str);
    println("=============== Inserting Document Done! ===============\n");
    int i = 0;
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"type\":\"file\"");
        println("internal rst_count on matched condition = %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"type\":\"object_path\"");
        println("internal rst_count on unmatched condition = %d\n", rst_count);
    }
    
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"type\":\"file\"");
        println("external rst_count on matched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"type\":\"object_path\"");
        println("external rst_count on unmatched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":2160");
        println("internal rst_count on embedded matched condition = %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":21");
        println("internal rst_count on embedded unmatched condition = %d\n", rst_count);
    }
    
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":2160");
        println("external rst_count on embedded matched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":21");
        println("external rst_count on embedded unmatched condition= %d\n", rst_count);
    }
}

/**
 * 
 * 
 * B. Creating index on non-embedded field, like 'type'.
 * 
 *      Time for creating index 'type' with 10M documents already existed there.
 *      Time for creating index 'sub_objects.sub_objects.sub_objects.type' with 10M documents already existed there.
 *      Time for each query on 'type' and on 'sub_objects.sub_objects.sub_objects.type'
 * 
 *      Note: we currently do not test compound index due to limted time. Intereting compound index cases are:
 *      Multi-key index and query that hit partial/all fields in the multi-key index.
 *      Text index and query that perform prefix/suffix query.
 */

void test_creating_index_and_then_query() {
    const char *index_key1 = "\"type\":\"text\"";
    create_index(index_key1);
    println("============== created index 1. ============== ");
    const char *index_key2 = "\"sub_objects.sub_objects.sub_objects.attributes.dim2\":1";
    create_index(index_key2);
    println("============== created index 2. ============== ");

    int i = 0;
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"type\":\"file\"");
        println("internal rst_count on matched condition = %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"type\":\"object_path\"");
        println("internal rst_count on unmatched condition = %d\n", rst_count);
    }
    
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"type\":\"file\"");
        println("external rst_count on matched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"type\":\"object_path\"");
        println("external rst_count on unmatched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":2160");
        println("internal rst_count on embedded matched condition = %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":21");
        println("internal rst_count on embedded unmatched condition = %d\n", rst_count);
    }
    
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":2160");
        println("external rst_count on embedded matched condition= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("\"sub_objects.sub_objects.sub_objects.attributes.dim2\":21");
        println("external rst_count on embedded unmatched condition= %d\n", rst_count);
    }
}

/** 
 * C. Removing all documents and all indexes.
 */


/**
 * D. Creating non-embedded field index, like 'type'. Inserting documents
 *      Time for each insertion operation.
 */
void import_with_single_index(const char *json_str){
    clear_everything();
    const char *index_key1 = "\"type\":\"text\"";
    create_index(index_key1);
    importing_json_doc_to_db(json_str);
}


/**
 * E. Removing all documents and all indexes.
 * 
 * F. Creating two field index, like 'type' and 'sub_objects.sub_objects.sub_objects.type'. 
 *      Inserting documents.
 *      Time for each insertion operation.
 */
void import_with_two_indexes(const char *json_str){
    const char *index_key1 = "\"type\":\"text\"";
    create_index(index_key1);
    println("============== created index 1. ============== ");
    const char *index_key2 = "\"sub_objects.sub_objects.sub_objects.attributes.dim2\":1";
    create_index(index_key2);
    println("============== created index 2. ============== ");
    importing_json_doc_to_db(json_str);
}
/**
 * G. Removing everything.
 * 
 * H. Creating 3 to 5 indexes, inserting documents
 *      Time for each insertion operation. 
 * 
 */ 

int
main (int argc, char *argv[])
{
    int64_t doc_count = init_db();
    printf("successfully init db, %d documents in mongodb.\n", doc_count);
    // random_test();
    // clear_everything();

    char* filename;

    if (argc != 2)
        print_usage();
    else {
        filename = argv[1];
        char *json_str = NULL;
        parse_hdf5_meta_as_json_str(filename, &json_str);
        println("%s\n", json_str);
        println("================= TEST BEGIN ==============");
        // Bench A.
        println("================= BENCH A ==============");
        test_inserting_query_no_index(json_str);
        // Bench B. 
        // println("================= BENCH B ==============");
        // test_creating_index_and_then_query();
        // // Bench C, D. 
        // println("================= BENCH C,D ==============");
        // import_with_single_index(json_str);
        // // Bench E, F.
        // println("================= BENCH E,F ==============");
        // import_with_two_indexes(json_str);
        // println("================= TEST END ==============");
    }
    return 0;
}