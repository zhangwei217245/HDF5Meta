//#include <mongoc.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "../lib/hdf5_meta.h"


extern int64_t init_db();
extern int64_t clear_all_docs();
extern void clear_all_indexes();
extern void drop_current_coll();
extern void create_index(const char *index_key);
extern void create_doc_id_index();
extern void create_dataset_name_index();
extern void create_root_obj_path_index();
extern void create_lv2_obj_path_index();
extern void create_lv3_obj_path_index();
extern int64_t query_count(const char *query_condition);
extern int64_t query_result_count(const char *query_condition);
extern void query_result_and_print(const char *query_condition);
extern int64_t get_all_doc_count();
extern int64_t importing_json_doc_to_db(const char *json_str);
extern void random_test();


void clear_everything(){
    drop_current_coll();
}

void print_usage() {
    printf("Usage: ./hdf5_reader /path/to/hdf5/file [A/B/C/D]\n");
    
    // Bench A.
    println("================= BENCH A ==============");
    println("test_inserting_query_no_index(json_str)");
    // Bench B. 
    println("================= BENCH B ==============");
    println("test_creating_index_and_then_query()");
    // Bench C. 
    println("================= BENCH C ==============");
    println("import_with_single_index(json_str)");
    // Bench D.
    println("================= BENCH D ==============");
    println("import_with_two_indexes(json_str)");

    println("================= BENCH E ==============");
    println("import_with_three_indexes(json_str)");

    println("================= BENCH F ==============");
    println("import_with_four_indexes(json_str)");

    println("================= BENCH G ==============");
    println("import_with_five_indexes(json_str)");
        
}

void issue_queries() {
    int i = 0;
    // for (i = 0; i < 100; i++) {
    //     int64_t rst_count = query_count("{\"h5doc_id\":5}");
    //     println("internal rst_count on matched h5doc_id = %d\n", rst_count);
    // }

    // for (i = 0; i < 100; i++) {
    //     int64_t rst_count = query_count("{\"h5doc_id\":\"object_path\"}");
    //     println("internal rst_count on unmatched h5doc_id = %d\n", rst_count);
    // }
    
    // for (i = 0; i < 100; i++) {
    //     int64_t rst_count = query_result_count("{\"h5doc_id\":5}");
    //     println("external rst_count on matched h5doc_id= %d\n", rst_count);
    // }

    // for (i = 0; i < 100; i++) {
    //     int64_t rst_count = query_result_count("{\"h5doc_id\":\"object_path\"}");
    //     println("external rst_count on unmatched h5doc_id= %d\n", rst_count);
    // }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("{\"sub_objects.sub_objects.sub_objects.dataset_name\":\"/20140212_071649_HN18_RT_60N_6hr_scan50/20140212_071649_HN18_RT_60N_6hr_scan50_0000_0027.tif\"}");
        println("internal rst_count on embedded matched dataset_name = %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_count("{\"sub_objects.sub_objects.sub_objects.dataset_name\":\"/20140212_071649_HN18_RT_60N_6hr_scan50\"}");
        println("internal rst_count on embedded unmatched dataset_name = %d\n", rst_count);
    }
    
    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("{\"sub_objects.sub_objects.sub_objects.dataset_name\":\"/20140212_071649_HN18_RT_60N_6hr_scan50/20140212_071649_HN18_RT_60N_55hr_scan50_0000_0027.tif\"}");
        println("external rst_count on embedded matched dataset_name= %d\n", rst_count);
    }

    for (i = 0; i < 100; i++) {
        int64_t rst_count = query_result_count("{\"sub_objects.sub_objects.sub_objects.dataset_name\":\"/20140212_071649_HN18_RT_60N_6hr_scan50\"}");
        println("external rst_count on embedded unmatched dataset_name= %d\n", rst_count);
    }
}

void create_index_on_docid(){
    const char *index_key1 = "{ \"h5doc_id\": 1 }";
    create_index(index_key1);
}

void create_index_on_dataset_name() {
    const char *index_key2 = "{ \"sub_objects.sub_objects.sub_objects.dataset_name\": \"text\"}";
    create_index(index_key2);
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
    issue_queries();
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
    // create_doc_id_index();
    println("============== created index 1. ============== ");
    // create_dataset_name_index();
    println("============== created index 2. ============== ");
    issue_queries();
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
    create_doc_id_index();
    println("============== created index 1. ============== ");
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
    clear_everything();
    create_doc_id_index();
    println("============== created index 1. ============== ");
    create_dataset_name_index();
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

void import_with_three_indexes(const char *json_str){
    clear_everything();
    create_doc_id_index();
    println("============== created index 1. ============== ");
    create_dataset_name_index();
    println("============== created index 2. ============== ");
    create_root_obj_path_index();
    println("============== created index 3. ============== ");
    importing_json_doc_to_db(json_str);
}

void import_with_four_indexes(const char *json_str){
    clear_everything();
    create_doc_id_index();
    println("============== created index 1. ============== ");
    create_dataset_name_index();
    println("============== created index 2. ============== ");
    create_root_obj_path_index();
    println("============== created index 3. ============== ");
    create_lv2_obj_path_index();
    println("============== created index 4. ============== ");
    importing_json_doc_to_db(json_str);
}

void import_with_five_indexes(const char *json_str){
    clear_everything();
    create_doc_id_index();
    println("============== created index 1. ============== ");
    create_dataset_name_index();
    println("============== created index 2. ============== ");
    create_root_obj_path_index();
    println("============== created index 3. ============== ");
    create_lv2_obj_path_index();
    println("============== created index 4. ============== ");
    create_lv3_obj_path_index();
    println("============== created index 5. ============== ");
    importing_json_doc_to_db(json_str);
}

void import_with_no_index(const char *json_str){
    clear_everything();
    importing_json_doc_to_db(json_str);
}

int
main (int argc, char *argv[])
{

    int64_t doc_count = init_db();
    printf("successfully init db, %d documents in mongodb.\n", doc_count);    

    char* filename;
    char test_opt;

    if (argc != 3)
        print_usage();
    else {
        filename = argv[1];
        test_opt = argv[2][0];

        char *json_str = NULL;

        println("================= EXTRACTING HDF5 METADATA ==============");
        parse_hdf5_meta_as_json_str(filename, &json_str);
        println("%s\n", json_str);
        println("================= ENDING EXTRACTING HDF5 METADATA ==============");

        switch(test_opt) {
            case 'A':
                // Bench A.
                println("================= BENCH A ==============");
                test_inserting_query_no_index(json_str);
                break;
            case 'B':
                // Bench B. 
                println("================= BENCH B ==============");
                test_creating_index_and_then_query();
                break;
            case 'C':
                // Bench C. 
                println("================= BENCH C ==============");
                import_with_single_index(json_str);
                break;
            case 'D':
                // Bench D.
                println("================= BENCH D ==============");
                import_with_two_indexes(json_str);
                break;
            case 'E':
                // Bench E.
                println("================= BENCH E ==============");
                import_with_three_indexes(json_str);
                break;
            case 'F':
                // Bench F.
                println("================= BENCH F ==============");
                import_with_four_indexes(json_str);
                break;
            case 'G':
                // Bench G.
                println("================= BENCH G ==============");
                import_with_four_indexes(json_str);
                break;
            case 'H':
                // Bench H.
                println("================= BENCH H ==============");
                import_with_no_index(json_str);
                break;
            default:
                println("Undefined test.");
                break;
        }
        println("================= Benchmark done. =================");
    }
    return 0;
}