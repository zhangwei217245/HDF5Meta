#ifndef MIQS_ON_DISK_INDEX
#define MIQS_ON_DISK_INDEX
#include "../dao/bin_file_ops.h"
#include "../libhl/linklist.h"
#include "../ds/art.h"


typedef struct{
    int type; // type: 1, int, 2, float, 3. string
    char *name;
    void *data;
    char *file_path;
    char *object_path;
} index_record_t;

/**
 * Create index record in memory.
 * 
 */
index_record_t *create_index_record(int type, char *name, void *data, char *file_path, char *object_path);

/**
 *  append index_record to the current position of the stream
 *  users can call fseek to adjust the position of the file pointer to the end of the file. 
 */
void append_index_record(index_record_t *ir, FILE *stream);

/**
 * Print the index record just for visual verification.
 * 
 */
void print_index_record(index_record_t ir);

/**
 * Search over disk, from the beginning of a file, and collect result matching given compare function
 * 
 */
index_record_t **find_index_record(char *name,
        int (*compare_value)(const void *data, const void *criterion), void *criterion,
        FILE *stream, int *out_len);

/**
 * Read one index record from current position of the file.
 */
index_record_t *read_index_record(FILE *stream);



int int_equals(const void *data, const void *criterion);
int double_equals(const void *data, const void *criterion);
int string_equals(const void *data, const void *criterion);


int append_int_value_tree(const void **rootp, FILE *stream);
int append_float_value_tree(const void **rootp, FILE *stream);
int append_string_art(art_tree *art, FILE *stream);
int append_string_linked_list(linked_list_t *list, FILE *stream);

#endif /* !MIQS_ON_DISK_INDEX */