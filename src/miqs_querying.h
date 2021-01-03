#include <stdlib.h>

typedef struct {
    char *file_path;
    char *obj_path;
} search_rst_entry_t;

typedef struct {
    size_t size;
    search_rst_entry_t *rst_arr;
} power_search_rst_t;

typedef enum miqs_attr_value_type {MIQS_ATV_UNKNOWN=0, MIQS_ATV_INTEGER = 1, MIQS_ATV_FLOAT = 2, MIQS_ATV_STRING = 3} miqs_attr_value_type_t;

/**
 * Exact search by attribute name, attribute value and the data type of the attribute value.
 * 
 * @param attr_name  Attribute name
 * @param attribute_value       Attribute value
 * @param attr_value_type       data type of the attribute value
 * @return An instance of power_search_rst_t struct, containing a list of <file_path, obj_path> entries.
 */
power_search_rst_t *exact_metadata_search(char *attr_name, void *attribute_value, miqs_attr_value_type_t attr_value_type);