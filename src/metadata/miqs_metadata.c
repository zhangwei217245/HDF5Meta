#include "miqs_metadata.h"

miqs_attr_type_t get_miqs_type_from_int(int number){
    miqs_attr_type_t rst = MIQS_AT_UNKNOWN;
    switch(number) {
        case 1:
            rst = MIQS_AT_INTEGER;
            break;
        case 2:
            rst = MIQS_AT_FLOAT;
            break;
        case 3:
            rst = MIQS_AT_STRING;
            break;
        default:
            break;
    }
    return rst;
}