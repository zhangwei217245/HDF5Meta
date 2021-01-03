#include "hash_utils.h"

unsigned long
djb2_hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;
    do {
        c = *str++;
        if (c != 0) {
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        } else {
            break;
        }
    } while (c != 0);
    return hash;
}


unsigned long
sdbm_hash(unsigned char *str)
{
    unsigned long hash = 0;
    int c;
    do {
        c = *str++;
        if (c != 0) {
            hash = c + (hash << 6) + (hash << 16) - hash;
        } else {
            break;
        }
    } while (c!=0);
    return hash;
}

unsigned long
lose_lose_hash(unsigned char *str)
{
    unsigned int hash = 0;
    int c;
    do {
        c = *str++;
        if (c != 0) {
            hash += c;
        } else {
            break;
        }
    } while (c != 0);
    return hash;
}