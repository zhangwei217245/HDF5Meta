#include "mem_perf.h"

void *ctr_malloc(size_t size, size_t *reg){
    *reg = (*reg) + size;
    return malloc(size);
}


void *ctr_calloc(size_t nitems, size_t size, size_t *reg){
    size_t t = nitems * size;
    *reg = (*reg) + t;
    return calloc(nitems, size);
}

void *ctr_realloc(void *ptr, size_t new_size, size_t *reg){
    *reg = new_size;
    return realloc(ptr, new_size);
}