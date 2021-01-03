#ifndef MIQS_MEM_PERF_H
#define MIQS_MEM_PERF_H

#include "../cbase/base_stdlib.h"
#include "../cbase/c99_stdlib.h"
#include "../timer_utils.h"

#define DECLARE_PERF_INFO_FIELDS size_t mem_usage;\
    uint64_t num_of_comparisons;\
    uint64_t num_of_reallocs;\
    stw_nanosec_t time_to_locate;\
    stw_nanosec_t time_for_expansion;\

typedef struct perf_info{
    DECLARE_PERF_INFO_FIELDS
} perf_info_t;

#define GET_PERF_INFO(_ds) \
    perf_info_t *perf_info = (perf_info_t *)calloc(1, sizeof(perf_info_t));\
    perf_info->mem_usage = _ds->mem_usage;\
    perf_info->num_of_comparisons = _ds->num_of_comparisons;\
    perf_info->num_of_reallocs = _ds->num_of_reallocs;\
    perf_info->time_to_locate = _ds->time_to_locate;\
    perf_info->time_for_expansion = _ds->time_for_expansion;\
    return perf_info;\


#define RESET_PERF_INFO_COUNTERS(_ds) \
    _ds->num_of_comparisons=0;\
    _ds->num_of_reallocs=0;\
    _ds->time_to_locate=0;\
    _ds->time_for_expansion=0;\


#define INIT_PERF_INFO_FIELDS(_ds, _type)\
    _ds->mem_usage+=sizeof(_type);\
    RESET_PERF_INFO_COUNTERS(_ds)\




/**
 * Memory allocation with counter.
 * allocate 'size' bytes of memory and return a generic pointer.
 */
void *ctr_malloc(size_t size, size_t *reg);

/**
 * Memory allocation with counter. (Clear allocation)
 * allocate nitems * size bytes of memory and return a generic pointer.
 */
void *ctr_calloc(size_t nitems, size_t size, size_t *reg);

/**
 * Calling realloc and record it size. 
 */
void *ctr_realloc(void *ptr, size_t new_size, size_t *reg);

#endif // END MIQS_MEM_PERF_H