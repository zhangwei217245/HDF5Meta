#define ENABLE_MPI

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "miqs_indexing.h"
#include "miqs_querying.h"
#include "utils/timer_utils.h"
#include "utils/string_utils.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

int
main (int argc, char **argv)
{
    int rank = 0, size = 1;
    int rst = 1;

    #ifdef ENABLE_MPI
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
    #endif

    printf("size=%d, rank=%d\n", size, rank);

    exit (0);
}