
/* File foo.  */
#ifndef MIQS_BIN_FILE_OPS
#define MIQS_BIN_FILE_OPS


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>



void append_int(int data, FILE *stream);

void append_double(double data, FILE *stream);

void append_string(char *data, FILE *stream);

void append_type(int type, FILE *stream);

int *read_int(FILE *file);

double *read_double(FILE *file);

char *read_string(FILE *file);

size_t skip_field(FILE *stream);

#endif /* !MIQS_BIN_FILE_OPS */
