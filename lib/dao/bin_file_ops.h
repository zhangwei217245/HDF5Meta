
/* File foo.  */
#ifndef MIQS_BIN_FILE_OPS
#define MIQS_BIN_FILE_OPS


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


void miqs_append_uint64(uint64_t data, FILE *stream);

void miqs_append_int(int data, FILE *stream);

void miqs_append_double(double data, FILE *stream);

void miqs_append_string(char *data, FILE *stream);

void miqs_append_type(int type, FILE *stream);

int *miqs_read_int(FILE *file);

double *miqs_read_double(FILE *file);

char *miqs_read_string(FILE *file);

uint64_t *miqs_read_uint64(FILE *file);

size_t miqs_skip_field(FILE *stream);

#endif /* !MIQS_BIN_FILE_OPS */
