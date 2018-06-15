//
// Created by 张威 on 7/14/17.
//

#ifndef PDC_STRING_UTILS_H
#define PDC_STRING_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define PATTERN_EXACT  0
#define PATTERN_SUFFIX 1
#define PATTERN_PREFIX 2
#define PATTERN_MIDDLE 3

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

/**
 * take the part after start position
 * you need to free str after use.
 * @param str
 * @param start
 * @return
 */
char *substr(const char* str, int start);
/**
 * take the part before end position
 * you need to free str after use.
 * @param str
 * @param end
 * @return
 */
char *subrstr(const char* str, int end);
/**
 * take the substring starting from start, ending at end-1
 * you need to free str after use.
 * @param str
 * @param start
 * @param end
 * @return
 */
char *substring(const char* str, int start, int end);
/**
 * determine the pattern type.
 * Currently, only support for different types:
 *
 * PREFIX : ab*
 * SUFFIX : *ab
 * MIDDLE : *ab*
 * EXACT  : ab
 *
 * @param pattern
 * @return
 */
int determine_pattern_type(const char *pattern);
/**
 * return the index of character c in given string str.
 * if not found, return -1
 *
 * @param str
 * @param c
 * @return
 */
int indexOf(const char* str, char c);

/**
 * to determine if a string str starts with prefix pre
 * @param pre
 * @param str
 * @return
 */
int startsWith(const char *pre, const char *str);
/**
 * to determine if a string str ends with suffix suf
 * @param suf
 * @param str
 * @return
 */
int endsWith(const char *suf, const char *str);
/**
 * to determine if a string str contains token tok
 * @param tok
 * @param str
 * @return
 */
int contains(const char *tok, const char *str);
/**
 * to determine if a string str exactly matches a token tok.
 * @param tok
 * @param str
 * @return
 */
int equals(const char *tok, const char *str);

/**
 * dynamically generate string for you according to the format you pass.
 * Since usually, it is hard to predict how much memory should be allocated
 * before generating an arbitrary string.
 *
 * remember to free the generated string after use.
 *
 * @param format
 * @param ...
 * @return
 */
char* dsprintf(const char *format, ...);

/**
 * Always put a line feed after the string you want to print.
 *
 * @param format
 * @param ...
 */
void println(const char *format, ...);

/**
 * Only support expressions like:
 *
 * *ab
 * ab*
 * *ab*
 * ab
 *
 * @param str
 * @param pattern
 * @return
 */
int simple_matches(const char *str, const char *pattern);

#endif //PDC_STRING_UTILS_H
