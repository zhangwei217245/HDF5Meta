//
// Created by 张威 on 7/14/17.
//

#include "string_utils.h"


int startsWith(const char *pre, const char *str){
    size_t lenpre = strlen(pre), lenstr = strlen(str);
    return lenstr < lenpre ? 0 : (strncmp(pre, str, lenpre) == 0);
}

int endsWith(const char *suf, const char *str){
    size_t lensuf = strlen(suf), lenstr = strlen(str);
    return lenstr < lensuf ? 0 : (strncmp(str+lenstr-lensuf, suf, lensuf)==0);
}

int contains(const char *tok, const char *str){
    return strstr(str, tok) != NULL;
}

int equals(const char *tok, const char *str){
    return strcmp(tok, str)==0;
}

int simple_matches(const char *str, const char *pattern){
    int pattern_type = determine_pattern_type(pattern);

    int result = 0;
    char *tok = NULL;
    switch(pattern_type){
        case PATTERN_EXACT:
            result = equals(str, pattern);
            break;
        case PATTERN_PREFIX:
            tok = subrstr(pattern, strlen(pattern)-1);
            result = (tok == NULL ? 0 : startsWith(tok, str));
            break;
        case PATTERN_SUFFIX:
            tok = substr(pattern, 1);
            result = (tok == NULL ? 0 : endsWith(tok, str));
            break;
        case PATTERN_MIDDLE:
            tok = substring(pattern, 1, strlen(pattern)-1);
            result = (tok == NULL ? 0 : contains(tok, str));
            break;
        default:
            break;
    }
    if (tok != NULL) {
        free(tok);
    }
    return result;
}

int determine_pattern_type(const char *pattern){

    if (startsWith("*", pattern)) {
        if (endsWith("*", pattern)) {
            return PATTERN_MIDDLE;
        } else {
            return PATTERN_SUFFIX;
        }
    } else {
        if (endsWith("*", pattern)) {
            return PATTERN_PREFIX;
        } else {
            return PATTERN_EXACT;
        }
    }
}
char *substr(const char *str, int start){
    return substring(str, start, -1);
}
char *subrstr(const char *str, int end) {
    return substring(str, 0, end);
}

char *substring(const char *str, int start, int end) {
    char *ret = NULL;
    int lenstr = strlen(str);
    if (str == NULL || lenstr <=0 ){
        return ret;
    }
    if (end < 0) {
        end = lenstr;
    }
    if (start < 0 || end > (lenstr+1)) {
        println("bingo 2");
        return ret;
    }

    int len = end - start;
    ret = (char *)calloc(len, sizeof(char));
    //ret[0] = '\0';
    strncpy(ret, &str[start], len);
    return ret;
}

int indexOf(const char* str, char c) {
    const char *p = strchr(str, c);
    if (p) {
        return p - str;
    }
    return -1;
}

char* dsprintf(const char *format, ...){
    char* ret;
    //1. declare argument list
    va_list args;
    //2. starting argument list
    va_start(args, format);
    //3. get arguments value
    int numbytes = vsnprintf( (char*)NULL,0, format, args);
    ret = (char*) calloc( (numbytes+1), sizeof(char) );

    va_start(args, format);
    vsprintf(ret, format, args);
    //4. ending argument list
    va_end(args);
    return ret;
}

void println(const char *format, ...){
    //1. declare argument list
    va_list args;
    //2. starting argument list
    va_start(args, format);
    //3. get arguments value
    vfprintf(stdout, format, args);
    //4. ending argument list
    va_end(args);
    fputc('\n',stdout);
}