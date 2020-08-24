//
// Created by Wei Zhang on 7/12/17.
//
#include <uuid/uuid.h>
#include "string_utils.h"


affix_t *create_affix_info(const char *body, size_t len, pattern_type_t affix_type, void *user){
    affix_t *rst = (affix_t *)calloc(1, sizeof(affix_t));
    rst->body = body;
    rst->length = len;
    rst->type = affix_type;
    rst->user = user;
    return rst;
}

int startsWith(const char *str, const char *pre){
    char *found = strstr(str, pre);
    return (found && (found - str) == 0);
}

int endsWith(const char *str, const char *suf){
    size_t lensuf = strlen(suf), lenstr = strlen(str);
    return lenstr < lensuf ? 0 : (strncmp(str+lenstr-lensuf, suf, lensuf)==0);
}

int contains(const char *str, const char *tok){
    return strstr(str, tok) != NULL;
}

int equals(const char *str, const char *tok){
    return strcmp(tok, str)==0;
}

int simple_matches(const char *str, const char *pattern){
    int result = 0;
    // Ensure both str and pattern cannot be empty.
    if (str == NULL || pattern == NULL) {
        return result;
    }
    int pattern_type = determine_pattern_type(pattern);

    char *tok = NULL;
    switch(pattern_type){
        case PATTERN_EXACT:
            result = equals(str, pattern);
            break;
        case PATTERN_PREFIX:
            tok = subrstr(pattern, strlen(pattern)-1);
            result = (tok == NULL ? 0 : startsWith(str, tok));
            break;
        case PATTERN_SUFFIX:
            tok = substr(pattern, 1);
            result = (tok == NULL ? 0 : endsWith(str, tok));
            break;
        case PATTERN_MIDDLE:
            tok = substring(pattern, 1, strlen(pattern)-1);
            result = (tok == NULL ? 0 : contains(str, tok));
            break;
        default:
            break;
    }
    if (tok != NULL) {
        //free(tok);
    }
    return result;
}

int is_matching_given_affix(const char *str, affix_t *affix_info){
    int matched = 0;
    switch (affix_info->type) 
    {
    case PATTERN_PREFIX:
        matched = startsWith(str, affix_info->body);
        break;
    case PATTERN_SUFFIX:
        matched = endsWith(str, affix_info->body);
        break;
    case PATTERN_MIDDLE:
        matched = contains(str, affix_info->body);
        break;
    default:
        break;
    }
    return matched;
}

pattern_type_t determine_pattern_type(const char *pattern){

    if (startsWith(pattern, "*")) {
        if (endsWith(pattern, "*")) {
            return PATTERN_MIDDLE;
        } else {
            return PATTERN_SUFFIX;
        }
    } else {
        if (endsWith(pattern, "*")) {
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
        return ret;
    }

    int len = end - start;
    ret = (char *)calloc(len, sizeof(char));
    strncpy(ret, &str[start], len);
    return ret;
}
int indexOfStr(const char* str, char *tok){
    const char *p = strstr(str, tok);
    if (p) {
        return p - str;
    }
    return -1;
}
int indexOf(const char* str, char c) {
    const char *p = strchr(str, c);
    if (p) {
        return p - str;
    }
    return -1;
}

char *
concat (const char *str, ...)
{
  va_list ap;
  size_t allocated = 100;
  char *result = (char *) malloc (allocated);

  if (result != NULL)
    {
      char *newp;
      char *wp;
      const char *s;

      va_start (ap, str);

      wp = result;
      for (s = str; s != NULL; s = va_arg (ap, const char *))
        {
          size_t len = strlen (s);

          /* Resize the allocated memory if necessary.  */
          if (wp + len + 1 > result + allocated)
            {
              allocated = (allocated + len) * 2;
              newp = (char *) realloc (result, allocated);
              if (newp == NULL)
                {
                  free (result);
                  return NULL;
                }
              wp = newp + (wp - result);
              result = newp;
            }

          wp = memcpy(wp, s, len)+len;
        }

      /* Terminate the result string.  */
      *wp++ = '\0';

      /* Resize memory to the optimal size.  */
      newp = realloc (result, wp - result);
      if (newp != NULL)
        result = newp;

      va_end (ap);
    }

    return result;
}

int str_append(char **result_ptr, const char *format, ...)
{
    char *str = NULL;
    char *old_json = NULL, *new_json = NULL;

    va_list arg_ptr;
    va_start(arg_ptr, format);
    vasprintf(&str, format, arg_ptr);

    // save old result
    asprintf(&old_json, "%s", (*result_ptr == NULL ? "" : *result_ptr));

    // calloc new json memory
    new_json = (char *)calloc(strlen(old_json) + strlen(str) + 1, sizeof(char));

    strcat(new_json, old_json);
    strcat(new_json, str);

    if (*result_ptr) free(*result_ptr);
    *result_ptr = new_json;

    free(old_json);
    free(str);

    return 0;
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
    fflush(stdout);
}

void eprintln(const char *format, ...){
    //1. declare argument list
    va_list args;
    //2. starting argument list
    va_start(args, format);
    //3. get arguments value
    vfprintf(stderr, format, args);
    //4. ending argument list
    va_end(args);
    fputc('\n',stderr);
    fflush(stderr);
}

char *reverse_str(char *str){
    int len = strlen(str);
    char *rst = (char *)calloc(len+1, sizeof(rst));
    int i = 0;
    for (i = 0; i < len; i++){
        rst[len-1 -i] = str[i];
    }
    return rst;
}


char **gen_uuids_strings(int count){
    uuid_t out;
    int c = 0;
    char **result = (char **)calloc(count, sizeof(char*));
    for (c = 0; c < count ; c++) {
        uuid_generate_random(out);
        result[c] = (char *)calloc(37, sizeof(char));
        uuid_unparse_lower(out, result[c]);
    }
    return result;
}

char **gen_rand_strings(int count, int maxlen){
    return gen_random_strings(count, maxlen, 1);
}

char **gen_random_strings(int count, int maxlen, int use_dynamic_length){
    
    char charset[] = ".-_"
                     "0123456789"
                     "abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    int c = 0;
    int i = 0;
    char **result = (char **)calloc(count, sizeof(char*));
    for (c = 0; c < count ; c++) {
        int len = use_dynamic_length==1?rand()%maxlen + 1:maxlen;
        char *str = (char *)calloc(len+1, sizeof(char));
        for (i = 0; i < len; i++) {
            size_t index = (double) rand() / RAND_MAX * (sizeof charset - 1);
            str[i] = charset[index];
        }
        str[len] = '\0';
        //printf("generated %s\n", str);
        result[c] = str;
    }
    return result;
}

char **read_words_from_text(const char *fileName, int *word_count){
    
    FILE* file = fopen(fileName, "r"); /* should check the result */
    if (file == NULL){
        println("File not available\n");
        exit(4);
    }
    int lines_allocated =128;
    int max_line_len = 512;
    char **words = (char **)malloc(sizeof(char*)*lines_allocated);
    if (words == NULL) {
        fprintf(stderr, "Out of memory\n");
        exit(1);
    }
    int i;
    int line_count = 0;
    for (i = 0; 1; i++) {
        int j;
        if (i >= lines_allocated) {
            int new_size;
            new_size = lines_allocated * 2;
            char **new_wordlist_ptr = (char **)realloc(words, sizeof(char*)*new_size);
            if (new_wordlist_ptr == NULL) {
                fprintf(stderr, "Out of memory\n");
                exit(3);
            }
            words = new_wordlist_ptr;
            lines_allocated = new_size;
        }
        words[line_count] = (char *)malloc(sizeof(char)*max_line_len);
        if (words[line_count]==NULL) {
            fprintf(stderr, "out of memory\n");
            exit(4);
        }
        if (fgets(words[line_count], max_line_len-1, file)==NULL) {
            break;
        }
        /* Get rid of CR or LF at end of line */
        for (j=strlen(words[line_count])-1;j>=0 && (words[line_count][j]=='\n' || words[line_count][j]=='\r');j--)
            ;
        words[line_count][j+1]='\0';
        line_count++;

    }
    *word_count = line_count;

    fclose(file);
    return words;
}