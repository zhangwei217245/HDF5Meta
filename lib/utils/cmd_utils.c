#include "string_utils.h"
#include "cmd_utils.h"

#define BUFSIZE 128

char *execute_cmd(char *cmd) {
    char *rst = NULL;
    
    char buf[BUFSIZE];
    FILE *fp;

    if ((fp = popen(cmd, "r")) == NULL) {
        printf("Error opening pipe!\n");
        return rst;
    }
    rst = (char *)calloc(BUFSIZE, sizeof(char));
    while (fgets(buf, BUFSIZE, fp) != NULL) {
        rst = concat(rst, buf);
        // printf("OUTPUT: %s", buf);
    }

    if(pclose(fp))  {
        printf("Command not found or exited with error status\n");
        return NULL;
    }

    return rst;
}