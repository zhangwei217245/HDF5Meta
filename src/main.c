#include <stdio.h>
#include "../lib/utils/query_utils.h"
#include "dira/printa.h"
#include "dirb/printb.h"

int main(void){
    gen_tags_in_loop();

    char *key_patterns[] = {"tag4", "*tag4", "tag4*", "*tag4*", "tag5", "*tag5", "tag5*", "*tag5*"};

    char *value_patterns[] = {"194", "*194", "194*", "*194*", "195", "*195", "195*", "*195*"};

    char *a = "tag0=190,tag1=191,tag2=192,tag3=193,tag4=194,tag5=195,tag6=196,tag7=197,tag8=198,tag9=199,tag10=1910,tag11=1911,tag12=1912,tag13=1913,tag14=1914,tag15=1915,tag16=1916,tag17=1917,tag18=1918,tag19=1919";

    int i, j;
    for (i = 0; i < 8; i++) {
        char *key_ptn = key_patterns[i];
        println("======== kptn : %s =======", key_ptn);
        for (j = 0; j < 8 ; j++) {
            char *value_ptn = value_patterns[j];
            //test has tag p
            int rst = has_tag_p(a, key_ptn);
            println("is there any tag matches pattern %s?  %d", key_ptn, rst);


            //test match kv pair
            char *rsts = k_v_matches_p(a, key_ptn, value_ptn );
            println("what is the tag matches "ANSI_COLOR_RED"%s"ANSI_COLOR_RESET" with value matches " ANSI_COLOR_BLUE "%s" ANSI_COLOR_RESET "?  %s", key_ptn, value_ptn,rsts);

        }
    }
}
