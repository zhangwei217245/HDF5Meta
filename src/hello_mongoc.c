//#include <mongoc.h>

extern void random_test();
int
main (int argc, char *argv[])
{
   //mongoc_client_t *client;

   /*
    *     * Required to initialize libmongoc's internals
    *         */
   //mongoc_init ();
    random_test();
   return 0;
}
