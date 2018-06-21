//#include <mongoc.h>

extern void random_test();
extern void init_db();

int
main (int argc, char *argv[])
{
    //mongoc_client_t *client;

    /*
    *     * Required to initialize libmongoc's internals
    *         */
    //mongoc_init ();
    init_db();
    random_test();
    return 0;
}
