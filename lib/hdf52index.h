#include "new_hdf5_meta.h"
#include "art.h"
#include "bplustree.h"

// For index creation,
// This is the opdata that will be passed to on_obj and on_attr functions.
typedef struct {
    char *file_path;
    char *obj_path;
    art_tree *root_art;
} index_anchor;


typedef struct {
    int is_numeric;
    int is_float;
    struct bplus_tree *bpt;
    art_tree *art;
}art_leaf_content_t;

typedef struct {
    art_tree *file_path_art;
    art_tree *obj_path_art;
} path_bpt_leaf_cnt_t;


void parse_hdf5_file(char *filepath, art_tree **tree);
