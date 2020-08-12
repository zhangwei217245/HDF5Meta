struct btree_node {
    struct btree_node *left;
    struct btree_node *right;
    void *item;
};

static void btree_free_node(struct btree_node *node);

static struct btree_node* find_min_node(struct btree_node *node);

static struct btree_node* find_max_node(struct btree_node *node);

int btree_search(struct btree_node *root, void *item, int (*comp)(const void*,const void*));

int btree_insert(struct btree_node **root, void *item, unsigned int size, int (*comp)(const void*,const void*));

struct btree_node* btree_delete_node(struct btree_node *root, void *item, unsigned int size, int (*compare_node)(const void*,const void*));

void btree_print(struct btree_node *root, void (*print)(const void *));

void btree_free(struct btree_node *root);