

/**
 * A struct storing the configuration of the indexing procedure.
 */
typedef struct {
    int parallelism;                        /** < parallelism of metadata index */
    // index_anchor_t *_idx_anchor;            /** < an internal instance of root index_anchor */
    int rank;                               /** < rank of the current MPI process */
    int size;                               /** < number of all MPI processes */
    int current_file_count;                 /** < The number of files current process have gone through */
    int topk;                               /** < The number of first few files that will be scanned */
    int is_build_from_scratch;              /** < Indicating if we are building the index from scratch */
    int is_mdb_enabled;                     /** < Indicating if mdb function is enabled (1=enabled; 0=disabled). 
                                                Usually, this should be set to 1. */
    int is_aof_enabled;                     /** < Indicating if aof function is enabled (1=enabled; 0=disabled). 
                                                Enabling this may slightly slow down the indexing performance, but enhancing the fault-tolerance. */
    char *dataset_path;                     /** <path to the dataset directory */
    char *index_dir_path;                   /** <path to the index file directory */
} indexing_config_t;

/**
 * Initializing metadata index with specified parallelism parameter.
 * 
 * @param parallelism Specified parallelism parameter.
 * @param rank The rank of the current process. If MPI is disabled, it should be 0.
 * @param size The number of all MPI processes. If MPI is disabled, it should be 1.
 * @param topk The number of first few files that needs to be scanned for indexing.
 * @param enable_mdb  Whether mdb functionality should be enabled. 1: enable. 0: disable.
 * @param enable_aof  Whether aof functionality should be enabled. 1: enable. 0: disable.
 * @param dataset_path path to the dataset directory.
 * @param index_dir_path path to the index file directory
 * @return An instance of indexing_config_t struct. 
 */
indexing_config_t *init_indexing_config(int parallelism, 
                                    int rank, 
                                    int size, 
                                    int topk,
                                    int enable_mdb, 
                                    int enable_aof,
                                    char *dataset_path,
                                    char *index_dir_path);


/**
 * Scan a collection of data and build the metadata index from scratch.
 * 
 * @param param                 an instance of indexing_config_t.
 * @return 0 success, 1 error raised.
 */
int indexing_data_collection(indexing_config_t *param);

// int save_index_change_to_aof(char *dir_path);

/**
 * Recover the in-memory index from the mdb files.
 * 
 * @param param                 an instance of indexing_config_t.
 * @return 0 success, 1 error raised.
 */
int recovering_index(indexing_config_t *param);

// int recovering_index_from_aof(char *aof_dir_path);