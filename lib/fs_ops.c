#include "fs_ops.h"

// int init_dir_entry(const char *name, dir_entry_t *start_dir) {
//     if (name == NULL || start_dir == NULL) {
//         return 0;
//     }
//     if (strlen(name)>1023) {
//         return 0;
//     }

//     char *original_cwd = (char *)calloc(1024, sizeof(char));
//     char *specified_dir = (char *)calloc(1024, sizeof(char));

//     getcwd(original_cwd, sizeof(original_cwd));

//     int result = chdir(name);
//     if (result!=0){
//         switch(result){
//             case EACCES:
//                 perror("Permission denied\n");
//                 return -1;
//                 break;
//             case EIO:
//                 perror("An input output error occurred\n");
//                 return -1;
//                 break;
//             case ENAMETOOLONG:
//                 perror("Path is to long\n");
//                 return -1;
//                 break;
//             case ENOTDIR:
//                 perror("A component of path not a directory\n"); 
//                 return -1;
//                 break;
//             case ENOENT:
//                 perror("No such file or directory\n"); fprintf(stderr, "enoent\n"); 
//                 return -1;
//                 break;
//             default:
//                 fprintf(stderr, "Couldn't change directory to %s\n", name);
//                 return -1;
//                 break;
//         }
//     }
    
//     getcwd(specified_dir, sizeof(specified_dir));
//     chdir(original_cwd);
    
//     char *buf = (char *)calloc(1024, sizeof(char));
//     snprintf(buf, 1023, "%s", name);
//     start_dir->name = buf;
//     start_dir->canonical_path = specified_dir;
//     start_dir->depth=0;
//     start_dir->subdir_entries=NULL;
//     start_dir->head = NULL;
//     start_dir->next = NULL;
//     start_dir->tail = NULL;
//     return 1;
// }


void collect_dir(const char *dir_path, int (*filter)(struct dirent *entry),
    int (*on_file)(struct dirent *f_entry, void *args), 
    int (*on_dir)(struct dirent *d_entry, void *args), 
    void *coll_args, void *on_file_args, void *on_dir_args){

    if (dir_path == NULL) { // if the given start_dir is not a valid struct.
        return;
    }

    

    DIR *dir;
    // struct stat s_buf;
    
    if (!(dir = opendir(dir_path))) { // if the given dir entry cannot be open.
        return;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            // Not visiting current directory and parental directory
            continue;
        }
        char *path = (char *)calloc(1024, sizeof(char));
        char *name = (char *)calloc(1024, sizeof(char));
        snprintf(name,1023, "%s", entry->d_name);
        snprintf(path, 1023, "%s/%s", dir_path, entry->d_name);

        // printf("entry = %s\n", path);

        if (filter!=NULL && filter(entry)==0) {
                continue;
        }
   
        if (entry->d_type == DT_DIR) {
            // printf("dir: %s\n", path);
            if (on_dir) {
                on_dir(entry, on_dir_args);
            }
            collect_dir(path, filter, on_dir, on_file, coll_args, on_file_args, on_dir_args);
        } else {
            if (on_file) {
                on_file(entry, on_file_args);
            }
        }
    }
    closedir(dir);
}