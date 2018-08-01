#include "fs_ops.h"


int init_dir_entry(const char *name, dir_entry_t *start_dir) {
    if (name == NULL || start_dir == NULL) {
        return 0;
    }
    if (strlen(name)>1023) {
        return 0;
    }

    char *original_cwd = (char *)calloc(1024, sizeof(char));
    char *specified_dir = (char *)calloc(1024, sizeof(char));

    getcwd(original_cwd, sizeof(original_cwd));

    
    int result = chdir(name);
    if (result!=0){
        switch(result){
            case EACCES: 
                perror("Permission denied\n"); return -1;
                break;
            case EIO:
                perror("An input output error occurred\n");return -1;
                break;
            case ENAMETOOLONG: 
                perror("Path is to long\n");return -1;
                break;
            case ENOTDIR: 
                perror("A component of path not a directory\n"); return -1;
                break;
            case ENOENT: 
                perror("No such file or directory\n"); fprintf(stderr, "enoent\n"); return -1;
                break;
            default: 
                fprintf(stderr, "Couldn't change directory to %s\n", name);return -1;
                break;
        }
    }
    
    getcwd(specified_dir, sizeof(specified_dir));
    chdir(original_cwd);
    
    char *buf = (char *)calloc(1024, sizeof(char));
    snprintf(buf, 1023, "%s", name);
    start_dir->name = buf;
    start_dir->canonical_path = specified_dir;
    start_dir->depth=0;
    start_dir->subdir_entries=NULL;
    start_dir->head = NULL;
    start_dir->next = NULL;
    start_dir->tail = NULL;
    return 1;
}

int deinit_dir_entry(dir_entry_t *entry){
    free(entry->name);
    free(entry->canonical_path);
    if (entry->subdir_entries) {
        dir_entry_t *cur = entry->subdir_entries;
        while (cur) {
            dir_entry_t *next = cur->next;
            deinit_dir_entry(cur);
            cur = next;
        }
    }
    free(entry);
    return 1;
}

void collect_dir(dir_entry_t *start_dir, int (*filter)(dir_entry_t *d_entry),
    int (*on_file)(dir_entry_t *f_entry), 
    int (*on_dir)(dir_entry_t *d_entry)){
    DIR *dir;
    struct dirent *entry;
    struct stat s_buf;
    
    if (!(dir = opendir(start_dir->name))) {
        return;
    }

    dir_entry_t *sub_dir_head = NULL;

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        stat(entry->d_name, &s_buf);
        dir_entry_t *last = sub_dir_head;
        sub_dir_head = (dir_entry_t *)calloc(1, sizeof(dir_entry_t));
        char *path = (char *)calloc(1024, sizeof(char));
        char *name = (char *)calloc(1024, sizeof(char));
        snprintf(name,1023, "%s", entry->d_name);
        snprintf(path, 1023, "%s/%s", start_dir->name, entry->d_name);
        sub_dir_head->name = name;
        sub_dir_head->canonical_path = path;
        sub_dir_head->depth = start_dir->depth + 1;
        if (filter!=NULL && filter(sub_dir_head)==0) {
                deinit_dir_entry(sub_dir_head);
                continue;
        }
        if (S_ISDIR(s_buf.st_mode)) {
            sub_dir_head->dir_type = DIR_ENTRY;
            if (on_dir) {
                on_dir(sub_dir_head);
            }
            collect_dir(sub_dir_head, filter, on_dir, on_file);
        } else if (S_ISREG(s_buf.st_mode)){
            sub_dir_head->dir_type = FILE_ENTRY;
            if (on_file) {
                on_file(sub_dir_head);
            }
        }
        if (last) {
            last->next = sub_dir_head;
            if (last->head) {
                last->head->tail = sub_dir_head;
                sub_dir_head->head = last->head;
            } else {
                last->head = sub_dir_head;
            }
        } else {
            sub_dir_head->head = sub_dir_head;
        }
    }
    start_dir->subdir_entries = sub_dir_head->head;
    closedir(dir);
}

// void listdir(const char *name, dir_entry_t *dir_entry)
// {
//     DIR *dir;
//     struct dirent *entry;

//     if (!(dir = opendir(name)))
//         return;

//     while ((entry = readdir(dir)) != NULL) {
//         if (entry->d_type == DT_DIR) {
//             char path[1024];
//             if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
//                 continue;
//             snprintf(path, sizeof(path), "%s/%s", name, entry->d_name);
//             printf("%*s[%s]\n", indent, "", entry->d_name);
//             listdir(path, indent + 2);
//         } else {
//             printf("%*s- %s\n", indent, "", entry->d_name);
//         }
//     }
//     closedir(dir);
// }
