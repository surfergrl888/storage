#ifndef __CLOUDFS_H_
#define __CLOUDFS_H_

#include "uthash.h"

//#define DEBUG
#define LOGGING_ENABLED
#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024
extern struct cloudfs_state state_;
extern int infile;
extern FILE *log_file;
extern int outfile;

struct cloudfs_state {
  char ssd_path[MAX_PATH_LEN];
  char fuse_path[MAX_PATH_LEN];
  char hostname[MAX_HOSTNAME_LEN];
  int ssd_size;
  int threshold;
  int avg_seg_size;
  int rabin_window_size;
  int cache_size;
  char no_dedup;
  char no_cache;
  char no_compress;
};

struct reference_struct {
  ino_t inode;
  int ref_count;
  UT_hash_handle hh;
};

int get_buffer(const char *buffer, int bufferLength);
int put_buffer(char *buffer, int bufferLength);
int bucket_exists(char *bucket);
void log_write(char *to_write);

int cloudfs_start(struct cloudfs_state* state,
                  const char* fuse_runtime_name);  
char *cloudfs_get_fullpath(const char *path);
char *cloudfs_get_metadata_fullpath(const char *path);
char *cloudfs_get_data_fullpath(const char *path);
#endif
