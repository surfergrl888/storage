#ifndef __CLOUDFS_DEDUP_H_
#define __CLOUDFS_DEDUP_H_

#include "uthash.h"
#include <openssl/md5.h>
#include <fuse.h>

#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024

extern int max_seg_size;

struct segment_hash_struct {
  char hash[MD5_DIGEST_LENGTH*2+1];
  int length;
  int ref_count;
  UT_hash_handle hh;
};

int get_segment_size(char *hash);

void dedup_init();
void dedup_destroy();
int dedup_migrate_file(const char *path, struct fuse_file_info *file_info,
                       int in_ssd, char move_entire_file);
int dedup_read(const char *path, char *buffer, size_t size,
               off_t offset, struct fuse_file_info *file_info);
int dedup_get_last_segment(const char *data_target_path, int meta_file);
int dedup_unlink_segments(const char *meta_path);

#endif
