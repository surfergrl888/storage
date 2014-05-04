#ifndef __CLOUDFS_DEDUP_H_
#define __CLOUDFS_DEDUP_H_

#include "uthash.h"
#include <openssl/md5.h>
#include <fuse.h>

#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024

extern int max_seg_size;

/* This is the struct used for the hash table of segments, as implemented by
 * uthash.h
 */
struct segment_hash_struct {
  char hash[MD5_DIGEST_LENGTH*2+1];
  int length;
  int ref_count;
  UT_hash_handle hh;
};

int get_segment_size(char *hash);

/* dedup_init: Initializes rabin, as well as the cache. It also restores the
 * segment hash table and cache if they were initialized in a previout mount
 */
void dedup_init();

/* dedup_destroy: Deinitializes rabin */
void dedup_destroy();

/* dedup_migrate_file: Breaks a file into segments, compresses them (if 
 * applicable) and migrates them to the cloud.
 * 
 * path: The path to the file (relative to the mount point)
 * file_info: The fuse_file_info struct relating to the open file
 * in_ssd: Whether the file is stored on the ssd or the cloud
 * 
 * returns: 0 on success, -1 on failure
 */
int dedup_migrate_file(const char *path, struct fuse_file_info *file_info,
                       int in_ssd);

/* dedup_read: Reads a deduplicated file by pulling the segments we need from
 * the cloud and reading them.
 * 
 * path: The path to the file (relative to the mount point)
 * buffer: The buffer to put the data
 * size: The amount of data to read
 * offset: The offset into the file at which to begin reading
 * 
 * returns: -1 on failure, the total number of bytes read on success
 */
int dedup_read(const char *path, char *buffer, size_t size,
               off_t offset);

/* dedup_get_last_segment: Pulls the last segment of a file from the cloud
 * (and removes it from the file's mappings); used for writing to a file
 *
 * data_target_path: The full path of the file in which to put the segment
 * meta_file: An open file descriptor for the metadata file
 * 
 * returns: 0 on success, -1 on failure
 */
int dedup_get_last_segment(const char *data_target_path, int meta_file);

/* dedup_unlink_segments: Deletes a file's segments from the hash table,
 * and, if necessary, from the cloud and the cache.
 * 
 * meta_path: The full path of the metadata file
 * 
 * returns: 0 on success, -1 on failure
 */
int dedup_unlink_segments(const char *meta_path);

#endif
