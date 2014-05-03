/* cloudfs.c
 * 
 * This file implements the basic (part 1) functionality of cloudfs.
 * 
 * Metadata is stored in a hidden file, "/.[inode # of the original file]".
 * We only store the file size and timestamps there, since everything else is
 * kept on the original SSD file, which is truncated to 0 bytes when we
 * migrate the data to the cloud.  We only make a metadata file for data
 * migrated to the cloud, so the way we know if a file is stored on the SSD
 * or the cloud is by checking for the existence of the metadata file. The
 * inode number is used for naming since it's a unique identifier.  We could
 * just use the path, but then we would have to worry about directory
 * permissions or making directories (depending on the implementation), so
 * using the inode number allows us to put the metadata in the root directory,
 * where we know we can write, without having to worry about collisions in
 * naming.
 * For parts 2+3, the hash mappings are stored in the metadata file immediately
 * after the timestamps.
 * 
 * Cloud file data is stored in /.[inode # of the original file]_data. For part
 * 1, this is the entire file, and for parts 2 and 3, this is the end of the
 * file, which we're modifying. [the cache is separate, see cloudfs_cache.c]
 */

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <utime.h>
#include <unistd.h>
#include "cloudapi.h"
#include "cloudfs_dedup.h"
#include "uthash.h"
#include "cloudfs.h"
#include "dedup.h"

#define UNUSED __attribute__((unused))
#define SUCCESS 0
// Autolab debugging version
//#define LOGFILE "./cloudfs.log"
// local debugging version
#define LOGFILE "/home/guest/storage/src/scripts/cloudfs.log"
#define META_TIMESTAMPS sizeof(off_t)
#define UTIME_NOW	((1l << 30) - 1l)
#define UTIME_OMIT	((1l << 30) - 2l)
#define META_ATIME_OFFSET META_TIMESTAMPS
#define META_MTIME_OFFSET META_TIMESTAMPS+sizeof(time_t)
#define META_ATTRTIME_OFFSET META_MTIME_OFFSET+sizeof(time_t)

struct cloudfs_state state_;
int infile, outfile;
struct reference_struct *reference_counts = NULL;
int bucketExists;
FILE *log_file;
char *bucketToCheck;

int get_buffer(const char *buffer, int bufferLength) {
  return write(outfile, buffer, bufferLength);  
}

int put_buffer(char *buffer, int bufferLength) {
  //fprintf(stdout, "put_buffer %d \n", bufferLength);
  return read(infile, buffer, bufferLength);
}

int check_bucket(const char *bucketName) {
  if (strcmp(bucketName, bucketToCheck) == 0)
    bucketExists = 1;
  return 0; 
}

void log_write(char *to_write) {
  
  if (log_file == 0)
    return;
  fprintf(log_file, "%s", to_write);
  fflush(log_file);
}

int bucket_exists(char *bucket) {
  bucketExists = 0;
  bucketToCheck = bucket;
  cloud_list_service(check_bucket);
  return bucketExists;
}

int get_weak_hash(const char *path)
{
  unsigned int i;
  int sum = 0;;
  
  for (i=0; i<strlen(path); i++) {
    if (path[i] == '+') {
      sum += i;
    }
  }
  return sum;
}

char *get_s3_key(const char *path) {
  unsigned int i;
  
  char *key = malloc(strlen(path)+1);
  strcpy(key, path);
  
  for (i=0; i<strlen(path); i++) {
    if (path[i] == '/') {
      key[i] = '+';
    }
  }
  
  return key;
}

char *cloudfs_get_fullpath(const char *path)
{
  char *fullpath = malloc(strlen(state_.ssd_path)+strlen(path)+1);
  
  strcpy(fullpath, state_.ssd_path);
  strcat(fullpath, path+1);
  
  return fullpath;
}

char *cloudfs_get_metadata_fullpath(const char *path)
{
  struct stat info;
  char *fullpath = cloudfs_get_fullpath(path);
  
  stat(fullpath, &info);
  free(fullpath);
  fullpath = malloc(strlen(state_.ssd_path)+2+sizeof(ino_t)/2 );
  
  sprintf(fullpath, "%s.%lx", state_.ssd_path, (unsigned long int)info.st_ino);
  
  return fullpath;
}

char *cloudfs_get_data_fullpath(const char *path)
{
  char *fullpath = cloudfs_get_metadata_fullpath(path);
  fullpath = realloc(fullpath, strlen(fullpath)+1+strlen("_data"));
  
  strcat(fullpath, "_data");
  return fullpath;
}

static int UNUSED cloudfs_error(char *error_str)
{
    int retval = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //
    
    fprintf(stderr, "CloudFS Error: %s\n", error_str);

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return retval;
}

/*
 * Initializes the FUSE file system (cloudfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED)
{
  cloud_init(state_.hostname);
  #ifdef LOGGING_ENABLED
  log_file = fopen(LOGFILE, "a+");
  #endif
  if (!state_.no_dedup) {
    dedup_init();
  }
  return NULL;
}

void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
  if (!state_.no_dedup) {
    dedup_destroy();
  }
  #ifdef LOGGING_ENABLED
  fclose(log_file);
  #endif
}

/* Directory operations */

int cloudfs_mkdir(const char *path, mode_t mode)
{
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err =  mkdir(fullpath, mode);
  free(fullpath);
  
  if (err) {
    return -errno;
  }
  return SUCCESS;
}

int cloudfs_opendir(const char *path, struct fuse_file_info *file_info)
{
  DIR *dir;
  
  char *fullpath = cloudfs_get_fullpath(path);
  dir =  opendir(fullpath);
  free(fullpath);
  if (dir == NULL)
    return -errno;
  
  file_info->fh = (uintptr_t)dir;
  return SUCCESS;
}

int cloudfs_readdir(const char *path UNUSED, void *buf, fuse_fill_dir_t filler,
                    off_t offset UNUSED, struct fuse_file_info *file_info)
{
  DIR *dir;
  struct dirent *dir_entry;
  
  dir = (DIR *)(file_info->fh);
  
  dir_entry = readdir(dir);
  if (dir_entry == NULL)
    return -errno;
  while (dir_entry != NULL) {
    if (filler(buf, dir_entry->d_name, NULL, 0) != 0) {
      return -ENOMEM;
    }
    dir_entry = readdir(dir);
  }
  
  return SUCCESS;
}

int cloudfs_releasedir(const char *path UNUSED,
                       struct fuse_file_info *file_info)
{
  int err;
  
  err =  closedir((DIR *)(file_info->fh));
  
  if (err)
    return -errno;
  
  return SUCCESS;
}

int cloudfs_rmdir(const char *path)
{
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = rmdir(fullpath);
  free(fullpath);
  
  if (err)
    return -errno;
  return SUCCESS;
}

/* Metadata operations */

int cloudfs_chmod(const char *path, mode_t mode)
{
  struct timespec cur_time;
  int err, meta_file;
  struct stat info;
  
  #ifdef DEBUG
    printf("call to chmod: %s\n", path);
  #endif
  char *fullpath = cloudfs_get_fullpath(path);
  err = chmod(fullpath, mode);
  if (err) {
    free(fullpath);
    return -errno;
  }
  stat(fullpath, &info);
  free(fullpath);
  if (S_ISDIR(info.st_mode)) {
    return SUCCESS;
  }
  fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(fullpath, &info);
  if (err && (errno == ENOENT)) {
    free(fullpath);
    return SUCCESS;
  }
  meta_file = open(fullpath, O_WRONLY);
  free(fullpath);
  if (meta_file < 0) {
    return -errno;
  }
  err = lseek(meta_file, META_ATTRTIME_OFFSET, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    return -errno;
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) != sizeof(time_t)){
    close(meta_file);
    return -errno;
  }
  close(meta_file);
  return SUCCESS;
}

int cloudfs_access(const char *path, int how) {
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = access(fullpath, how);
  free(fullpath);
  
  if (err)
    return -errno;
  return SUCCESS;
}

// For all files we store metadata at dir/otherdir/.../.filename
// All files begin with a single char that specifies the location of the data.
// For files small enough for the SSD, we just use the normal file's metadata.
// 
// Migrated files (cloud-stored files), however, are different:
// 
// The access information, userID, device, and groupID are all stored in the
// proxy file, to save space, since we keep a proxy of the file in the
// filesystem to make directory operations really easy.  So, the metadata
// file contains the timestamps and size.  WE can easily infer the number of
// blocks from the size so there's no need to waste space on it.
int cloudfs_getattr(const char *path, struct stat *statbuf)
{
  int err;
  int metadata_file;
  struct stat temp;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = stat(fullpath, statbuf);
  free(fullpath);
  
  #ifdef DEBUG
    printf("call to getattr: %s\n", path);
  #endif
  if (err)
    return -errno;
  if (!S_ISDIR(statbuf->st_mode)) {
    fullpath = cloudfs_get_metadata_fullpath(path);
    err = stat(fullpath, &temp);
    if (err && (errno == ENOENT)) {
      free(fullpath);
      return SUCCESS;
    }
    metadata_file = open(fullpath, O_RDONLY);
    free(fullpath);
    if (metadata_file < 0) {
      return -errno;
    }
    if (read(metadata_file, &(statbuf->st_size), sizeof(off_t)) != 
        sizeof(off_t)) {
      #ifdef DEBUG
      printf("Error with metadata - getting size!\n");
      #endif
      close(metadata_file);
      return -errno;
    }
    if (read(metadata_file, &(statbuf->st_atime), sizeof(time_t)) !=
        sizeof(time_t)){
      #ifdef DEBUG
      printf("Error with metadata - getting timestamps!\n");
      #endif
      close(metadata_file);
      return -errno;
    }
    if (read(metadata_file, &(statbuf->st_mtime), sizeof(time_t)) !=
        sizeof(time_t)){
      #ifdef DEBUG
      printf("Error with metadata - getting timestamps!\n");
      #endif
      close(metadata_file);
      return -errno;
    }
    if (read(metadata_file, &(statbuf->st_ctime), sizeof(time_t)) !=
        sizeof(time_t)){
      #ifdef DEBUG
      printf("Error with metadata - getting timestamps!\n");
      #endif
      close(metadata_file);
      return -errno;
    }
    statbuf->st_blocks = statbuf->st_size/512;
    close(metadata_file);
  }
  
  return SUCCESS;
}

int cloudfs_getxattr(const char *path, const char *name, char *value,
                      size_t size)
{
  int err;
  
  #ifdef DEBUG
    printf("call to getxattr: %s\n", path);
  #endif
  char *fullpath = cloudfs_get_fullpath(path);
  err = lgetxattr(fullpath, name, value, size);
  free(fullpath);
  
  if (err)
    return -errno;
  return SUCCESS;
}

int cloudfs_setxattr(const char *path, const char *name, const char *value,
                      size_t size, int flags)
{
  struct timespec cur_time;
  int err, meta_file;
  struct stat info;
  
  #ifdef DEBUG
    printf("call to setxattr: %s\n", path);
  #endif
  char *fullpath = cloudfs_get_fullpath(path);
  err = lsetxattr(fullpath, name, value, size, flags);
  if (err) {
    free(fullpath);
    return -errno;
  }
  stat(fullpath, &info);
  free(fullpath);
  if (S_ISDIR(info.st_mode)) {
    return SUCCESS;
  }
  fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(fullpath, &info);
  if (err && (errno == ENOENT)) {
    free(fullpath);
    return SUCCESS;
  }
  meta_file = open(fullpath, O_RDWR);
  free(fullpath);
  if (meta_file < 0) {
    return -1;
  }
  err = lseek(meta_file, META_ATTRTIME_OFFSET, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    return -errno;
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) != sizeof(time_t)){
    close(meta_file);
    return -errno;
  }
  close(meta_file);
  return SUCCESS;
}

int cloudfs_utimens(const char *path, const struct timespec tv[2]) {
  int err;
  struct timespec cur_time;
  struct timeval time_temp[2];
  char *meta_fullpath;
  struct stat statbuf;
  int metadata_file;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = stat(fullpath, &statbuf);
  
  #ifdef DEBUG
    printf("call to utimens: %s\n", path);
  #endif
  if (err)
    return -errno;
  if (S_ISDIR(statbuf.st_mode)) {
    time_temp[0].tv_sec = tv[0].tv_sec;
    time_temp[1].tv_sec = tv[1].tv_sec;
    time_temp[0].tv_usec = tv[0].tv_nsec/1000;
    time_temp[1].tv_usec = tv[1].tv_nsec/1000;
    err = utimes(fullpath, time_temp);
    free(fullpath);
    if (err)
      return -errno;
    return SUCCESS;
  }
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &statbuf);
  if (err && (errno == ENOENT)) {
    time_temp[0].tv_sec = tv[0].tv_sec;
    time_temp[1].tv_sec = tv[1].tv_sec;
    time_temp[0].tv_usec = tv[0].tv_nsec/1000;
    time_temp[1].tv_usec = tv[1].tv_nsec/1000;
    err = utimes(fullpath, time_temp);
    free(fullpath);
    free(meta_fullpath);
    if (err)
      return -errno;
    return SUCCESS;
  }
  metadata_file = open(meta_fullpath, O_RDWR);
  free(meta_fullpath);
  free(fullpath);
  if (metadata_file < 0) {
    return -1;
  }
  err = lseek(metadata_file, META_TIMESTAMPS, SEEK_SET);
  if (err < 0) {
    close(metadata_file);
    return -errno;
  }
  if (tv[0].tv_nsec == UTIME_OMIT) {
    err = lseek(metadata_file, sizeof(time_t), SEEK_CUR);
    if (err < 0) {
      close(metadata_file);
      return -errno;
    }
  }
  else {
    if (tv[0].tv_nsec == UTIME_NOW) {
      err = clock_gettime(CLOCK_REALTIME, &cur_time);
      if (err) {
        close(metadata_file);
        return -errno;
      }
      if (write(metadata_file, &(cur_time.tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
    }
    else {
      if (write(metadata_file, &(tv[0].tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
    }
  }
  if (tv[1].tv_nsec == UTIME_OMIT) {
    close(metadata_file);
    return SUCCESS;
  }
  else {
    if (tv[1].tv_nsec == UTIME_NOW) {
      err = clock_gettime(CLOCK_REALTIME, &cur_time);
      if (err) {
        close(metadata_file);
        return -errno;
      }
      if (write(metadata_file, &(cur_time.tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
    }
    else {
      if (write(metadata_file, &(tv[1].tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
    }
    close(metadata_file);
    return SUCCESS;
  }
}

/* File creation/deletion */

int cloudfs_mknod(const char *path, mode_t mode, dev_t dev) {
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  
  #ifdef DEBUG
    printf("call to mknod: %s\n", path);
  #endif
  err = mknod(fullpath, mode, dev);
  
  if (err) {
    #ifdef DEBUG
      printf("Error making file: %d\n", err);
    #endif
    free(fullpath);
    return -errno;
  }
  free(fullpath);
  return SUCCESS;
}

int cloudfs_unlink(const char *path) {
  char *fullpath, *meta_fullpath, *data_fullpath;
  struct stat temp;
  char *s3_key;
  int err;
  char s3_bucket[11];
  #ifdef LOGGING_ENABLED
  char log_string[100];
  #endif
  
  #ifdef DEBUG
    printf("call to unlink: %s\n", path);
  #endif
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "call to unlink: path=%s\n", path);
  log_write(log_string);
  #endif
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &temp);
  if (!(err && (errno == ENOENT))) {
    if (state_.no_dedup) {
      s3_key = get_s3_key(path);
      sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path)+100);
      cloud_delete_object(s3_bucket, s3_key);
      free(s3_key);
    }
    else {
      if (dedup_unlink_segments(meta_fullpath)) {
        free(meta_fullpath);
        return -errno;
      }
    }
    data_fullpath = cloudfs_get_data_fullpath(path);
    err = stat(data_fullpath, &temp);
    if (!(err && (errno == ENOENT))) {
      unlink(data_fullpath);
    }
    free(data_fullpath);
    unlink(meta_fullpath);
  }
  
  
  fullpath = cloudfs_get_fullpath(path);
  unlink(fullpath);
  free(meta_fullpath);
  free(fullpath);
  
  return SUCCESS;
}

/* File I/O */

int cloudfs_read(const char *path, char *buffer, size_t size,
                 off_t offset, struct fuse_file_info *file_info)
{
  int err, meta_file, data_file;
  char *meta_fullpath, *fullpath;
  size_t retval = 0;
  struct timespec cur_time;
  #ifdef LOGGING_ENABLED
  char log_string[100];
  #endif
  struct stat temp;
  
  #ifdef DEBUG
    printf("call to read: %s\n", path);
  #endif
  if (state_.no_dedup) {
    err = lseek(file_info->fh, offset, SEEK_SET);
    if (err < 0) {
      return -errno;
    }
    
    retval = read(file_info->fh, buffer, size);
    if ((signed int)retval == -1) {
      return -errno;
    }
  }
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &temp);
  if (err && (errno == ENOENT)) {
    free(meta_fullpath);
    if (!state_.no_dedup) {
      if ((signed int)file_info->fh < 0) {
        fullpath = cloudfs_get_fullpath(path);
        data_file = open(fullpath, O_RDONLY);
        free(fullpath);
        if (data_file < 0) {
          #ifdef LOGGING_ENABLED
          sprintf(log_string, "read failure 0: path=%s, errno=%d\n", path,errno);
          log_write(log_string);
          #endif
          return -errno;
        }
      }
      else {
        data_file = file_info->fh;
      }
      err = lseek(data_file, offset, SEEK_SET);
      if (err < 0) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "read failure 1: path=%s, errno=%d\n", path,errno);
        log_write(log_string);
        #endif
        close(data_file);
        return -errno;
      }
      
      retval = read(data_file, buffer, size);
      if ((signed int)retval == -1) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "read failure 2: path=%s, errno=%d\n", path,errno);
        log_write(log_string);
        #endif
        close(data_file);
        return -errno;
      }
      if (data_file != (signed int)file_info->fh)
        close(data_file);
    }
    return retval;
  }
  if (!state_.no_dedup) {
    retval = dedup_read(path, buffer, size, offset);
    if ((signed int)retval == -1) {
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "read failure 3: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
  }
  meta_file = open(meta_fullpath, O_WRONLY);
  free(meta_fullpath);
  if (meta_file < 0) {
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "read failure 4: path=%s, errno=%d\n", path, errno);
    log_write(log_string);
    #endif
    return -errno;
  }
  err = lseek(meta_file, META_ATIME_OFFSET, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "read failure 5: path=%s, errno=%d\n", path, errno);
    log_write(log_string);
    #endif
    return -errno;
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) != sizeof(time_t)){
    close(meta_file);
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "read failure 6: path=%s, errno=%d\n", path, errno);
    log_write(log_string);
    #endif
    return -errno;
  }
  close(meta_file);
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "cloud read done: path=%s, total_bytes_read=%d\n", path,
          retval);
  log_write(log_string);
  #endif
  return retval;
}

int cloudfs_write(const char *path, const char *buffer, size_t size,
                  off_t offset, struct fuse_file_info *file_info)
{
  int err, meta_file, i, in_ssd;
  #ifdef LOGGING_ENABLED
  char log_string[100];
  #endif
  char *meta_fullpath, *data_fullpath;
  struct stat info;
  size_t retval = 0;
  struct timespec cur_time;
  off_t new_size;
  
  #ifdef DEBUG
    printf("call to write: %s\n", path);
  #endif
  if (state_.no_dedup) {
    err = lseek(file_info->fh, offset, SEEK_SET);
    if (err < 0) {
      return -errno;
    }
    
    retval = write(file_info->fh, buffer, size);
    if ((signed int)retval == -1) {
      return -errno;
    }
  }
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &info);
  in_ssd = (err && (errno == ENOENT));
  if (in_ssd) {
    free(meta_fullpath);
    if (!state_.no_dedup) {
      if ((signed int)file_info->fh < 0) {
        return -EBADF;
      }
      err = lseek(file_info->fh, offset, SEEK_SET);
      if (err < 0) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "write failure 1: path=%s, errno=%d\n", path,
                errno);
        log_write(log_string);
        #endif
        return -errno;
      }
      
      retval = write(file_info->fh, buffer, size);
      if ((signed int)retval == -1) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "write failure 2: path=%s, errno=%d\n", path,
                errno);
        log_write(log_string);
        #endif
        return -errno;
      }
      
      /*fstat(file_info->fh, &info);
      if ((info.st_size > state_.threshold) &&
          (info.st_size >= max_seg_size)) {
        close(file_info->fh);
        data_fullpath = cloudfs_get_fullpath(path);
        file_info->fh = open(data_fullpath, O_RDWR);
        free(data_fullpath);
        if (dedup_migrate_file(path, file_info, in_ssd, 0))
          return -errno;
        
      }*/
    }
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "ssd write done: path=%s, bytes_written=%d\n", path, retval);
    log_write(log_string);
    #endif
    return retval;
  }
  meta_file = open(meta_fullpath, O_RDWR);
  free(meta_fullpath);
  if (meta_file < 0)
    return -errno;
  if (state_.no_dedup) {
    err = fstat(file_info->fh, &info);
    if (err) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 4: path=%s, errno=%d\n", path, errno);
      log_write(log_string);
      #endif
      return -errno;
    }
    if (write(meta_file, &(info.st_size), sizeof(off_t)) != sizeof(off_t)) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 5: path=%s, errno=%d\n", path, errno);
      log_write(log_string);
      #endif
      return -errno;
    }
  }
  else {
    if ((signed int)file_info->fh < 0) {
      data_fullpath = cloudfs_get_data_fullpath(path);
      err = stat(data_fullpath, &info);
      if (err && (errno == ENOENT)) {
        if (dedup_get_last_segment(data_fullpath, meta_file)) {
          close(meta_file);
          free(data_fullpath);
          #ifdef LOGGING_ENABLED
          sprintf(log_string, "write failure 6: path=%s, errno=%d\n", path,
                  errno);
          log_write(log_string);
          #endif
          return -errno;
        }
      }
      file_info->fh = open(data_fullpath, O_RDWR);
      free(data_fullpath);
      if ((signed int)file_info->fh < 0) {
        close(meta_file);
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "write failure 7: path=%s, errno=%d\n", path,
                errno);
        log_write(log_string);
        #endif
        return -errno;
      }
    }
    err = lseek(file_info->fh, 0, SEEK_END);
    if (err < 0) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 8: path=%s, errno=%d\n", path, errno);
      log_write(log_string);
      #endif
      return -errno;
    }
    
    retval = write(file_info->fh, buffer, size);
    if ((signed int)retval == -1) {
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 9: path=%s, errno=%d\n", path, errno);
      log_write(log_string);
      #endif
      close(meta_file);
      return -errno;
    }
    
    fstat(file_info->fh, &info);
    /*if (info.st_size >= max_seg_size) {
      err = lseek(file_info->fh, 0, SEEK_SET);
      if (err < 0) {
        close(meta_file);
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "write failure 10: path=%s, errno=%d\n", path,
                errno);
        log_write(log_string);
        #endif
        return -errno;
      }
      if (dedup_migrate_file(path, file_info, in_ssd, 0)) {
        close(meta_file);
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "write failure 11: path=%s, errno=%d\n", path,
                errno);
        log_write(log_string);
        #endif
        return -errno;
      }
    }*/
    err = lseek(meta_file, 0, SEEK_SET);
    if (err < 0) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 12: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
    if (read(meta_file, &new_size, sizeof(off_t)) != sizeof(off_t)) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 13: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
    err = lseek(meta_file, 0, SEEK_SET);
    if (err < 0) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 14: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
    new_size += size;
    if (write(meta_file, &new_size, sizeof(off_t)) != sizeof(off_t)) {
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 15: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  for (i=0; i<3; i++) {
    if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) !=
        sizeof(time_t)){
      close(meta_file);
      #ifdef LOGGING_ENABLED
      sprintf(log_string, "write failure 16: path=%s, errno=%d\n", path,errno);
      log_write(log_string);
      #endif
      return -errno;
    }
  }
  close(meta_file);
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "cloud write done: path=%s, bytes_written=%d\n", path,
          retval);
  log_write(log_string);
  #endif
  return retval;
}

int cloudfs_open(const char *path, struct fuse_file_info *file_info)
{
  char *meta_fullpath, *data_fullpath, *s3_key = NULL;
  struct reference_struct *reference_count;
  struct stat info, temp;
  S3Status status;
  char s3_bucket[11];
  int err, already_in_ssd;
  #ifdef LOGGING_ENABLED
  char log_string[100];
  #endif
  
  #ifdef DEBUG
    printf("call to open: %s\n", path);
  #endif
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "call to open: path=%s\n", path);
  log_write(log_string);
  #endif
  // The first thing we do is check the permissions, which are stored with the
  // proxy file
  char *fullpath = cloudfs_get_fullpath(path);
  if ((file_info->flags & 3) == O_RDONLY) {
    if (access(fullpath, R_OK)) {
      free(fullpath);
      return -errno;
    }
  }
  else if (file_info->flags & O_WRONLY) {
    if (access(fullpath, W_OK)) {
      free(fullpath);
      return -errno;
    }
  }
  else if (file_info->flags & O_RDWR) {
    if (access(fullpath, R_OK | W_OK)) {
      free(fullpath);
      return -errno;
    }
  }
  stat(fullpath, &info);
  free(fullpath);
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &temp);
  if (err && (errno == ENOENT)) {
    free(meta_fullpath);
    if (state_.no_dedup && ((file_info->flags & 3) == O_RDONLY)) {
      file_info->fh = -1;
      return SUCCESS;
    }
    fullpath = cloudfs_get_fullpath(path);
    file_info->fh = open(fullpath, file_info->flags);
    free(fullpath);
    
    if ((signed int)file_info->fh < 0)
       return -errno;
  }
  else {
    free(meta_fullpath);
    if (state_.no_dedup) {
      data_fullpath = cloudfs_get_data_fullpath(path);
      err = stat(data_fullpath, &info);
      already_in_ssd = !(err && (errno == ENOENT));
      file_info->fh = open(data_fullpath, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      free(data_fullpath);
      if ((signed int)file_info->fh < 0)
        return -errno;
      
      if (!already_in_ssd) {
        outfile = file_info->fh;
        sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path)+100);
        s3_key = get_s3_key(path);
        status = cloud_get_object(s3_bucket, s3_key, get_buffer);
        if (status != S3StatusOK) {
          #ifdef DEBUG
            cloud_print_error();
          #endif
          close(file_info->fh);
          free(s3_key);
          return -1;
        }
        free(s3_key);
      }
    }
    else
      file_info->fh = -1;
  }
  if (!state_.no_dedup && ((file_info->flags & 3) == O_RDONLY)) {
    return SUCCESS;
  }
  HASH_FIND(hh, reference_counts,&(info.st_ino),sizeof(ino_t),reference_count);
  if (reference_count == NULL) {
    reference_count = malloc(sizeof(struct reference_struct));
    reference_count->ref_count = 1;
    reference_count->inode = info.st_ino;
    HASH_ADD(hh, reference_counts, inode, sizeof(ino_t), reference_count);
  }
  else {
    reference_count->ref_count++;
  }
  return SUCCESS;
}

int cloudfs_release(const char *path, struct fuse_file_info *file_info)
{
  char *meta_fullpath, *data_fullpath, *s3_key;
  struct reference_struct *reference_count;
  struct stat info, temp;
  S3Status status;
  char s3_bucket[11];
  int meta_file;
  int err, in_ssd;
  #ifdef LOGGING_ENABLED
  char log_string[100];
  #endif
  
  #ifdef DEBUG
    printf("call to release: %s\n", path);
  #endif
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "call to release: path=%s\n", path);
  log_write(log_string);
  #endif
  if (!state_.no_dedup && ((file_info->flags & 3) == O_RDONLY)) {
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "release exit 1\n");
    log_write(log_string);
    #endif
    return SUCCESS;
  }
  data_fullpath = cloudfs_get_fullpath(path);
  stat(data_fullpath, &info);
  free(data_fullpath);
  HASH_FIND(hh, reference_counts,&(info.st_ino),sizeof(ino_t),reference_count);
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  err = stat(meta_fullpath, &temp);
  in_ssd = (err && (errno == ENOENT));
  if ((signed int)file_info->fh >= 0)
    fstat(file_info->fh, &info);
  #ifdef LOGGING_ENABLED
  sprintf(log_string, "basic file info: in_ssd=%d, ref_cnt=%d, size=%ld\n", in_ssd, reference_count->ref_count, (long)info.st_size);
  log_write(log_string);
  #endif
  if ((reference_count->ref_count > 1) || (in_ssd &&
                                           (info.st_size <= state_.threshold))) {
    #ifdef LOGGING_ENABLED
    sprintf(log_string, "release exit 2\n");
    log_write(log_string);
    #endif
    reference_count->ref_count--;
    free(meta_fullpath);
    if ((signed int)file_info->fh >= 0)
      close(file_info->fh);
    return SUCCESS;
  }
  if (state_.no_dedup) {
    sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path)+100);
    s3_key = get_s3_key(path);
    if (in_ssd) {
      data_fullpath = cloudfs_get_fullpath(path);
      if (!bucket_exists(s3_bucket)) {
        cloud_create_bucket(s3_bucket);
      }
    }
    else {
      data_fullpath = cloudfs_get_data_fullpath(path);
    }
    lseek(file_info->fh, 0, SEEK_SET);
    infile = open(data_fullpath, O_RDONLY);
    free(data_fullpath);
    if (infile < 0) {
      free(s3_key);
      return -1;
    }
    status = cloud_put_object(s3_bucket, s3_key, info.st_size, put_buffer);
    if (status != S3StatusOK) {
      #ifdef DEBUG
        cloud_print_error();
      #endif
      free(s3_key);
      close(infile);
      return -1;
    }
    free(s3_key);
    close(infile);
    if (in_ssd) {
      meta_file = open(meta_fullpath, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (meta_file < 0) {
        return -errno;
      }
      if (write(meta_file, &(info.st_size), sizeof(off_t)) != sizeof(off_t)) {
        close(meta_file);
        unlink(meta_fullpath);
        free(meta_fullpath);
        return -errno;
      }
      if (write(meta_file, &(info.st_atime), sizeof(time_t)) != sizeof(time_t)) {
        close(meta_file);
        unlink(meta_fullpath);
        free(meta_fullpath);
        return -errno;
      }
      if (write(meta_file, &(info.st_mtime), sizeof(time_t)) != sizeof(time_t)) {
        close(meta_file);
        unlink(meta_fullpath);
        free(meta_fullpath);
        return -errno;
      }
      if (write(meta_file, &(info.st_ctime), sizeof(time_t)) != sizeof(time_t)) {
        close(meta_file);
        unlink(meta_fullpath);
        free(meta_fullpath);
        return -errno;
      }
      close(meta_file);
      data_fullpath = cloudfs_get_fullpath(path);
      if (truncate(data_fullpath, 0)) {
        free(data_fullpath);
        unlink(meta_fullpath);
        free(meta_fullpath);
        return -errno;
      }
      free(data_fullpath);
      free(meta_fullpath);
      close(file_info->fh);
    }
    else {
      free(meta_fullpath);
      close(file_info->fh);
      data_fullpath = cloudfs_get_data_fullpath(path);
      unlink(data_fullpath);
      free(data_fullpath);
    }
  }
  else {
    free(meta_fullpath);
    if (in_ssd) {
      close(file_info->fh);
      data_fullpath = cloudfs_get_fullpath(path);
      file_info->fh = open(data_fullpath, O_RDWR);
      free(data_fullpath);
      if ((signed int)file_info->fh < 0) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "release failure 1: errno=%d\n", errno);
        log_write(log_string);
        #endif
        return -1;
      }
    }
    else {
      data_fullpath = cloudfs_get_data_fullpath(path);
      err = stat(data_fullpath, &temp);
      if (err && (errno == ENOENT)) {
        #ifdef LOGGING_ENABLED
        sprintf(log_string, "release exit 3\n");
        log_write(log_string);
        #endif
        if ((signed int)file_info->fh >= 0) {
          close(file_info->fh);
        }
        free(data_fullpath);
        HASH_DEL(reference_counts, reference_count);
        free(reference_count);
        return SUCCESS;
      }
      if ((signed int)file_info->fh < 0) {
        file_info->fh = open(data_fullpath, O_RDWR);
        if ((signed int)file_info->fh < 0) {
          #ifdef LOGGING_ENABLED
          sprintf(log_string, "release failure 2: errno=%d\n", errno);
          log_write(log_string);
          #endif
          free(data_fullpath);
          return -errno;
        }
      }
      free(data_fullpath);
    }
    if (dedup_migrate_file(path, file_info, in_ssd, 1)) {
      return -errno;
    }
    if ((signed int)file_info->fh >= 0) {
      close(file_info->fh);
    }
    if (!in_ssd) {
      data_fullpath = cloudfs_get_data_fullpath(path);
      unlink(data_fullpath);
      free(data_fullpath);
    }
  }
  HASH_DEL(reference_counts, reference_count);
  free(reference_count);
  return SUCCESS;
}

/*
 * Functions supported by cloudfs 
 */
static 
struct fuse_operations cloudfs_operations = {
    .init           = cloudfs_init,
    .getattr        = cloudfs_getattr,
    .getxattr       = cloudfs_getxattr,
    .setxattr       = cloudfs_setxattr,
    .mkdir          = cloudfs_mkdir,
    .opendir        = cloudfs_opendir,
    .access         = cloudfs_access,
    .utimens        = cloudfs_utimens,
    .chmod          = cloudfs_chmod,
    .rmdir          = cloudfs_rmdir,
    .readdir        = cloudfs_readdir,
    .mknod          = cloudfs_mknod,
    .open           = cloudfs_open,
    .read           = cloudfs_read,
    .write          = cloudfs_write,
    .release        = cloudfs_release,
    .unlink         = cloudfs_unlink,
    .destroy        = cloudfs_destroy
};

int cloudfs_start(struct cloudfs_state *state,
                  const char* fuse_runtime_name) {

  int argc = 0;
  char* argv[10];
  argv[argc] = (char *) malloc(128 * sizeof(char));
  strcpy(argv[argc++], fuse_runtime_name);
  argv[argc] = (char *) malloc(1024 * sizeof(char));
  strcpy(argv[argc++], state->fuse_path);
  argv[argc++] = "-s"; // set the fuse mode to single thread
  //#ifdef DEBUG
    //argv[argc++] = "-f"; // run fuse in foreground 
  //#endif

  state_  = *state;

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  
  return fuse_stat;
}
