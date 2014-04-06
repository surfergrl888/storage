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
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <utime.h>
#include <unistd.h>
#include "cloudapi.h"
#include "cloudfs.h"
#include "dedup.h"

#define UNUSED __attribute__((unused))
#define SUCCESS 0
#define META_TIMESTAMPS 1+sizeof(int)+sizeof(off_t)
#define UTIME_NOW	((1l << 30) - 1l)
#define UTIME_OMIT	((1l << 30) - 2l)
#define META_ATIME_OFFSET META_TIMESTAMPS
#define META_MTIME_OFFSET META_TIMESTAMPS+sizeof(time_t)+sizeof(unsigned long)
#define META_ATTRTIME_OFFSET META_MTIME_OFFSET+sizeof(time_t)+\
                             sizeof(unsigned long)
#define META_REF_COUNT_OFFSET META_ATTRTIME_OFFSET+sizeof(time_t)+\
                              sizeof(unsigned long)

static struct cloudfs_state state_;
static int infile, outfile;

int get_buffer(const char *buffer, int bufferLength) {
  return write(outfile, buffer, bufferLength);  
}

int put_buffer(char *buffer, int bufferLength) {
  //fprintf(stdout, "put_buffer %d \n", bufferLength);
  return read(infile, buffer, bufferLength);
}

int get_weak_hash(const char *path)
{
  int i;
  int sum = 0;;
  
  for (i=0; i<strlen(path); i++) {
    if (path[i] == '+') {
      sum += i;
    }
  }
  return sum;
}

char *get_s3_key(const char *path) {
  int i;
  
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
  strcat(fullpath, path);
  
  return fullpath;
}

char *cloudfs_get_metadata_fullpath(const char *path)
{
  char *fullpath = malloc(strlen(state_.ssd_path)+strlen(path)+2);
  char *cur_pos, *end_pos, *fullpath_pos;
  int next_jump = 0;
  
  strcpy(fullpath, state_.ssd_path);
  cur_pos = (char *)path;
  fullpath_pos = fullpath+strlen(fullpath);
  end_pos = (char *)(path + strlen(path));
  next_jump = strcspn(cur_pos, "/");
  while (cur_pos + next_jump != end_pos) {
    strncpy(fullpath_pos, cur_pos, next_jump+1);
    cur_pos += next_jump+1;
    fullpath += next_jump+1;
    next_jump = strcspn(cur_pos, "/");
  }
  strcat(fullpath_pos, ".");
  strcat(fullpath_pos, cur_pos);
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
  return NULL;
}

void cloudfs_destroy(void *data UNUSED) {
  cloud_destroy();
}

/* Directory operations */

int cloudfs_mkdir(const char *path, mode_t mode)
{
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err =  mkdir(fullpath, mode);
  free(fullpath);
  
  if (err)
    return -errno;
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
  char data_location;
  struct stat info;
  
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
  meta_file = open(fullpath, O_RDWR);
  free(fullpath);
  if (read(meta_file, &data_location, 1) != 1) {
    printf("Error with metadata!\n");
    close(meta_file);
    return -1;
  }
  if (data_location == 'S') {
    close(meta_file);
    return SUCCESS;
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
  if (write(meta_file, &(cur_time.tv_nsec), sizeof(unsigned long)) !=
      sizeof(unsigned long)) {
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
  char data_location;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = stat(fullpath, statbuf);
  free(fullpath);
  
  if (err)
    return -errno;
  if (!S_ISDIR(statbuf->st_mode)) {
    fullpath = cloudfs_get_metadata_fullpath(path);
    metadata_file = open(fullpath, O_RDONLY);
    free(fullpath);
    if (read(metadata_file, &data_location, 1) != 1) {
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (data_location == 'S') {
      close(metadata_file);
      return SUCCESS;
    }
    if (data_location != 'C') {
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    lseek(metadata_file, sizeof(int), SEEK_CUR);
    if (read(metadata_file, &(statbuf->st_size), sizeof(off_t)) != 
        sizeof(off_t)) {
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_atime), sizeof(time_t)) !=
        sizeof(time_t)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_atimensec), sizeof(unsigned long))
        != sizeof(unsigned long)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_mtime), sizeof(time_t)) !=
        sizeof(time_t)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_mtimensec), sizeof(unsigned long))
        != sizeof(unsigned long)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_ctime), sizeof(time_t)) !=
        sizeof(time_t)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
    }
    if (read(metadata_file, &(statbuf->st_ctimensec), sizeof(unsigned long))
        != sizeof(unsigned long)){
      printf("Error with metadata!\n");
      close(metadata_file);
      return -1;
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
  char data_location;
  struct stat info;
  
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
  meta_file = open(fullpath, O_RDWR);
  free(fullpath);
  if (read(meta_file, &data_location, 1) != 1) {
    printf("Error with metadata!\n");
    close(meta_file);
    return -1;
  }
  if (data_location == 'S') {
    close(meta_file);
    return SUCCESS;
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
  if (write(meta_file, &(cur_time.tv_nsec), sizeof(unsigned long)) !=
      sizeof(unsigned long)) {
    close(meta_file);
    return -errno;
  }
  close(meta_file);
  return SUCCESS;
}

int cloudfs_utimens(const char *path, const struct timespec tv[2]) {
  int err;
  char data_location;
  struct timespec cur_time;
  char *meta_fullpath;
  struct stat statbuf;
  int metadata_file;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = stat(fullpath, &statbuf);
  
  if (err)
    return -errno;
  if (S_ISDIR(statbuf.st_mode)) {
    err = utimensat(-1, fullpath, tv, 0);
    free(fullpath);
    if (err)
      return -errno;
    return SUCCESS;
  }
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  metadata_file = open(meta_fullpath, O_RDWR);
  free(meta_fullpath);
  if (read(metadata_file, &data_location, 1) != 1) {
    printf("Error with metadata!\n");
    free(fullpath);
    close(metadata_file);
    return -1;
  }
  if (data_location == 'S') {
    err = utimensat(-1, fullpath, tv, 0);
    free(fullpath);
    close(metadata_file);
    if (err)
      return -errno;
    return SUCCESS;
  }
  free(fullpath);
  if (data_location != 'C') {
    printf("Error with metadata!\n");
    close(metadata_file);
    return -1;
  }
  err = lseek(metadata_file, META_TIMESTAMPS, SEEK_SET);
  if (err < 0) {
    close(metadata_file);
    return -errno;
  }
  if (tv[0].tv_nsec == UTIME_OMIT) {
    err = lseek(metadata_file, sizeof(time_t)+sizeof(unsigned long), SEEK_CUR);
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
      if (write(metadata_file, &(cur_time.tv_nsec), sizeof(unsigned long)) ==
          -1) {
        close(metadata_file);
        return -errno;
      }
    }
    else {
      if (write(metadata_file, &(tv[0].tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
      if (write(metadata_file, &(tv[0].tv_nsec), sizeof(unsigned long)) ==
          -1) {
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
      if (write(metadata_file, &(cur_time.tv_nsec), sizeof(unsigned long)) ==
          -1) {
        close(metadata_file);
        return -errno;
      }
    }
    else {
      if (write(metadata_file, &(tv[1].tv_sec), sizeof(time_t)) == -1) {
        close(metadata_file);
        return -errno;
      }
      if (write(metadata_file, &(tv[1].tv_nsec), sizeof(unsigned long)) ==
          -1) {
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
  int err, meta_file, null = 0;
  
  char *fullpath = cloudfs_get_fullpath(path);
  char *meta_fullpath = cloudfs_get_metadata_fullpath(path);
  
  err = mknod(fullpath, mode, dev);
  if (err) {
    free(fullpath);
    free(meta_fullpath);
    return -errno;
  }
  
  // We need unmitigated r/w access to the metadata file.
  err = mknod(meta_fullpath, S_IRUSR|S_IWUSR, dev);
  if (err) {
    unlink(fullpath);
    free(fullpath);
    free(meta_fullpath);
    return -errno;
  }
  meta_file = open(meta_fullpath, O_WRONLY);
  if (meta_file == NULL) {
    unlink(meta_fullpath);
    unlink(fullpath);
    free(fullpath);
    free(meta_fullpath);
    return -errno;
  }
  if (write(meta_file, "S", 1) != 1) {
    close(meta_file);
    unlink(meta_fullpath);
    unlink(fullpath);
    free(fullpath);
    free(meta_fullpath);
    return -errno;
  }
  if (write(meta_file, &(null), sizeof(int)) != 1) {
    close(meta_file);
    unlink(meta_fullpath);
    unlink(fullpath);
    free(fullpath);
    free(meta_fullpath);
    return -errno;
  }
  close(meta_file);
  free(fullpath);
  free(meta_fullpath);
  return SUCCESS;
}

int cloudfs_unlink(const char *path) {
  char *fullpath, *meta_fullpath, *data_fullpath;
  char *s3_key;
  int meta_file, err;
  char s3_bucket[11];
  char data_location;
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_WRONLY);
  if (meta_file == NULL) {
    free(meta_fullpath);
    return -errno;
  }
  err = read(meta_file, &data_location, 0);
  if (err != 1) {
    close(meta_file);
    free(meta_fullpath);
    return -errno;
  }
  if (data_location == 'C') {
    close(meta_file);
    s3_key = get_s3_key(path);
    sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path));
    cloud_delete_object(s3_bucket, s3_key);
    data_fullpath = cloudfs_get_data_fullpath(path);
    unlink(data_fullpath);
    free(s3_key);
    free(data_fullpath);
  }
  
  
  fullpath = cloudfs_get_fullpath(path);
  unlink(fullpath);
  free(fullpath);
  
  unlink(meta_fullpath);
  free(meta_fullpath);
  
  return SUCCESS;
}

/* File I/O */

int cloudfs_read(const char *path, char *buffer, size_t size,
                 off_t offset, struct fuse_file_info *file_info)
{
  int err, meta_file;
  char *meta_fullpath;
  size_t retval;
  struct timespec cur_time;
  char data_location;
  
  err = lseek(file_info->fh, offset, SEEK_SET);
  if (err < 0) {
    return -errno;
  }
  
  retval = read(file_info->fh, buffer, size);
  if (retval == -1) {
    return -errno;
  }
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_WRONLY);
  free(meta_fullpath);
  if (meta_file == NULL)
    return -errno;
  err = read(meta_file, &data_location, 0);
  if (err != 1) {
    close(meta_file);
    return -errno;
  }
  if (data_location == 'S') {
    close(meta_file);
    return retval;
  }
  err = lseek(meta_file, META_ATIME_OFFSET, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    return -errno;
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) != sizeof(time_t)){
    close(meta_file);
    return -errno;
  }
  if (write(meta_file, &(cur_time.tv_nsec), sizeof(unsigned long)) !=
      sizeof(unsigned long)) {
    close(meta_file);
    return -errno;
  }
  close(meta_file);
  return retval;
}

int cloudfs_write(const char *path, const char *buffer, size_t size,
                  off_t offset, struct fuse_file_info *file_info)
{
  int err, meta_file, i;
  char *meta_fullpath;
  char data_location;
  struct stat info;
  size_t retval;
  struct timespec cur_time;
  
  err = lseek(file_info->fh, offset, SEEK_SET);
  if (err < 0) {
    return -errno;
  }
  
  retval = write(file_info->fh, buffer, size);
  if (retval == -1) {
    return -errno;
  }
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_RDWR);
  free(meta_fullpath);
  if (meta_file == NULL)
    return -errno;
  err = read(meta_file, &data_location, 0);
  if (err != 1) {
    close(meta_file);
    return -errno;
  }
  if (data_location == 'S') {
    close(meta_file);
    return retval;
  }
  err = fstat(file_info->fh, &info);
  if (err) {
    close(meta_file);
    return -errno;
  }
  lseek(meta_file, sizeof(int), SEEK_CUR);
  if (write(meta_file, &(info.st_size), sizeof(off_t)) != sizeof(off_t)) {
    close(meta_file);
    return -errno;
  }
  err = clock_gettime(CLOCK_REALTIME, &cur_time);
  for (i=0; i<3; i++) {
    if (write(meta_file, &(cur_time.tv_sec), sizeof(time_t)) !=
        sizeof(time_t)){
      close(meta_file);
      return -errno;
    }
    if (write(meta_file, &(cur_time.tv_nsec), sizeof(unsigned long)) !=
        sizeof(unsigned long)) {
      close(meta_file);
      return -errno;
    }
  }
  close(meta_file);
  return retval;
}

int cloudfs_open(const char *path, struct fuse_file_info *file_info)
{
  char *meta_fullpath, *data_fullpath, *s3_key = NULL;
  struct stat info;
  S3Status status;
  char data_location;
  char s3_bucket[11];
  int meta_file;
  int err, ref_count;
  
  // The first thing we do is check the permissions, which are stored with the
  // proxy file
  char *fullpath = cloudfs_get_fullpath(path);
  if (file_info->flags & O_RDONLY) {
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
    if (access(fullpath, R_OK & W_OK)) {
      free(fullpath);
      return -errno;
    }
  }
  stat(fullpath, &info);
  free(fullpath);
  
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_RDONLY);
  free(meta_fullpath);
  if (meta_file == NULL)
    return -errno;
  err = read(meta_file, &data_location, 1);
  if (err != 1) {
    close(meta_file);
    return -errno;
  }
  err = read(meta_file, &ref_count, sizeof(int));
  if (err != sizeof(int)) {
    close(meta_file);
    return -errno;
  }
  lseek(meta_file, 1, SEEK_SET);
  if (data_location == 'S') {
    fullpath = cloudfs_get_fullpath(path);
    file_info->fh = open(fullpath, file_info->flags);
    free(fullpath);
    
    if (file_info->fh == NULL)
       return -errno;
  }
  else {
    data_fullpath = cloudfs_get_data_fullpath(path);
    file_info->fh = open(data_fullpath, O_RDWR&O_CREAT, info.st_mode);
    free(data_fullpath);
    if (file_info->fh == NULL)
      return -errno;
    
    if (ref_count == 0) {
      infile = file_info->fh;
      sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path));
      s3_key = get_s3_key(path);
      status = cloud_get_object(s3_bucket, s3_key, get_buffer);
      if (status != S3StatusOK) {
        close(file_info->fh);
        free(s3_key);
        cloudfs_error("error with the cloud\n");
        return -1;
      }
    }
  }
  ref_count ++;
  err = write(meta_file, &ref_count, sizeof(int));
  if (err != sizeof(int)) {
    if (data_location == 'C')
      cloud_delete_object(s3_bucket, s3_key);
    free(s3_key);
    close(file_info->fh);
    close(meta_file);
    return -errno;
  }
  close(meta_file);
  free(s3_key);
  return SUCCESS;
}

int cloudfs_release(const char *path, struct fuse_file_info *file_info)
{
  char *meta_fullpath, *data_fullpath, *s3_key;
  struct stat info;
  S3Status status;
  char data_location;
  char s3_bucket[11];
  int meta_file;
  int err, ref_count;
  
  fstat(file_info->fh, &info);
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_RDONLY);
  free(meta_fullpath);
  if (meta_file == NULL)
    return -errno;
  err = read(meta_file, &data_location, 1);
  if (err != 1) {
    close(meta_file);
    return -errno;
  }
  err = read(meta_file, &ref_count, sizeof(int));
  if (err != sizeof(int)) {
    close(meta_file);
    return -errno;
  }
  ref_count--;
  if ((ref_count > 0) || ((data_location == 'S') &&
                          (info.st_size <= state_.threshold))) {
    lseek(meta_file, 1, SEEK_SET);
    if (write(meta_file, &ref_count, sizeof(int)) != sizeof(int)) {
      close(meta_file);
      return -errno;
    }
    close(file_info->fh);
    return SUCCESS;
  }
  lseek(file_info->fh, 0, SEEK_SET);
  sprintf(s3_bucket,"%d",strlen(path)+get_weak_hash(path));
  s3_key = get_s3_key(path);
  outfile = file_info->fh;
  status = cloud_put_object(s3_bucket, s3_key, info.st_size, put_buffer);
  if (status != S3StatusOK) {
    close(meta_file);
    free(s3_key);
    cloudfs_error("error with the cloud\n");
    return -1;
  }
  if (data_location == 'S') {
    lseek(meta_file, 0, SEEK_SET);
    write(meta_file, "C", 1);
    write(meta_file, &ref_count, sizeof(int));
    write(meta_file, &(info.st_size), sizeof(off_t));
    write(meta_file, &(info.st_atime), sizeof(time_t));
    write(meta_file, &(info.st_atimensec), sizeof(unsigned long));
    write(meta_file, &(info.st_mtime), sizeof(time_t));
    write(meta_file, &(info.st_mtimensec), sizeof(unsigned long));
    write(meta_file, &(info.st_ctime), sizeof(time_t));
    write(meta_file, &(info.st_ctimensec), sizeof(unsigned long));
    ftruncate(file_info->fh, 0);
    close(file_info->fh);
  }
  if (data_location == 'C') {
    lseek(meta_file, 1, SEEK_SET);
    write(meta_file, &ref_count, sizeof(int));
    close(file_info->fh);
    data_fullpath = cloudfs_get_data_fullpath(path);
    unlink(data_fullpath);
    free(data_fullpath);
  }
  close(meta_file);
  free(s3_key);
  return SUCCESS;
}

/*
 * Functions supported by cloudfs 
 */
static 
struct fuse_operations cloudfs_operations = {
    .init           = cloudfs_init,
    //
    // TODO
    //
    // This is where you add the VFS functions that your implementation of
    // MelangsFS will support, i.e. replace 'NULL' with 'melange_operation'
    // --- melange_getattr() and melange_init() show you what to do ...
    //
    // Different operations take different types of parameters. This list can
    // be found at the following URL:
    // --- http://fuse.sourceforge.net/doxygen/structfuse__operations.html
    //
    
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
    .destroy        = cloudfs_destroy,
    .flag_nopath    = 0
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
  //argv[argc++] = "-f"; // run fuse in foreground 

  state_  = *state;

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  
  return fuse_stat;
}
