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
#include <unistd.h>
#include "cloudapi.h"
#include "cloudfs.h"
#include "dedup.h"

#define UNUSED __attribute__((unused))
#define SUCCESS 0
#define META_TIMESTAMPS 1+sizeof(off_t)
# define UTIME_NOW	((1l << 30) - 1l)
# define UTIME_OMIT	((1l << 30) - 2l)

static struct cloudfs_state state_;

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
  int err;
  
  char *fullpath = cloudfs_get_fullpath(path);
  err = chmod(fullpath, mode);
  free(fullpath);
  
  if (err)
    return -errno;
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

int cloudfs_setxattr(const char *path, const char *name, char *value,
                      size_t size, int flags)
{
  int err;
  char *fullpath = cloudfs_get_fullpath(path);
  err = lsetxattr(fullpath, name, value, size, flags);
  free(fullpath);
  
  if (err)
    return -errno;
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
    
    .getattr        = NULL,
    .mkdir          = NULL,
    .readdir        = NULL,
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
  //argv[argc++] = "-f"; // run fuse in foreground 

  state_  = *state;

  int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);
  
  return fuse_stat;
}
