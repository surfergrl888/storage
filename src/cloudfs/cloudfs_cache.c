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
#include "cloudfs_cache.h"
#include "cloudfs_dedup.h"
#include "cloudfs.h"

#define CACHE_DIR "/.cache"

struct cache_entry_node *cache_head = NULL;
int current_cache_size = 0;

char *get_cache_fullpath(char *hash) {
  char cache_path[1+7+MD5_DIGEST_LENGTH*2+1];
  
  sprintf(cache_path, "%s/%s", CACHE_DIR, hash);
  return cloudfs_get_fullpath(cache_path);
}

void init_cache() {
  struct stat temp;
  
  if (state_.cache_size < max_seg_size) {
    state_.no_cache = 1;
    return;
  }
  
  char *cache_dirpath = cloudfs_get_fullpath(CACHE_DIR);
  if (stat(cache_dirpath, &temp) && (errno == ENOENT)) {
    if (mkdir(cache_dirpath, S_IRWXU|S_IRWXG|S_IRWXO) != 0) {
      state_.no_cache = 1;
    }
  }
  free(cache_dirpath);
}

int in_cache(char *hash) {
  struct cache_entry_node *current_node = cache_head;
  
  while (current_node != NULL) {
    if (strcmp(current_node->hash, hash) == 0)
      return 1;
  }
  return 0;
}

void remove_from_cache(char *hash) {
  char *cache_file;
  
  if (cache_head == NULL)
    return;
  
  struct cache_entry_node *current_node = cache_head;
  if (strcmp(current_node->hash, hash) == 0) {
    cache_head = current_node->next;
    if (current_node->next != NULL)
      cache_head->prev = NULL;
    cache_file = get_cache_fullpath(hash);
    unlink(cache_file);
    free(cache_file);
    current_cache_size -= get_segment_size(hash);
    free(current_node);
    return;
  }
  current_node = current_node->next;
  while (current_node != NULL) {
    if (strcmp(current_node->hash, hash) == 0) {
      (current_node->prev)->next = current_node->next;
      if (current_node->next != NULL)
        (current_node->next)->prev = current_node->prev;
      cache_file = get_cache_fullpath(current_node->hash);
      unlink(cache_file);
      free(cache_file);
      current_cache_size -= get_segment_size(hash);
      free(current_node);
      return;
    }
    current_node = current_node->next;
  }
}

void add_to_cache(char *hash) {
  struct cache_entry_node *new_node = malloc(sizeof(struct cache_entry_node));
  
  new_node->prev = NULL;
  strcpy(new_node->hash, hash);
  new_node->next = cache_head;
  if (cache_head != NULL)
    cache_head->prev = new_node;
  cache_head = new_node;
  current_cache_size += get_segment_size(hash);
}

void update_in_cache(char *hash) {
  if (cache_head == NULL)
    return;
  
  struct cache_entry_node *current_node = cache_head;
  if (strcmp(current_node->hash, hash) == 0) {
    return;
  }
  current_node = current_node->next;
  while (current_node != NULL) {
    if (strcmp(current_node->hash, hash) == 0) {
      (current_node->prev)->next = current_node->next;
      if (current_node->next != NULL)
        (current_node->next)->prev = current_node->prev;
      current_node->prev = NULL;
      current_node->next = cache_head;
      cache_head->prev = current_node;
      cache_head = current_node;
      return;
    }
    current_node = current_node->next;
  }
}

void make_space_in_cache(int size) {
  if (state_.cache_size - current_cache_size >= size)
    return;
  
  struct cache_entry_node *current_node = cache_head;
  while (current_node->next != NULL)
    current_node = current_node->next;
  
  while (state_.cache_size - current_cache_size < size) {
    char *cache_file = get_cache_fullpath(current_node->hash);
    unlink(cache_file);
    free(cache_file);
    current_cache_size -= get_segment_size(current_node->hash);
    if (current_node->prev == NULL) {
      cache_head = NULL;
      free(current_node);
      return;
    }
    else {
      (current_node->prev)->next = NULL;
      struct cache_entry_node *temp = current_node;
      current_node = current_node->prev;
      free(temp);
    }
  }
}
