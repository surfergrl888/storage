/* cloudfs_cache.c 
 * 
 * This file contains all the cache management functions.  The cache
 * replacement policy is LRU, and is managed in memory as a linked list, with
 * the most recently used segment at the head of the list, and the least
 * recently used segment at the tail.  So, to add a segment to the cache, we
 * just insert it in at the head of the list, and to make space for new
 * segments, we pull elements from the tail of the list.  The cache is only
 * added to on reads, since writes go to a separate file and the written
 * data is only segmented on release().  If we're accessing something already
 * in the cache, we pull it out, and reinsert it at the head to enforce the
 * ordering.
 *
 * We also keep track of the total size of the data stored in the cache, so
 * we know whether we have enough space in the cache for a new segment, and if
 * not, when to stop removing items from the cache (essentially, when we've
 * cleared up enough space).  Since the least recently used items are at the
 * tail of the list, the list is also doubly linked so it's much faster to pull
 * items from the tail.
 *
 * The cache is stored in a hidden directory in the root directory, and each
 * segment file's name is just the hash string.
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
#include "cloudfs_cache.h"
#include "cloudfs_dedup.h"
#include "cloudfs.h"

#define CACHE_DIR "/.cache"

struct cache_entry_node *cache_head = NULL;
int current_cache_size = 0;

// Each segment is stored in /.cache/[hash]
char *get_cache_fullpath(char *hash) {
  char cache_path[1+7+MD5_DIGEST_LENGTH*2+1];
  
  sprintf(cache_path, "%s/%s", CACHE_DIR, hash);
  return cloudfs_get_fullpath(cache_path);
}

// Here we essentially make sure that the cache size specified is actually big
// enough to store at least one segment, and we make sure that the cache folder
// either exists or is created sucesssfully.  If not, we disable caching since
// we need enough space, AND a folder to put everything in.
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
