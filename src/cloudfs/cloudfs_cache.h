#ifndef __CLOUDFS_CACHE_H_
#define __CLOUDFS_CACHE_H_

#include <openssl/md5.h>

struct cache_entry_node {
  char hash[MD5_DIGEST_LENGTH*2+1];
  struct cache_entry_node *prev;
  struct cache_entry_node *next;
};

void init_cache();
int in_cache(char *hash);
void remove_from_cache(char *hash);
char *get_cache_fullpath(char *hash);
void add_to_cache(char *hash);
void update_in_cache(char *hash);
void make_space_in_cache(int size);

#endif
