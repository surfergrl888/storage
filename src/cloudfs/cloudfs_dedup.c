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
#include <openssl/md5.h>
#include <sys/sendfile.h>
#include <time.h>
#include <utime.h>
#include <unistd.h>
#include "cloudapi.h"
#include "zlib.h"
#include "uthash.h"
#include "cloudfs.h"
#include "compressapi.h"
#include "cloudfs_cache.h"
#include "cloudfs_dedup.h"
#include "dedup.h"

#define DEDUP_VARIATION(x) (x/16)
#define HASH_TABLE_FILE "/.hash_table"
#define META_SEGMENT_LIST sizeof(off_t)+3*sizeof(time_t)
#define COMPRESS_TEMP_FILE "./temp_compress"
#define SEGMENT_TEMP_FILE "./segment_temp"
#define SEGMENT_FILE_PREFIX "./segment_"

rabinpoly_t *rabin;
int max_seg_size;
char log_string[100];

struct segment_hash_struct *segment_hash_table = NULL;

int get_segment_size(char *hash) {
  struct segment_hash_struct *segment;
  
  HASH_FIND_STR(segment_hash_table, hash, segment);
  if (segment != NULL) {
    return segment->length;
  }
  else {
    return 0;
  }
}

void rebuild_hash_table() {
  int hash_table_file, err;
  char *hash_table_file_path, *cache_path;
  struct stat temp;
  struct segment_hash_struct *current_segment;
  
  log_write("restoring hash table\n");
  hash_table_file_path = cloudfs_get_fullpath(HASH_TABLE_FILE);
  err = stat(hash_table_file_path, &temp);
  if (err && (errno == ENOENT)) {
    free(hash_table_file_path);
    return;
  }
  hash_table_file = open(hash_table_file_path, O_RDONLY);
  while (1) {
    current_segment = malloc(sizeof(struct segment_hash_struct));
    if (read(hash_table_file, current_segment, sizeof(struct segment_hash_struct))
        != sizeof(struct segment_hash_struct)) {
      free(current_segment);
      break;
    }
    sprintf(log_string, "%s %d\n", current_segment->hash,
            current_segment->ref_count);
    log_write(log_string);
    HASH_ADD_STR(segment_hash_table, hash, current_segment);
    if (!state_.no_cache) {
      cache_path = get_cache_fullpath(current_segment->hash);
      err = stat(cache_path, &temp);
      if (!(err && (errno == ENOENT))) {
        add_to_cache(current_segment->hash);
      }
      free(cache_path);
    }
  }
  free(hash_table_file_path);
  close(hash_table_file);
}

int update_hash_table_file() {
  char *hash_table_file_path;
  int hash_table_file;
  struct segment_hash_struct *current_segment;
  
  hash_table_file_path = cloudfs_get_fullpath(HASH_TABLE_FILE);
  hash_table_file = open(hash_table_file_path, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
  
  log_write("updating hash table\n");
  for(current_segment=segment_hash_table; current_segment != NULL;
      current_segment=current_segment->hh.next) {
    sprintf(log_string, "%s %d\n", current_segment->hash,
            current_segment->ref_count);
    log_write(log_string);
    if (write(hash_table_file, current_segment,
              sizeof(struct segment_hash_struct)) !=
              sizeof(struct segment_hash_struct)) {
      #ifdef DEBUG
        printf("Error updating hash table on disk!\n");
      #endif
      free(hash_table_file_path);
      close(hash_table_file);
      return -1;
    }
  }
  free(hash_table_file_path);
  close(hash_table_file);
  return 0;
}

void dedup_init() {
  int min_seg_size;
  
  log_write("in dedup_init\n");
  max_seg_size = state_.avg_seg_size+DEDUP_VARIATION(state_.avg_seg_size);
  min_seg_size = state_.avg_seg_size-DEDUP_VARIATION(state_.avg_seg_size);
  rabin = rabin_init(state_.rabin_window_size, state_.avg_seg_size,
                     min_seg_size, max_seg_size);
  if (!state_.no_cache) {
    init_cache();
  }
  rebuild_hash_table();
}

void dedup_destroy() {
  rabin_free(&rabin);
  update_hash_table_file();
}

int dedup_migrate_file(const char *path, struct fuse_file_info *file_info, int in_ssd, char move_entire_file) {
  struct segment_hash_struct *current_segment;
  char s3_bucket[4];
	unsigned char current_hash[MD5_DIGEST_LENGTH];
	char current_hash_string[MD5_DIGEST_LENGTH*2+1];
	char *data_fullpath, *meta_fullpath, *compress_temp_path;
  int new_segment = 0;
  struct stat info;
  FILE *temp_file = NULL;
	int len, segment_len = 0, compressed_len = 0, temp_fd;
	// int b;
	off_t segment_start = 0;
	S3Status status;
	FILE *segmenting_file = NULL;
	int segmenting_fd, meta_file;
	int cur_offset = 0;
	char buf[1024];
	MD5_CTX ctx;
	int bytes, err, i;
	int total_segment_len = 0;
  
  #ifdef DEBUG
    printf("calling dedup_migrate_file\n");
  #endif
  err = lseek(file_info->fh, 0, SEEK_SET);
  if (err < 0)
    return -1;
	meta_fullpath = cloudfs_get_metadata_fullpath(path);
	meta_file = open(meta_fullpath, O_WRONLY|O_CREAT,
	                 S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
	if (meta_file == NULL) {
	  free(meta_fullpath);
	  return -1;
	}
	if (in_ssd) {
	  fstat(file_info->fh, &info);
	  #ifdef DEBUG
      printf("initializing metadata for a file that is currently on the ssd\n");
    #endif
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
    data_fullpath = cloudfs_get_fullpath(path);
  }
  else {
    data_fullpath = cloudfs_get_data_fullpath(path);
  }
  #ifdef DEBUG
    printf("seeking to the end of the metadata file\n");
  #endif
  err = lseek(meta_file, 0, SEEK_END);
  if (err < 0) {
    close(meta_file);
    if (in_ssd)
      unlink(meta_fullpath);
    free(meta_fullpath);
    return -1;
  }
  #ifdef DEBUG
    printf("opening a file descriptor for moving segments\n");
  #endif
  segmenting_fd = open(data_fullpath, O_RDWR);
  free(data_fullpath);
  if (segmenting_fd == NULL) {
    close(meta_file);
    if (in_ssd)
      unlink(meta_fullpath);
    free(meta_fullpath);
    return -1;
  }
  if (!state_.no_compress) {
    segmenting_file = fdopen(segmenting_fd, "rb");
    if (segmenting_file == NULL) {
      close(segmenting_fd);
      close(meta_file);
      if (in_ssd)
        unlink(meta_fullpath);
      free(meta_fullpath);
      return -1;
    }
  }
  #ifdef DEBUG
    printf("breaking the file into segments...\n");
  #endif
  MD5_Init(&ctx);
	while( (bytes = read(file_info->fh, buf, sizeof buf)) > 0 ) {
		char *buftoread = (char *)&buf[0];
		while ((len = rabin_segment_next(rabin, buftoread, bytes, 
											&new_segment)) > 0) {
			MD5_Update(&ctx, buftoread, len);
			segment_len += len;
			if (new_segment) {
				MD5_Final(current_hash, &ctx);
				
				for(i = 0; i < MD5_DIGEST_LENGTH; i++)
          sprintf(&current_hash_string[i*2], "%02x", current_hash[i]);
        #ifdef DEBUG
          printf("got a new segment: size=%u, hash=%s\n", segment_len, current_hash_string);
        #endif
        current_segment = NULL;
        HASH_FIND_STR(segment_hash_table, current_hash_string, current_segment);
        total_segment_len += segment_len;
        
        if( total_segment_len >= 0x4200) {
          int mk=0;
          mk=lseek(segmenting_fd, 0, SEEK_CUR);
          mk = mk;
          mk++;
        }
        if (current_segment != NULL) {
          current_segment->ref_count ++;
          if (!state_.no_compress) {
            err = fseek(segmenting_file, segment_len, SEEK_CUR);
          }
          else {
            err = lseek(segmenting_fd, segment_len, SEEK_CUR);
          }
          if (err < 0) {
            close(meta_file);
            if (state_.no_compress)
		          close(segmenting_fd);
		        else
		          fclose(segmenting_file);
            if (in_ssd)
              unlink(meta_fullpath);
            free(meta_fullpath);
            #ifdef DEBUG
              printf("resetting rabin\n");
            #endif
            rabin_reset(rabin);
            return -1;
          }
        }
        else {
          s3_bucket[0] = current_hash_string[0];
          s3_bucket[1] = current_hash_string[1];
          s3_bucket[2] = current_hash_string[2];
          s3_bucket[3] = 0;
          if (!bucket_exists(s3_bucket)) {
            cloud_create_bucket(s3_bucket);
          }
          if (!state_.no_compress) {
            #ifdef DEBUG
              printf("compressing the segment...\n");
            #endif
            compress_temp_path = cloudfs_get_fullpath(COMPRESS_TEMP_FILE);
            temp_fd = open(compress_temp_path, O_RDWR|O_CREAT,
                             S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
            if (temp_fd == NULL) {
              free(compress_temp_path);
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              free(meta_fullpath);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
            temp_file = fopen(compress_temp_path, "rb+");
            if (temp_file == NULL) {
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              free(meta_fullpath);
              close(temp_fd);
              unlink(compress_temp_path);
              free(compress_temp_path);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
            err = def(segmenting_file, temp_file, segment_len,
                      Z_DEFAULT_COMPRESSION);
            if (err != Z_OK) {
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              close(temp_fd);
              free(meta_fullpath);
              fclose(temp_file);
              unlink(compress_temp_path);
              free(compress_temp_path);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
            fclose(temp_file);
            stat(compress_temp_path, &info);
            err = lseek(temp_fd, 0, SEEK_SET);
            if (err < 0) {
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              free(meta_fullpath);
              close(temp_fd);
              unlink(compress_temp_path);
              free(compress_temp_path);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
            #ifdef DEBUG
              printf("moving the segment... bucket=%s, key=%s, len=%d\n", s3_bucket, current_hash_string+3, info.st_size);
            #endif
            infile = temp_fd;
            status = cloud_put_object(s3_bucket, current_hash_string+3, info.st_size, put_buffer);
            if (status != S3StatusOK) {
              #ifdef DEBUG
                cloud_print_error();
              #endif
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              free(meta_fullpath);
              close(temp_fd);
              unlink(compress_temp_path);
              free(compress_temp_path);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
            close(temp_fd);
            unlink(compress_temp_path);
            free(compress_temp_path);
          }
          else {
            #ifdef DEBUG
              printf("moving the segment...\n");
            #endif
            infile = segmenting_fd;
            status = cloud_put_object(s3_bucket, current_hash_string+3, segment_len, put_buffer);
            if (status != S3StatusOK) {
              #ifdef DEBUG
                cloud_print_error();
              #endif
              close(meta_file);
              if (state_.no_compress)
		            close(segmenting_fd);
		          else
		            fclose(segmenting_file);
              if (in_ssd)
                unlink(meta_fullpath);
              free(meta_fullpath);
              #ifdef DEBUG
                printf("resetting rabin\n");
              #endif
              rabin_reset(rabin);
              return -1;
            }
          }
          current_segment = malloc(sizeof(struct segment_hash_struct));
          current_segment->length = segment_len;
          memcpy(current_segment->hash, current_hash_string, MD5_DIGEST_LENGTH*2+1);
          current_segment->ref_count = 1;
          sprintf(log_string, "adding %s to the hash table\n", current_segment->hash);
          log_write(log_string);
          printf("segment: %s %d\n", current_hash_string, segment_len);  
          HASH_ADD_STR(segment_hash_table, hash, current_segment);
        }
        #ifdef DEBUG
          printf("updating hash table...\n");
        #endif
        update_hash_table_file();
        #ifdef DEBUG
          printf("updating metadata...\n");
        #endif
        if (write(meta_file, current_hash_string, MD5_DIGEST_LENGTH*2+1) != MD5_DIGEST_LENGTH*2+1) {
          if (current_segment->ref_count == 1) {
            sprintf(log_string, "removing %s from the hash table\n", current_segment->hash);
            log_write(log_string);
            HASH_DEL(segment_hash_table, current_segment);
            free(current_segment);
          }
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
          else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
				/*printf("%u ", segment_len);
				for(b = 0; b < MD5_DIGEST_LENGTH; b++)
					printf("%02x", hash[b]);
				printf("\n");*/
				
				MD5_Init(&ctx);
				segment_len = 0;
			}

			buftoread += len;
			bytes -= len;

			if (!bytes) {
				break;
			}
		}
		if (len == -1) {
		  if (state_.no_compress)
		    close(segmenting_fd);
		  else
		    fclose(segmenting_file);
		  if (in_ssd)
        unlink(meta_fullpath);
      free(meta_fullpath);
			fprintf(stderr, "Failed to process the segment\n");
			#ifdef DEBUG
        printf("resetting rabin\n");
      #endif
      rabin_reset(rabin);
			return -1;
		}
	}
	MD5_Final(current_hash, &ctx);
  #ifdef DEBUG
    printf("done segmenting, moving on to final segment...\n");
  #endif
	if (move_entire_file) {
	  for(i = 0; i < MD5_DIGEST_LENGTH; ++i)
      sprintf(&current_hash_string[i*2], "%02x", (unsigned int)current_hash[i]);
    current_segment = NULL;
	  HASH_FIND_STR(segment_hash_table, current_hash_string, current_segment);
    if (current_segment != NULL)
      current_segment->ref_count ++;
    else {
      s3_bucket[0] = current_hash_string[0];
      s3_bucket[1] = current_hash_string[1];
      s3_bucket[2] = current_hash_string[2];
      s3_bucket[3] = 0;
      if (!bucket_exists(s3_bucket)) {
        cloud_create_bucket(s3_bucket);
      }
      if (!state_.no_compress) {
        #ifdef DEBUG
          printf("compressing file...\n");
        #endif
        compress_temp_path = cloudfs_get_fullpath(COMPRESS_TEMP_FILE);
        temp_fd = open(compress_temp_path, O_RDWR|O_CREAT,
                         S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
        if (temp_fd == NULL) {
          free(compress_temp_path);
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
          else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
        temp_file = fopen(compress_temp_path, "rb+");
        if (temp_file == NULL) {
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
          else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          close(temp_fd);
          unlink(compress_temp_path);
          free(compress_temp_path);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
        err = def(segmenting_file, temp_file, segment_len,
                  Z_DEFAULT_COMPRESSION);
        if (err != Z_OK) {
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
	        else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          fclose(temp_file);
          close(temp_fd);
          unlink(compress_temp_path);
          free(compress_temp_path);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
        fclose(temp_file);
        stat(compress_temp_path, &info);
        err = lseek(temp_fd, 0, SEEK_SET);
        if (err < 0) {
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
	        else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          close(temp_fd);
          unlink(compress_temp_path);
          free(compress_temp_path);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
        infile = temp_fd;
        #ifdef DEBUG
          printf("moving segment...\n");
        #endif
        status = cloud_put_object(s3_bucket, current_hash_string+3, info.st_size, put_buffer);
        if (status != S3StatusOK) {
          #ifdef DEBUG
            cloud_print_error();
          #endif
          close(meta_file);
          if (state_.no_compress)
		        close(segmenting_fd);
		      else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          close(temp_fd);
          unlink(compress_temp_path);
          free(compress_temp_path);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
        close(temp_fd);
        unlink(compress_temp_path);
        free(compress_temp_path);
      }
      else {
        #ifdef DEBUG
          printf("moving segment...\n");
        #endif
        infile = segmenting_fd;
        status = cloud_put_object(s3_bucket, current_hash_string+3, segment_len, put_buffer);
        if (status != S3StatusOK) {
          #ifdef DEBUG
            cloud_print_error();
          #endif
          close(meta_file);
          if (state_.no_compress)
          close(segmenting_fd);
		      else
		        fclose(segmenting_file);
          if (in_ssd)
            unlink(meta_fullpath);
          free(meta_fullpath);
          #ifdef DEBUG
            printf("resetting rabin\n");
          #endif
          rabin_reset(rabin);
          return -1;
        }
      }
      current_segment = malloc(sizeof(struct segment_hash_struct));
      current_segment->length = segment_len;
      memcpy(current_segment->hash, current_hash_string, MD5_DIGEST_LENGTH*2+1);
      current_segment->ref_count = 1;
      sprintf(log_string, "adding %s to the hash table\n", current_segment->hash);
      log_write(log_string);
      HASH_ADD_STR(segment_hash_table, hash, current_segment);
    }
    #ifdef DEBUG
      printf("updating hash table...\n");
    #endif
    update_hash_table_file();
    #ifdef DEBUG
      printf("updating metadata...\n");
    #endif
    if (write(meta_file, current_hash_string, MD5_DIGEST_LENGTH*2+1) != MD5_DIGEST_LENGTH*2+1) {
      if (current_segment->ref_count == 1) {
        sprintf(log_string, "removing %s from the hash table\n", current_segment->hash);
        log_write(log_string);
        HASH_DEL(segment_hash_table, current_segment);
        free(current_segment);
      }
      close(meta_file);
      if (state_.no_compress)
		    close(segmenting_fd);
      else
		    fclose(segmenting_file);
      if (in_ssd)
        unlink(meta_fullpath);
      free(meta_fullpath);
      #ifdef DEBUG
        printf("resetting rabin\n");
      #endif
      rabin_reset(rabin);
      return -1;
    }
	}
	else {
	  #ifdef DEBUG
      printf("moving the rest of the data...\n");
    #endif
	  if (in_ssd) {
	    data_fullpath = cloudfs_get_data_fullpath(path);
	    close(file_info->fh);
	    file_info->fh = open(data_fullpath, O_RDWR|O_CREAT,
                           S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (file_info->fh == NULL) {
        free(data_fullpath);
        if (state_.no_compress)
		    close(segmenting_fd);
        else
		      fclose(segmenting_file);
        free(meta_fullpath);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      if (sendfile(file_info->fh, segmenting_fd, NULL, segment_len) < 0) {
        if (state_.no_compress)
		      close(segmenting_fd);
        else
		      fclose(segmenting_file);
        free(meta_fullpath);
        close(file_info->fh);
        unlink(data_fullpath);
        free(data_fullpath);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      free(data_fullpath);
		  free(meta_fullpath);
	  }
	  else {
	    data_fullpath = cloudfs_get_data_fullpath(path);
	    char *temp_path = cloudfs_get_fullpath(SEGMENT_TEMP_FILE);
	    temp_fd = open(temp_path, O_RDWR|O_CREAT,
                     S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (temp_fd == NULL) {
        free(data_fullpath);
        if (state_.no_compress)
		    close(segmenting_fd);
        else
		      fclose(segmenting_file);
		    unlink(data_fullpath);
        free(meta_fullpath);
        free(temp_path);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      if (sendfile(temp_fd, segmenting_fd, NULL, segment_len) < 0) {
        if (state_.no_compress)
		      close(segmenting_fd);
        else
		      fclose(segmenting_file);
        free(meta_fullpath);
        close(temp_fd);
        unlink(temp_path);
        free(temp_path);
        unlink(data_fullpath);
        free(data_fullpath);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      err = lseek(segmenting_fd, 0, SEEK_SET);
      if (err < 0) {
        if (state_.no_compress)
		      close(segmenting_fd);
        else
		      fclose(segmenting_file);
        free(meta_fullpath);
        close(temp_fd);
        unlink(temp_path);
        free(temp_path);
        unlink(data_fullpath);
        free(data_fullpath);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      err = ftruncate(segmenting_fd, 0);
      off_t start = 0;
      if (sendfile(segmenting_fd, temp_fd, &(start), segment_len) < 0) {
        if (state_.no_compress)
		      close(segmenting_fd);
        else
		      fclose(segmenting_file);
        free(meta_fullpath);
        close(temp_fd);
        unlink(temp_path);
        free(temp_path);
        unlink(data_fullpath);
        free(data_fullpath);
        #ifdef DEBUG
          printf("resetting rabin\n");
        #endif
        rabin_reset(rabin);
        return -1;
      }
      close(temp_fd);
      unlink(temp_path);
      free(temp_path);
      free(data_fullpath);
      free(meta_fullpath);
	  }
	}
	close(meta_file);
	if (in_ssd) {
	  err = ftruncate(segmenting_fd, 0);
	}
	if (state_.no_compress)
	  close(segmenting_fd);
  else
		fclose(segmenting_file);
  #ifdef DEBUG
    printf("resetting rabin\n");
  #endif
  rabin_reset(rabin);
  #ifdef DEBUG
    printf("done migrating file\n");
  #endif
  return 0;
}

int read_segment(char *hash, int bytes_to_read, char *buf,
                 off_t offset) {
  struct segment_hash_struct *segment;
  int err;
  S3Status status;
  char s3_bucket[4];
  char *data_path, *compress_temp_path;
  FILE *temp_file = NULL, *data_file = NULL;
  int temp_fd, data_fd;
  
  sprintf(log_string, "reading segment %s, %d bytes, offset %ld\n", hash, bytes_to_read, offset);
  log_write(log_string);
  if (state_.no_cache) {
    data_path = cloudfs_get_fullpath(SEGMENT_TEMP_FILE);
  }
  else {
    data_path = get_cache_fullpath(hash);
  }
  if (!state_.no_cache || (state_.no_cache && !in_cache(hash))) {
    if (!state_.no_cache) {
      HASH_FIND_STR(segment_hash_table, hash, segment);
      make_space_in_cache(segment->length);
    }
    s3_bucket[0] = hash[0];
    s3_bucket[1] = hash[1];
    s3_bucket[2] = hash[2];
    s3_bucket[3] = 0;
    if (!state_.no_compress) {
      compress_temp_path = cloudfs_get_fullpath(COMPRESS_TEMP_FILE);
      temp_fd = open(compress_temp_path, O_RDWR|O_CREAT,
                       S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (temp_fd == NULL) {
        free(compress_temp_path);
        free(data_path);
        sprintf(log_string, "read_segment line 783: %d\n", errno);
        log_write(log_string);
        return -1;
      }
      outfile = temp_fd;
      status = cloud_get_object(s3_bucket, hash+3, get_buffer);
      if (status != S3StatusOK) {
        #ifdef DEBUG
          cloud_print_error();
        #endif
        sprintf(log_string, "read_segment line 793: %d\n", status);
        log_write(log_string);
        close(temp_fd);
        unlink(compress_temp_path);
        free(data_path);
        free(compress_temp_path);
        return -1;
      }
      err = lseek(temp_fd, 0, SEEK_SET);
      if (err < 0) {
        sprintf(log_string, "read_segment line 803: %d\n", errno);
        log_write(log_string);
        close(temp_fd);
        unlink(compress_temp_path);
        free(data_path);
        free(compress_temp_path);
        return -1;
      }
      temp_file = fdopen(temp_fd, "r+");
      if (temp_file == NULL) {
        sprintf(log_string, "read_segment line 813: %d\n", errno);
        log_write(log_string);
        close(temp_fd);
        free(data_path);
        unlink(compress_temp_path);
        free(compress_temp_path);
        return -1;
      }
      data_file = fopen(data_path, "w+");
      if (data_file == NULL) {
        sprintf(log_string, "read_segment line 823: %d\n", errno);
        log_write(log_string);
        fclose(temp_file);
        unlink(compress_temp_path);
        free(compress_temp_path);
        free(data_path);
        return -1;
      }
      err = inf(temp_file, data_file);
      if (err != Z_OK) {
        sprintf(log_string, "read_segment line 833: %d\n", errno);
        log_write(log_string);
        fclose(data_file);
        unlink(data_path);
        fclose(temp_file);
        unlink(compress_temp_path);
        free(compress_temp_path);
        free(data_path);
        return -1;
      }
      fclose(data_file);
      fclose(temp_file);
      unlink(compress_temp_path);
      free(compress_temp_path);
    }
    else {
      data_fd = open(data_path, O_RDWR|O_CREAT,
                       S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
      if (data_fd == NULL) {
        free(data_path);
        return -1;
      }
      outfile = data_fd;
      status = cloud_get_object(s3_bucket, hash+3, get_buffer);
      if (status != S3StatusOK) {
        #ifdef DEBUG
          cloud_print_error();
        #endif
        close(data_fd);
        unlink(data_path);
        free(data_path);
        return -1;
      }
      close(data_fd);
    }
    if (!state_.no_cache) {
      add_to_cache(hash);
    }
  }
  else {
    update_in_cache(hash);
  }
  data_fd = open(data_path, O_RDONLY);
  if (data_fd == NULL) {
    unlink(data_path);
    free(data_path);
    sprintf(log_string, "read_segment line 872: %d\n", errno);
    log_write(log_string);
    return -1;
  }
  err = lseek(data_fd, offset, SEEK_SET);
  if (err < 0) {
    close(data_fd);
    sprintf(log_string, "read_segment line 879: %d\n", errno);
    log_write(log_string);
    unlink(data_path);
    free(data_path);
    return -1;
  }
  if (read(data_fd, buf, bytes_to_read) < 0) {
    close(data_fd);
    sprintf(log_string, "read_segment line 887: %d\n", errno);
    log_write(log_string);
    unlink(data_path);
    free(data_path);
    return -1;
  }
  close(data_fd);
  if (state_.no_cache)
    unlink(data_path);
  free(data_path);
  return 0;
}

int dedup_read(const char *path, char *buffer, size_t size,
               off_t offset, struct fuse_file_info *file_info) {
  int err, bytes_read, meta_file, data_file;
  unsigned int total_bytes_read = 0;
  struct segment_hash_struct *current_segment;
  struct stat info;
  char segment_hash[MD5_DIGEST_LENGTH*2+1];
  char *meta_fullpath, *data_fullpath;
  off_t file_size, segment_offset, current_offset = 0;
  int bytes_to_read;
  
  sprintf(log_string, "dedup_read to %s, %d bytes, offset %ld\n", path, size, offset);
  log_write(log_string);
  meta_fullpath = cloudfs_get_metadata_fullpath(path);
  meta_file = open(meta_fullpath, O_RDONLY);
  free(meta_fullpath);
  if (meta_file == NULL) {
    return -1;
  }
  if (read(meta_file, &file_size, sizeof(off_t)) != sizeof(off_t)) {
    close(meta_file);
    return -1;
  }
  if (offset >= file_size) {
    close(meta_file);
    return 0;
  }
  err = lseek(meta_file, META_SEGMENT_LIST, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    return -1;
  }
  while (1) {
    bytes_read = read(meta_file, segment_hash, MD5_DIGEST_LENGTH*2+1);
    if (bytes_read < 0) {
      sprintf(log_string, "dedup_read line 935: %d\n", errno);
      log_write(log_string);
      close(meta_file);
      return -1;
    }
    if (bytes_read != MD5_DIGEST_LENGTH*2+1) {
      data_fullpath = cloudfs_get_data_fullpath(path);
      err = stat(data_fullpath, &info);
      if (err) {
        sprintf(log_string, "dedup_read line 944: %d\n", errno);
        log_write(log_string);
        free(data_fullpath);
        close(meta_file);
        return -1;
      }
      data_file = open(data_fullpath, O_RDONLY);
      free(data_fullpath);
      if (data_file == NULL) {
        close(meta_file);
        sprintf(log_string, "dedup_read line 954: %d\n", errno);
        log_write(log_string);
        return -1;
      }
      err = lseek(data_file, offset - current_offset, SEEK_SET);
      if (err < 0) {
        sprintf(log_string, "dedup_read line 960: %d\n", errno);
        log_write(log_string);
        close(data_file);
        close(meta_file);
        return -1;
      }
      close(meta_file);
      bytes_read = read(data_file, buffer, size);
      close(data_file);
      return bytes_read;
    }
    HASH_FIND_STR(segment_hash_table, segment_hash, current_segment);
    if (current_segment == NULL) {
      close(meta_file);
      sprintf(log_string, "dedup_read line 974: %d\n", errno);
      log_write(log_string);
      return -1;
    }
    if (current_offset + current_segment->length > offset) {
      break;
    }
    current_offset += current_segment->length;
  }
  segment_offset = offset - current_offset;
  while (total_bytes_read < size) {
    if (size-total_bytes_read > current_segment->length-segment_offset) {
      bytes_to_read = current_segment->length-segment_offset;
    }
    else {
      bytes_to_read = size-total_bytes_read-segment_offset;
    }
    if (read_segment(segment_hash, bytes_to_read, buffer+total_bytes_read,
                     segment_offset)) {
      close(meta_file);
      return -1;
    }
    total_bytes_read += bytes_to_read;
    current_offset += current_segment->length;
    segment_offset = 0;
    if (total_bytes_read == size) {
      break;
    }
    if (current_offset == file_size) {
      break;
    }
    bytes_read = read(meta_file, segment_hash, MD5_DIGEST_LENGTH*2+1);
    if (bytes_read < 0) {
      close(meta_file);
      sprintf(log_string, "dedup_read line 1008: %d\n", errno);
      log_write(log_string);
      return -1;
    }
    if (bytes_read != MD5_DIGEST_LENGTH*2+1) {
      data_fullpath = cloudfs_get_data_fullpath(path);
      err = stat(data_fullpath, &info);
      if (err) {
        free(data_fullpath);
        close(meta_file);
        sprintf(log_string, "dedup_read line 1018: %d\n", errno);
        log_write(log_string);
        return -1;
      }
      data_file = open(data_fullpath, O_RDONLY);
      free(data_fullpath);
      if (data_file == NULL) {
        close(meta_file);
        sprintf(log_string, "dedup_read line 1026: %d\n", errno);
        log_write(log_string);
        return -1;
      }
      bytes_read = read(data_file, buffer+total_bytes_read, size);
      if (bytes_read < 0) {
        close(data_file);
        sprintf(log_string, "dedup_read line 1033: %d\n", errno);
        log_write(log_string);
        close(meta_file);
        return -1;
      }
      close(meta_file);
      close(data_file);
      return total_bytes_read+bytes_read;
    }
    HASH_FIND_STR(segment_hash_table, segment_hash, current_segment);
    if (current_segment == NULL) {
      close(meta_file);
      sprintf(log_string, "dedup_read line 1045: hash = %s, %d\n", segment_hash, errno);
      log_write(log_string);
      return -1;
    }
  }
  close(meta_file);
  return total_bytes_read;
}

int dedup_get_last_segment(const char *data_target_path, int meta_file) {
  struct stat info;
  struct segment_hash_struct *last_segment;
  char segment_hash[MD5_DIGEST_LENGTH*2+1];
  char s3_bucket[4];
  S3Status status;
  char *compress_temp_path;
  FILE *temp, *data;
  int err, data_file, temp_file;
  
  err = lseek(meta_file, MD5_DIGEST_LENGTH*2+1, SEEK_END);
  if (err < 0) {
    return -1;
  }
  if (read(meta_file, segment_hash, MD5_DIGEST_LENGTH*2+1) != MD5_DIGEST_LENGTH*2+1) {
    return -1;
  }
  s3_bucket[0] = segment_hash[0];
  s3_bucket[1] = segment_hash[1];
  s3_bucket[2] = segment_hash[2];
  s3_bucket[3] = 0;
  if (!state_.no_compress) {
    compress_temp_path = cloudfs_get_fullpath(COMPRESS_TEMP_FILE);
    temp_file = open(compress_temp_path, O_RDWR|O_CREAT,
                     S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if (temp_file == NULL) {
      free(compress_temp_path);
      return -1;
    }
    outfile = temp_file;
    status = cloud_get_object(s3_bucket, segment_hash+3, get_buffer);
    if (status != S3StatusOK) {
      #ifdef DEBUG
        cloud_print_error();
      #endif
      close(temp_file);
      unlink(compress_temp_path);
      free(compress_temp_path);
      return -1;
    }
    temp = fdopen(temp_file, "r+");
    if (temp == NULL) {
      close(temp_file);
      unlink(compress_temp_path);
      free(compress_temp_path);
      return -1;
    }
    
    data = fopen(data_target_path, "w+");
    if (data == NULL) {
      fclose(temp);
      unlink(compress_temp_path);
      free(compress_temp_path);
      return -1;
    }
    err = inf(temp, data);
    if (err != Z_OK) {
      fclose(data);
      unlink(data_target_path);
      fclose(temp);
      unlink(compress_temp_path);
      free(compress_temp_path);
      return -1;
    }
    fclose(data);
    fclose(temp);
    unlink(compress_temp_path);
    free(compress_temp_path);
  }
  else {
    data_file = open(data_target_path, O_RDWR|O_CREAT,
                     S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if (data_file == NULL) {
      return -1;
    }
    outfile = data_file;
    status = cloud_get_object(s3_bucket, segment_hash+3, get_buffer);
    if (status != S3StatusOK) {
      #ifdef DEBUG
        cloud_print_error();
      #endif
      close(data_file);
      unlink(data_target_path);
      return -1;
    }
    close(data_file);
  }
  fstat(meta_file, &info);
  if (ftruncate(meta_file, info.st_size-MD5_DIGEST_LENGTH*2+1)) {
    unlink(data_target_path);
    return -1;
  }
  HASH_FIND_STR(segment_hash_table, segment_hash, last_segment);
  if (last_segment == NULL) {
    unlink(data_target_path);
    return -1;
  }
  if (last_segment->ref_count > 1) {
    last_segment->ref_count--;
  }
  else {
    sprintf(log_string, "removing %s from the hash table\n", segment_hash);
    log_write(log_string);
    if (!state_.no_cache) {
      remove_from_cache(segment_hash);
    }
    HASH_DEL(segment_hash_table, last_segment);
    free(last_segment);
    s3_bucket[0] = segment_hash[0];
    s3_bucket[1] = segment_hash[1];
    s3_bucket[2] = segment_hash[2];
    s3_bucket[3] = 0;
    cloud_delete_object(s3_bucket, segment_hash+3);
  }
  return update_hash_table_file();
}

int dedup_unlink_segments(const char *meta_path) {
  struct segment_hash_struct *current_segment;
  char current_hash[MD5_DIGEST_LENGTH*2+1];
  char s3_bucket[4];
  int meta_file, bytes_read, err;

  meta_file = open(meta_path, O_RDONLY);
  if (meta_file == NULL) {
    return -1;
  }
  err = lseek(meta_file, META_SEGMENT_LIST, SEEK_SET);
  if (err < 0) {
    close(meta_file);
    return -1;
  }
  while (1) {
    bytes_read = read(meta_file, current_hash, MD5_DIGEST_LENGTH*2+1);
    if (bytes_read == 0) {
      break;
    }
    else if (bytes_read != MD5_DIGEST_LENGTH*2+1) {
      close(meta_file);
      return -1;
    }
    HASH_FIND_STR(segment_hash_table, current_hash, current_segment);
    if (current_segment == NULL)
      continue;
    sprintf(log_string, "unlinking segment %s, ref_count=%d\n", current_hash, current_segment->ref_count);
    log_write(log_string);
    if (current_segment->ref_count > 1) {
      current_segment->ref_count--;
    }
    else {
      sprintf(log_string, "removing %s from the hash table\n", current_segment->hash);
      log_write(log_string);
      if (!state_.no_cache) {
        remove_from_cache(current_hash);
      }
      HASH_DEL(segment_hash_table, current_segment);
      free(current_segment);
      current_segment = NULL;
      s3_bucket[0] = current_hash[0];
      s3_bucket[1] = current_hash[1];
      s3_bucket[2] = current_hash[2];
      s3_bucket[3] = 0;
      cloud_delete_object(s3_bucket, current_hash+3);
    }
    sprintf(log_string, "done unlinking segment %s, ref_count=%d\n", current_hash, ((current_segment == NULL) ? 0 : current_segment->ref_count));
    log_write(log_string);
  }
  close(meta_file);
  return update_hash_table_file();
}
