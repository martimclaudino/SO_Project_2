#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "kvs.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms)
{
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init()
{
  if (kvs_table != NULL)
  {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate()
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int already_subscribed(int fd, char *key)
{
  int index = hash(key);
  KeyNode *keyNode = kvs_table->table[index];
  while (keyNode != NULL)
  {
    if (strcmp(keyNode->key, key) == 0)
    {
      for (int i = 0; i < MAX_SESSION_COUNT; i++)
      {
        if (keyNode->subscribers[i] == fd)
        {
          return 1;
        }
      }
      return 0;
    }
    keyNode = keyNode->next;
  }
  return 0;
}

int check_if_pair_exists(char *key)
{
  int index = hash(key);
  KeyNode *keyNode = kvs_table->table[index];

  while (keyNode != NULL)
  {
    if (strcmp(keyNode->key, key) == 0)
    {
      return 1; // Return copy of the value if found
    }
    keyNode = keyNode->next; // Move to the next node
  }
  return 0; // Key not found
}

int clean_client(int notif_fd)
{
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL)
    {
      for (int j = 0; j < MAX_SESSION_COUNT; j++)
      {
        if (keyNode->subscribers[j] == notif_fd)
        {
          keyNode->subscribers[j] = 0;
        }
      }
      keyNode = keyNode->next;
    }
  }
  return 0;
}

void kvs_subscribe(int fd, char *key)
{
  register_subscribe(fd, kvs_table, key);
}

int kvs_unsubscribe(int fd, char *key)
{
  return register_unsubscribe(fd, kvs_table, key);
}

void sort_keys_and_values(char keys[][MAX_STRING_SIZE],
                          char values[][MAX_STRING_SIZE], size_t num_pairs)
{
  for (size_t i = 0; i < num_pairs - 1; i++)
  {
    for (size_t j = 0; j < num_pairs - i - 1; j++)
    {
      if (strcmp(keys[j], keys[j + 1]) > 0)
      {
        // Swap keys
        char temp[MAX_STRING_SIZE];
        strcpy(temp, keys[j]);
        strcpy(keys[j], keys[j + 1]);
        strcpy(keys[j + 1], temp);

        // Swap values
        strcpy(temp, values[j]);
        strcpy(values[j], values[j + 1]);
        strcpy(values[j + 1], temp);
      }
    }
  }
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE])
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  sort_keys_and_values(keys, values, num_pairs);
  int locked_keys[TABLE_SIZE] = {0};

  pthread_rwlock_rdlock(&kvs_table->GlobalLock);
  // Lock all unlocked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 0)
    {
      pthread_rwlock_wrlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 1;
    }
  }

  for (size_t i = 0; i < num_pairs; i++)
  {
    if (write_pair(kvs_table, keys[i], values[i]) != 0)
    {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  // Unlock all locked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 1)
    {
      pthread_rwlock_unlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 0;
    }
  }
  pthread_rwlock_unlock(&kvs_table->GlobalLock);

  return 0;
}

void sort_only_keys(char keys[][MAX_STRING_SIZE], size_t num_pairs)
{
  for (size_t i = 0; i < num_pairs - 1; i++)
  {
    for (size_t j = 0; j < num_pairs - i - 1; j++)
    {
      if (strcmp(keys[j], keys[j + 1]) > 0)
      {
        // Swap keys
        char temp[MAX_STRING_SIZE];
        strcpy(temp, keys[j]);
        strcpy(keys[j], keys[j + 1]);
        strcpy(keys[j + 1], temp);
      }
    }
  }
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd)
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  sort_only_keys(keys, num_pairs);
  int locked_keys[TABLE_SIZE] = {0};
  write(fd, "[", 1);

  pthread_rwlock_rdlock(&kvs_table->GlobalLock);
  // Lock all unlocked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 0)
    {
      pthread_rwlock_rdlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 1;
    }
  }

  for (size_t i = 0; i < num_pairs; i++)
  {
    char *result = read_pair(kvs_table, keys[i]);
    char buffer[MAX_STRING_SIZE];
    if (result == NULL)
    {
      sprintf(buffer, "(%s,KVSERROR)", keys[i]);
      write(fd, buffer, strlen(buffer));
    }
    else
    {
      sprintf(buffer, "(%s,%s)", keys[i], result);
      write(fd, buffer, strlen(buffer));
    }
    free(result);
  }
  // Unlock all locked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 1)
    {
      pthread_rwlock_unlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 0;
    }
  }
  pthread_rwlock_unlock(&kvs_table->GlobalLock);

  write(fd, "]\n", 2);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd)
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int counter = 0;
  char missing_keys[num_pairs][MAX_STRING_SIZE];
  sort_only_keys(keys, num_pairs);
  int locked_keys[TABLE_SIZE] = {0};

  pthread_rwlock_rdlock(&kvs_table->GlobalLock);
  // Lock all unlocked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 0)
    {
      pthread_rwlock_wrlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 1;
    }
  }
  for (size_t i = 0; i < num_pairs; i++)
  {
    if (delete_pair(kvs_table, keys[i]) != 0)
    {
      // Put missing keys in an array
      strcpy(missing_keys[counter], keys[i]);
      counter++;
    }
  }
  // Unlock all locked keys
  for (size_t i = 0; i < num_pairs; i++)
  {
    int index = hash(keys[i]);
    if (locked_keys[index] == 1)
    {
      pthread_rwlock_unlock(&kvs_table->rwlock[index]);
      locked_keys[index] = 0;
    }
  }
  pthread_rwlock_unlock(&kvs_table->GlobalLock);

  if (counter > 0)
  {
    // Write missing keys to the file descriptor
    write(fd, "[", 1);
    for (int i = 0; i < counter; i++)
    {
      size_t BUFFER_SIZE = strlen(missing_keys[i]) + 13;
      char buffer[MAX_STRING_SIZE + 13];
      sprintf(buffer, "(%s,KVSMISSING)", missing_keys[i]);
      write(fd, buffer, BUFFER_SIZE);
    }
    write(fd, "]\n", 2);
  }

  return 0;
}

void kvs_show(int fd)
{
  pthread_rwlock_wrlock(&kvs_table->GlobalLock);
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL)
    {
      // Calculate the length of the formatted string
      int len = snprintf(NULL, 0, "(%s, %s)\n", keyNode->key, keyNode->value);
      char buffer[len + 1]; // Allocate buffer with the required length
      snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key,
               keyNode->value);
      write(
          fd, buffer,
          strlen(buffer));     // Write the formatted string to the file descriptor
      keyNode = keyNode->next; // Move to the next node
    }
  }
  pthread_rwlock_unlock(&kvs_table->GlobalLock);
}

int kvs_backup(int fd, int out_fd, const char *job_path, size_t pathLength,
               int *bck_counter, DIR *dir)
{
  // Lock all keys
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    pthread_rwlock_rdlock(&kvs_table->rwlock[i]);
  }
  int pid = fork();

  if (pid == 0)
  {
    char bck_path[MAX_JOB_FILE_NAME_SIZE];
    char *dot1 = strrchr(job_path, '.');
    if (dot1 != NULL)
    {
      *dot1 = '\0'; // Removes ".job"
    }

    snprintf(bck_path, pathLength + (size_t)(2 + (*bck_counter / 10)),
             "%s-%d.bck", job_path, *bck_counter);
    int bck_fd = open(bck_path, O_WRONLY | O_CREAT, 0644);
    for (int i = 0; i < TABLE_SIZE; i++)
    {
      KeyNode *keyNode = kvs_table->table[i];
      while (keyNode != NULL)
      {
        // Calculate the length of the formatted string
        int len = snprintf(NULL, 0, "(%s, %s)\n", keyNode->key, keyNode->value);
        char buffer[len + 1]; // Allocate buffer with the required length
        snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key,
                 keyNode->value);
        write(
            bck_fd, buffer,
            strlen(
                buffer));        // Write the formatted string to the file descriptor
        keyNode = keyNode->next; // Move to the next node
      }
    }
    close(bck_fd);
    close(fd);
    close(out_fd);
    closedir(dir);
    _exit(0);
  }
  else
  {
    // Unlock all keys
    for (int i = 0; i < TABLE_SIZE; i++)
    {
      pthread_rwlock_unlock(&kvs_table->rwlock[i]);
    }
    (*bck_counter)++;
  }

  return 0;
}

void kvs_wait(unsigned int delay_ms)
{
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}