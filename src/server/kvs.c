#include "kvs.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "../common/io.h"
#include "operations.h"
#include "string.h"

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of
// the project
int hash(const char *key) {
  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1;  // Invalid index for non-alphabetic or number strings
}

struct HashTable *create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));

  if (!ht) return NULL;

  pthread_rwlock_init(&ht->GlobalLock, NULL);

  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i] = NULL;
    pthread_rwlock_init(&ht->rwlock[i], NULL);
  }
  return ht;
}

int register_subscribe(int fd, HashTable *ht, char *key) {
  printf("key: %c\n", *key);
  int index = hash(key);
  printf("passou do hash\n");
  KeyNode *keyNode = ht->table[index];
  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        printf("keynode: %d\n", keyNode->subscribers[i]);
        if (keyNode->subscribers[i] == 0) {
          keyNode->subscribers[i] = fd;
          // printf("keynode com subscrição: %d\n", keyNode->subscribers[i]);
          for (int l = 0; l < MAX_SESSION_COUNT; l++) {
            printf("keynode->subscribers[%d]: %d\n", l,
                   keyNode->subscribers[l]);
          }
          return 1;
        }
      }
    }
    keyNode = keyNode->next;
  }
  return 0;
}

int register_unsubscribe(int fd, HashTable *ht, char *key) {
  printf("key in register_unsubscribe: %c\n", *key);
  int index = hash(key);
  KeyNode *keyNode = ht->table[index];

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      for (int l = 0; l < MAX_SESSION_COUNT; l++) {
        printf("keynode->subscribers[%d]: %d\n", l, keyNode->subscribers[l]);
      }
      printf("^ subs of each key before unsub\n");

      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        printf("keynode->subscribers[%d]: %d\n", i, keyNode->subscribers[i]);
        printf("fd: %d\n", fd);
        if (keyNode->subscribers[i] == fd) {
          printf("achou o sub: %s\n", keyNode->key);
          keyNode->subscribers[i] = 0;
          printf("se fez unsub isto deve ser zero: %s\n", keyNode->key);
          return 0;
        }
      }
      return 1;
    }
    keyNode = keyNode->next;
  }
  printf("não achou o sub\n");
  return 1;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
  int index = hash(key);
  int notif_fd;
  KeyNode *keyNode = ht->table[index];

  // Search for the key node
  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      free(keyNode->value);
      keyNode->value = strdup(value);
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (keyNode->subscribers[i] != 0) {
          notif_fd = keyNode->subscribers[i];
          char buffer[82];
          snprintf(buffer, sizeof(buffer), "%s%s", key, value);
          if (write_all(notif_fd, buffer, strlen(buffer)) == -1) {
            return 1;
          }
          return 0;
        }
      }
      return 0;
    }
    keyNode = keyNode->next;  // Move to the next node
  }

  // Key not found, create a new key node
  keyNode = malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);        // Allocate memory for the key
  keyNode->value = strdup(value);    // Allocate memory for the value
  keyNode->next = ht->table[index];  // Link to existing nodes
  ht->table[index] = keyNode;  // Place new key node at the start of the list
  memset(keyNode->subscribers, 0, MAX_SESSION_COUNT * sizeof(int));
  return 0;
}

// Returns
// NULL -> key doesn't exist
// char* -> key exists
char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);
  KeyNode *keyNode = ht->table[index];
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value;  // Return copy of the value if found
    }
    keyNode = keyNode->next;  // Move to the next node
  }
  return NULL;  // Key not found
}

int kvs_disconnect_server(int req_fd, int resp_fd, int notif_fd) {
  int res = clean_client(notif_fd);
  char message_sent[3];
  message_sent[0] = '2';
  message_sent[1] = (char)(res + '0');
  message_sent[2] = '\0';
  if (write_all(resp_fd, message_sent, 3) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  return 0;
}

int delete_pair(HashTable *ht, const char *key) {
  int index = hash(key);
  char key_sent[41];
  int notif_fd;
  KeyNode *keyNode = ht->table[index];
  KeyNode *prevNode = NULL;
  //  Search for the key node
  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Key found; delete this node
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index] =
            keyNode->next;  // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next;  // Link the previous node to the next node
      }
      sprintf(key_sent, "%s", keyNode->key);
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (keyNode->subscribers[i] != 0) {
          notif_fd = keyNode->subscribers[i];
          char buffer[82];
          snprintf(buffer, sizeof(buffer), "%s%s", key_sent, "DELETED");
          if (write_all(notif_fd, buffer, strlen(buffer)) == -1) {
            return 1;
          }
        }
      }
      // Free the memory allocated for the key and value
      free(keyNode->key);
      free(keyNode->value);
      free(keyNode);  // Free the key node itself
      return 0;       // Exit the function
    }
    prevNode = keyNode;       // Move prevNode to current node
    keyNode = keyNode->next;  // Move to the next node
  }
  return 1;
}

void free_table(HashTable *ht) {
  pthread_rwlock_wrlock(&ht->GlobalLock);

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp);
    }
    pthread_rwlock_destroy(&ht->rwlock[i]);
  }
  pthread_rwlock_unlock(&ht->GlobalLock);
  pthread_rwlock_destroy(&ht->GlobalLock);
  free(ht);
}