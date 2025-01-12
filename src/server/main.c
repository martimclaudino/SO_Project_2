#include <dirent.h> // Para manipulação de diretórios
#include <errno.h>  // Para interpretação de erros
#include <fcntl.h>  // Para open()
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // Para funções de manipulação de strings
#include <sys/stat.h>
#include <sys/types.h> // Para tipos POSIX básicos
#include <sys/wait.h>
#include <unistd.h>

#include <signal.h>

#include "../common/io.h"
#include "../common/protocol.h"
#include "constants.h"
#include "kvs.h"
#include "operations.h"
#include "parser.h"

#define min(a, b) ((a) < (b) ? (a) : (b))

typedef struct
{
  char *job_path;
  size_t pathLength;
  int bcks_max;
  DIR *dir;
  int *thread_index;
} FilePath;

typedef struct
{
  char *req_pipe_path;
  char *resp_pipe_path;
  char buffer[121];

} Client;

int count_job_files(const char *directory);

void *process_file(void *counter);

int running_threads = 0;
int bck_executing = 0;
pthread_mutex_t thread_lock;
pthread_mutex_t backup_lock;
FilePath *file_paths = NULL;
int file_num = 0;
pthread_mutex_t sem_mutex = PTHREAD_MUTEX_INITIALIZER;
int insert_index = 0;
int remove_index = 0;
char queue[MAX_SESSION_COUNT][121];
sem_t full;
sem_t empty;

// returns
// 0->key doesn't exist
// 1->key exists
int subscribe(int fd, char *key)
{
  if (!check_if_pair_exists(key))
  {
    return 0;
  }
  if (!already_subscribed(fd, key))
    kvs_subscribe(fd, key);
  return 1;
}

// Returns
// 0 -> subscription existed and removed
// 1 -> subscription didn't exist
int unsubscribe(int fd, char *key)
{
  if (!check_if_pair_exists(key))
  {
    return 1;
  }
  printf("key exists\n");

  if (already_subscribed(fd, key))
  {
    printf("subscription existed\n");
    kvs_unsubscribe(fd, key);
    return 0;
  }
  return 1;
}

void *respond_client();

// Puts the pipes into the queue
void write_msg(char *req_pipe, char *resp_pipe, char *notif_pipe)
{
  sem_wait(&empty);
  pthread_mutex_lock(&sem_mutex);

  memcpy(queue[insert_index], "1", 1);
  memcpy(queue[insert_index] + 1, req_pipe, 40);
  memcpy(queue[insert_index] + 41, resp_pipe, 40);
  memcpy(queue[insert_index] + 81, notif_pipe, 40);

  insert_index = (insert_index + 1) % MAX_SESSION_COUNT;
  pthread_mutex_unlock(&sem_mutex);
  sem_post(&full);
}

// Gets the pipes from the queue
void read_msg(char *Msg)
{
  printf("entrou no read_msg\n");
  sem_wait(&full);
  printf("passou no semaforo\n");
  pthread_mutex_lock(&sem_mutex);
  memcpy(Msg, queue[remove_index], 121);
  // strcpy(Msg, queue[remove_index]);
  queue[remove_index][0] = '\0';
  remove_index = (remove_index + 1) % MAX_SESSION_COUNT;
  pthread_mutex_unlock(&sem_mutex);
  sem_post(&empty);
}

int main(int argc, char *argv[])
{
  if (argc != 5)
  {
    fprintf(stderr, "Usage: %s <directory> <n_backups>\n", argv[0]);
    return 1;
  }

  if (kvs_init())
  {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  pthread_mutex_init(&thread_lock, NULL);
  pthread_mutex_init(&backup_lock, NULL);
  int bcks_max = atoi(argv[3]);

  int MAX_THREADS = atoi(argv[2]);
  int thread_counter = 0;

  char *server_pipe_path = argv[4];

  unlink(server_pipe_path);
  if (mkfifo(server_pipe_path, 0666) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }

  char *directory = argv[1];
  file_num = count_job_files(directory);
  file_paths = (FilePath *)malloc((size_t)file_num * sizeof(FilePath));

  pthread_t threads[file_num];

  DIR *dir = opendir(directory);
  if (dir)
  {
    struct dirent *entry;
    while ((entry = readdir(dir)))
    {
      if (strstr(entry->d_name, ".job"))
      {
        size_t pathLength = strlen(directory) + strlen(entry->d_name) + 2;

        file_paths[thread_counter].job_path =
            (char *)malloc(pathLength * sizeof(char));
        snprintf(file_paths[thread_counter].job_path, pathLength, "%s/%s",
                 directory, entry->d_name);

        file_paths[thread_counter].pathLength =
            strlen(directory) + strlen(entry->d_name) + 2;
        file_paths[thread_counter].bcks_max = bcks_max;
        file_paths[thread_counter].dir = dir;

        while (1)
        {
          pthread_mutex_lock(&thread_lock);
          if (running_threads < MAX_THREADS)
          {
            int *thread_index = malloc(sizeof(int));
            *thread_index = thread_counter;
            file_paths[thread_counter].thread_index = thread_index;
            pthread_create(&threads[thread_counter], NULL, process_file,
                           (void *)thread_index);
            thread_counter++;
            running_threads++;
            pthread_mutex_unlock(&thread_lock);
            break;
          }
          pthread_mutex_unlock(&thread_lock);
        }
      }
    }
  }
  else
  {
    printf("Erro ao abrir o diretório\n");
  }
  int pipe_fd;
  if ((pipe_fd = open(server_pipe_path, O_RDONLY)) < 0)
  {
    perror("Failed to open named pipe");
    return 1;
  }

  sem_init(&full, 0, 0);
  sem_init(&empty, 0, MAX_SESSION_COUNT);

  pthread_t t_client[MAX_SESSION_COUNT];

  for (int i = 0; i < MAX_SESSION_COUNT; i++)
  {
    pthread_create(&t_client[i], NULL, respond_client, NULL);
  }

  /* sigset_t set;
  int sig;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1); */

  while (1)
  {
    char op_code = -1;
    char req_pipe[41];
    char resp_pipe[41];
    char notify_pipe[41];

    if (read_all(pipe_fd, &op_code, 1, NULL) == -1)
    {
      fprintf(stderr, "Failed to read message from client opcode\n");
      return 1;
    }

    if (op_code == '1')
    {
      printf("entrou no if do opcode\n");
      if (read_all(pipe_fd, req_pipe, 40, NULL) != 1)
      {
        fprintf(stderr, "Failed to read message from client req\n");
        return 1;
      }
      req_pipe[40] = '\0';

      if (read_all(pipe_fd, resp_pipe, 40, NULL) != 1)
      {
        fprintf(stderr, "Failed to read message from client resp\n");
        return 1;
      }
      resp_pipe[40] = '\0';

      if (read_all(pipe_fd, notify_pipe, 40, NULL) != 1)
      {
        fprintf(stderr, "Failed to read message from client notif\n");
        return 1;
      }
      notify_pipe[40] = '\0';
      // Puts the pipes into the queue
      write_msg(req_pipe, resp_pipe, notify_pipe);
    }
  }

  for (int i = 0; i < thread_counter; i++)
  {
    pthread_join(threads[i], NULL);
  }
  for (int i = 0; i < file_num; i++)
  {
    free(file_paths[i].job_path);
  }
  free(file_paths);
  closedir(dir);
  pthread_mutex_destroy(&thread_lock);
  pthread_mutex_destroy(&backup_lock);
  kvs_terminate();
}

int count_job_files(const char *directory)
{
  DIR *dir = opendir(directory);
  if (!dir)
  {
    perror("Failed to open directory");
    return -1; // Error code
  }

  int job_file_count = 0;
  struct dirent *entry;

  while ((entry = readdir(dir)))
  {
    // Check if the file ends with ".job"
    if (strstr(entry->d_name, ".job"))
    {
      job_file_count++;
    }
  }

  closedir(dir);
  return job_file_count;
}

void *respond_client()
{
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  while (1)
  {
    // Spliting the 3 client paths

    char client_msg[121];
    read_msg(client_msg);

    int req_fd, resp_fd, notif_fd;
    char opcode = client_msg[0];
    char req_pipe_path[41];
    char resp_pipe_path[41];
    char notif_pipe_path[41];
    strncpy(req_pipe_path, client_msg + 1, 40);
    req_pipe_path[40] = '\0';
    strncpy(resp_pipe_path, client_msg + 41, 40);
    resp_pipe_path[40] = '\0';
    strncpy(notif_pipe_path, client_msg + 81, 40);
    notif_pipe_path[40] = '\0';

    /* printf("req_pipe_path: %s\n", req_pipe_path);
    printf("resp_pipe_path: %s\n", resp_pipe_path);
    printf("notif_pipe_path: %s\n", notif_pipe_path); */

    // Opening the pipes
    if ((resp_fd = open(resp_pipe_path, O_WRONLY)) < 0)
    {
      perror("Failed to open response pipe");
      return NULL;
    }

    if ((notif_fd = open(notif_pipe_path, O_WRONLY)) < 0)
    {
      write_all(resp_fd, opcode + "1", 2);
      perror("Failed to open notification pipe");
      return NULL;
    }
    if ((req_fd = open(req_pipe_path, O_RDONLY)) < 0)
    {
      write_all(resp_fd, opcode + "1", 2);
      perror("Failed to open request pipe");
      return NULL;
    }

    // Connection successful message
    char connection_output[3];
    connection_output[0] = opcode;
    connection_output[1] = '\0';
    strcat(connection_output, "0");
    printf("connection_output: %s\n", connection_output);
    if (write_all(resp_fd, connection_output, 3) == -1)
    {
      perror("Failed to write to response pipe");
      return NULL;
    }

    while (1)
    {
      // Spliting the 3 client paths
      char opcode2;
      printf("antes do opcode\n");
      if (read_all(req_fd, &opcode2, 1, NULL) == -1)
      {
        fprintf(stderr, "Failed to read message from client\n");
        break;
      }
      char key[41];
      if (opcode2 == '2')
      {
        kvs_disconnect_server(req_fd, resp_fd, notif_fd);
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        break;
      }
      else if (opcode2 == '3')
      {
        char received_key[41];
        if (read_all(req_fd, received_key, 40, NULL) == -1)
        {
          fprintf(stderr, "Failed to read message from client\n");
          break;
        }
        received_key[40] = '\0';
        strcpy(key, received_key);
        printf("key: %s\n", key);
        printf("notif_fd: %d\n", notif_fd);
        int res = subscribe(notif_fd, key);

        char subscription_output[3];
        subscription_output[0] = opcode2;
        subscription_output[1] = (char)(res + '0');
        subscription_output[2] = '\0';

        printf("subscription output: ");
        for (int i = 0; i < 3; i++)
        {
          if (subscription_output[i] == '\0')
            printf("\\0");
          else
            printf("%c", subscription_output[i]);
        }
        printf("\n");

        if (write_all(resp_fd, subscription_output, 3) == -1)
        {
          perror("Failed to write to response pipe");
          break;
        }
      }
      else if (opcode2 == '4')
      {
        char received_key[41];
        if (read_all(req_fd, received_key, 40, NULL) == -1)
        {
          fprintf(stderr, "Failed to read message from client\n");
          break;
        }
        received_key[40] = '\0';
        strcpy(key, received_key);
        printf("key to unsub: %s\n", key);

        int res2 = unsubscribe(notif_fd, key);
        printf("res2: %d\n", res2);
        // Unsubscription output
        char unsubscription_output[3];
        unsubscription_output[0] = opcode2;
        unsubscription_output[1] = (char)(res2 + '0');
        unsubscription_output[2] = '\0';
        if (write_all(resp_fd, unsubscription_output, 3) == -1)
        {
          perror("Failed to write to response pipe");
          break;
        }
      }
    }
  }
}

void *process_file(void *counter)
{
  // Process .job file
  int index = *((int *)counter);
  free(counter);
  FilePath file0 = file_paths[index];
  char *job_path = file0.job_path;
  int bcks_max = file0.bcks_max;
  size_t pathLength = file0.pathLength;
  DIR *dir = file0.dir;

  int fd = open(job_path, O_RDONLY);
  if (fd == -1)
  {
    perror("Failed to open job file");
    goto cleanup;
  }

  char out_path[MAX_JOB_FILE_NAME_SIZE];
  char *dot = strrchr(job_path, '.');
  if (dot != NULL)
  {
    *dot = '\0'; // Removes ".job"
  }

  snprintf(out_path, pathLength, "%s.out", job_path);
  int out_fd = open(out_path, O_WRONLY | O_CREAT, 0644);
  int finish = 0;
  int bck_counter = 1;
  while (finish != 1)
  {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(fd))
    {
    case CMD_WRITE:
      num_pairs =
          parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values))
      {
        fprintf(stderr, "Failed to write pair\n");
      }

      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd))
      {
        fprintf(stderr, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd))
      {
        fprintf(stderr, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:

      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(fd, &delay, NULL) == -1)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0)
      {
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&backup_lock);
      if (bck_executing == bcks_max)
      {
        wait(NULL);
        (bck_executing)--;
      }
      else
      {
        (bck_executing)++;
      }

      if (kvs_backup(fd, out_fd, job_path, pathLength, &bck_counter, dir))
      {
        fprintf(stderr, "Failed to perform backup.\n");
      }
      pthread_mutex_unlock(&backup_lock);

      break;
    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      printf(
          "Available commands:\n"
          "  WRITE [(key,value),(key2,value2),...]\n"
          "  READ [key,key2,...]\n"
          "  DELETE [key,key2,...]\n"
          "  SHOW\n"
          "  WAIT <delay_ms>\n"
          "  BACKUP\n"
          "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      finish = 1;
      break;
    }
  }
cleanup:
  pthread_mutex_lock(&thread_lock);
  running_threads--;
  pthread_mutex_unlock(&thread_lock);
  close(out_fd);
  close(fd);
  pthread_exit(NULL);
}