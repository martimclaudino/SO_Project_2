#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

#include <fcntl.h>     // Para open()
#include <sys/types.h> // Para tipos POSIX básicos
#include <dirent.h>    // Para manipulação de diretórios
#include <string.h>    // Para funções de manipulação de strings
#include <unistd.h>    // Para write()
#include <errno.h>     // Para interpretação de erros
#include "parser.h"
#include <sys/wait.h>
#include <pthread.h>
#include "../common/io.h"

#define min(a, b) ((a) < (b) ? (a) : (b))

typedef struct
{
  char *job_path;
  size_t pathLength;
  int bcks_max;
  DIR *dir;
  int *thread_index;
} FilePath;

int count_job_files(const char *directory);

void *process_file(void *counter);

int running_threads = 0;
int bck_executing = 0;
pthread_mutex_t thread_lock;
pthread_mutex_t backup_lock;
FilePath *file_paths = NULL;
int file_num = 0;

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
  int bcks_max = atoi(argv[2]);

  int MAX_THREADS = atoi(argv[3]);
  int thread_counter = 0;

  char *named_pipe_path = argv[4];

  unlink(named_pipe_path);
  if (mkfifo(named_pipe_path, 0666) < 0)
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

        file_paths[thread_counter].job_path = (char *)malloc(pathLength * sizeof(char));
        snprintf(file_paths[thread_counter].job_path, pathLength, "%s/%s", directory, entry->d_name);

        file_paths[thread_counter].pathLength = strlen(directory) + strlen(entry->d_name) + 2;
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
            pthread_create(&threads[thread_counter], NULL, process_file, (void *)thread_index);
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
  if (pipe_fd = open(named_pipe_path, O_RDONLY) < 0)
  {
    perror("Failed to open named pipe");
    return 1;
  }
  // Receiving client connections
  while (1)
  {
    char buffer[121];
    if (read_all(pipe_fd, buffer, 121, NULL) == -1)
    {
      fprintf(stderr, "Failed to read message from client\n");
      return 1;
    }

    // Spliting the 3 client paths
    char opcode = buffer[0];
    char req_pipe_path[41];
    char resp_pipe_path[41];
    char notif_pipe_path[41];
    strncpy(req_pipe_path, buffer + 1, 40);
    req_pipe_path[40] = '\0';
    strncpy(resp_pipe_path, buffer + 41, 40);
    resp_pipe_path[40] = '\0';
    strncpy(notif_pipe_path, buffer + 81, 40);
    notif_pipe_path[40] = '\0';

    int req_fd, resp_fd, notif_fd;

    // Opening the pipes
    if (resp_fd = open(resp_fd, O_RDONLY) < 0)
    {
      perror("Failed to open response pipe");
      return 1;
    }
    if (req_fd = open(req_fd, O_WRONLY) < 0)
    {
      write(resp_pipe_path, opcode + "1", 2);
      perror("Failed to open request pipe");
      return 1;
    }

    if (notif_fd = open(notif_fd, O_RDONLY) < 0)
    {
      write(resp_pipe_path, opcode + "1", 2);
      perror("Failed to open notification pipe");
      return 1;
    }
    // Connection successful message
    write(resp_pipe_path, opcode + "0", 2);
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
      num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
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
      num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

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
      num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

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