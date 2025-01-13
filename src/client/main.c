#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <threads.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

typedef struct
{
  char *notif_path;
} NotifPipe;

int req_fd = -1, resp_fd = -1, pipe_control = 0;
char req_pipe_path_main[40] = {0};
char resp_pipe_path_main[40] = {0};
char notif_pipe_path_main[40] = {0};

void *notif_func(void *fd)
{
  int notif_fd = *(int *)fd;
  if (notif_fd < 0)
  {
    perror("Failed to open notification pipe");
    return NULL;
  }

  char buffer[81];
  while (1)
  {
    char opcode;
    if (read_all(notif_fd, &opcode, 1, NULL) == -1)
    {
      fprintf(stderr, "Failed to read message from client\n");
      return NULL;
    }

    if (opcode == '0')
    {
      if (pipe_control == 0)
      {
        // forced disconnect with SIGUSR1
        close(req_fd);
        close(resp_fd);
        close(notif_fd);
        pipe_control = -1;
      }
      pthread_exit(NULL);
      return NULL;
    }

    char key[41];
    char value[41];
    if (read_all(notif_fd, buffer, 80, NULL) == -1)
    {
      fprintf(stderr, "Failed to read message from client\n");
      return NULL;
    }

    buffer[80] = '\0';
    strncpy(key, buffer, 40);
    key[40] = '\0';
    strncpy(value, buffer + 40, 40);
    value[40] = '\0';
    char message[85];
    snprintf(message, 85, "(%s,%s)\n", key, value);
    if (write_all(1, message, 85) == -1)
    {
      perror("Failed to write to response pipe");
      return NULL;
    }
  }
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }
  char req_pipe_path[40] = {0};
  char resp_pipe_path[40] = {0};
  char notif_pipe_path[40] = {0};
  strcpy(req_pipe_path, "/tmp/req");
  strcpy(resp_pipe_path, "/tmp/resp");
  strcpy(notif_pipe_path, "/tmp/notif");

  strcpy(req_pipe_path_main, req_pipe_path);
  strcpy(resp_pipe_path_main, resp_pipe_path);
  strcpy(notif_pipe_path_main, notif_pipe_path);

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  char *server_pipe_path = argv[2];

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  int notif_fd = -1;

  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path,
                  notif_pipe_path, &notif_fd, &req_fd, &resp_fd))
  {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, notif_func, (void *)&notif_fd) != 0)
  {
    fprintf(stderr, "Failed to create notification thread\n");
    return 1;
  }

  while (1)
  {
    if (pipe_control == -1)
    {
      break;
    }
    switch (get_next(STDIN_FILENO, &pipe_control))
    {
    case CMD_DISCONNECT:
      pipe_control = 1;
      if (kvs_disconnect() != 0)
      {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }

      unlink(req_pipe_path);
      unlink(resp_pipe_path);
      unlink(notif_pipe_path);
      // TODO: end notifications thread
      pthread_join(notif_thread, NULL);
      printf("Disconnected from server\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0]))
      {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      kvs_unsubscribe(keys[0]);

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0)
      {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      break;

    case FORCED_TO_DISCONNECT:
      pthread_join(notif_thread, NULL);
      printf("Disconnected from server\n");
      unlink(req_pipe_path);
      unlink(resp_pipe_path);
      unlink(notif_pipe_path);
      return 0;
    }
  }
}
