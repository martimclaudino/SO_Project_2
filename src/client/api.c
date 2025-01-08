#include "api.h"
#include <fcntl.h>
#include <stdio.h>
#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe)
{
  // create pipes and connect
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  if (mkfifo(req_pipe_path, 0666) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }
  int server_fd;
  if (server_fd = open(server_pipe_path, O_RDONLY) < 0)
  {
    perror("Failed to open named pipe");
    return 1;
  }

  char msg[121];
  memset(req_pipe_path + strlen(req_pipe_path), '\0', sizeof(req_pipe_path) - strlen(req_pipe_path));
  memset(resp_pipe_path + strlen(resp_pipe_path), '\0', sizeof(resp_pipe_path) - strlen(resp_pipe_path));
  memset(notif_pipe_path + strlen(notif_pipe_path), '\0', sizeof(notif_pipe_path) - strlen(notif_pipe_path));
  // Concatenate the paths into msg
  snprintf(msg, sizeof(msg), "1%s%s%s", req_pipe_path, resp_pipe_path, notif_pipe_path);

  // Opening the pipes
  int req_fd, resp_fd, notif_fd;
  if (req_fd = open(req_pipe_path, O_WRONLY) < 0)
  {
    perror("Failed to open request pipe");
    return 1;
  }
  if (resp_fd = open(resp_pipe_path, O_RDONLY) < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }
  if (notif_fd = open(notif_pipe_path, O_RDONLY) < 0)
  {
    perror("Failed to open notification pipe");
    return 1;
  }

  // Send the message to the server
  if (write_all(server_fd, msg, sizeof(msg)) == -1)
  {
    fprintf(stderr, "Failed to send message to server\n");
    return 1;
  }

  char server_connection_result[2];
  read_all(resp_fd, server_connection_result, 2, NULL);
  if (server_connection_result[1] == '1')
  {
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return 1;
  }
  else
  {
    write(1, "Server returned 1 for operation: connect\n", 41);
  }
  return 0;
}

int kvs_disconnect(void)
{
  // close pipes and unlink pipe files

  return 0;
}

int kvs_subscribe(const char *key)
{
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key)
{
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}
