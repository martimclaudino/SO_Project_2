#include "api.h"

#include <fcntl.h>
#include <stdio.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
int req_fd, resp_fd, notif_fd;
int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
  // create pipes and connect
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  if (mkfifo(req_pipe_path, 0666) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  int server_fd;
  if (server_fd = open(server_pipe_path, O_RDONLY) < 0) {
    perror("Failed to open named pipe");
    return 1;
  }

  char msg[121];
  memset(req_pipe_path + strlen(req_pipe_path), '\0',
         sizeof(req_pipe_path) - strlen(req_pipe_path));
  memset(resp_pipe_path + strlen(resp_pipe_path), '\0',
         sizeof(resp_pipe_path) - strlen(resp_pipe_path));
  memset(notif_pipe_path + strlen(notif_pipe_path), '\0',
         sizeof(notif_pipe_path) - strlen(notif_pipe_path));
  // Concatenate the paths into msg
  snprintf(msg, sizeof(msg), "1%s%s%s", req_pipe_path, resp_pipe_path,
           notif_pipe_path);

  // Opening the pipes

  if (req_fd = open(req_pipe_path, O_WRONLY) < 0) {
    perror("Failed to open request pipe");
    return 1;
  }
  if (resp_fd = open(resp_pipe_path, O_RDONLY) < 0) {
    perror("Failed to open response pipe");
    return 1;
  }
  if (notif_fd = open(notif_pipe_path, O_RDONLY) < 0) {
    perror("Failed to open notification pipe");
    return 1;
  }

  // Send the message to the server
  if (write_all(server_fd, msg, sizeof(msg)) == -1) {
    fprintf(stderr, "Failed to send message to server\n");
    return 1;
  }

  char server_connection_result[2];

  read_all(resp_fd, server_connection_result, 2, NULL);
  write(1, "Server returned %d for operation: connect\n",
        server_connection_result[1], 41);
  if (server_connection_result[1] == '1') {
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return 1;
  }

  return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  if (write(req_fd, "2", 2) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: disconnect\n", response[1]); N
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  write(1, "Server returned %d for operation: disconnect\n", response[1], 44);
  return 0;
}

// POSSIVEL ERRO AQUI nos nao estamos a fazer if quando escrevemos server
// returned..

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  if (write(req_fd, "3" + *key, strlen(key) + 1) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: subscribe\n", response[1]); NAO
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  write(1, "Server returned %d for operation: subscribe\n", response[1], 43);
  if (response[1] == '1') {
    return 1;
  }
  return 0;

  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  if (write(req_fd, "4" + *key, strlen(key) + 1) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: unsubscribe\n", response[1]); N
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  write(1, "Server returned %d for operation: unsubscribe\n", response[1], 45);
  if (response[1] == '1') {
    return 1;
  }
  return 0;
  // pipe
  return 0;
}
