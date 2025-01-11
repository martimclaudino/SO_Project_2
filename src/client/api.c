#include "api.h"

#include <fcntl.h>
#include <src/common/io.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"

int req_fd, resp_fd, notif_fd;

int kvs_connect(char *req_pipe_path, char *resp_pipe_path,
                char *server_pipe_path, char *notif_pipe_path,
                int *notif_fd_main, int *req_fd_main, int *resp_fd_main) {
  printf("entrou no kvs_connect\n");
  // create pipes and connect
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  if (mkfifo(req_pipe_path, 0777) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0777) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0777) < 0) {
    perror("Failed to create named pipe");
    return 1;
  }
  printf("abriu pipes\n");
  int server_fd;
  if ((server_fd = open(server_pipe_path, O_WRONLY)) < 0) {
    perror("Failed to open named pipe");
    return 1;
  }
  printf("abriu server_fd\n");

  char msg[121] = {"\0"};

  // Concatenate the paths into msg

  memcpy(msg, "1", 1);
  memcpy(msg + 1, req_pipe_path, 40);
  memcpy(msg + 41, resp_pipe_path, 40);
  memcpy(msg + 81, notif_pipe_path, 40);

  // Send the message to the server
  if (write_all(server_fd, msg, 121) == -1) {
    fprintf(stderr, "Failed to send message to server\n");
    return 1;
  }
  printf("depois de mandar a mensagem para o server\n");

  // Opening the pipes
  if ((resp_fd = open(resp_pipe_path, O_RDONLY)) < 0) {
    printf("falhou a abrir o resp\n");
    perror("Failed to open response pipe");
    return 1;
  }
  printf("resp_fd: %d\n", resp_fd);

  if ((notif_fd = open(notif_pipe_path, O_RDONLY)) < 0) {
    printf("falhou a abrir o notif");
    perror("Failed to open notification pipe");
    return 1;
  }
  printf("notif_fd: %d\n", notif_fd);

  if ((req_fd = open(req_pipe_path, O_WRONLY)) < 0) {
    printf("falhou a abrir o req\n");
    perror("Failed to open request pipe");
    return 1;
  }
  printf("req_fd: %d\n", req_fd);

  *notif_fd_main = notif_fd;
  *req_fd_main = req_fd;
  *resp_fd_main = resp_fd;
  printf("antes de mandar a mensagem para o server\n");

  char server_connection_result[3];
  printf("Antes do Server\n");
  read_all(resp_fd, server_connection_result, 3, NULL);
  /*   write(1, "Server returned %d for operation: connect\n",
          server_connection_result[1], 41); */
  printf("Depois do Server\n");
  char message[43];
  printf("server_connection_result: %s\n", server_connection_result);
  snprintf(message, sizeof(message),
           "Server returned %s for operation: connect\n",
           server_connection_result);
  if (write_all(1, message, 43) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (server_connection_result[1] == '1') {
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return 1;
  }
  close(server_fd);
  return 0;
}

int kvs_disconnect(/*int req_fd_dis, int resp_fd_dis, int notif_fd_dis, char
                    *req_pipe_path, char *resp_pipe_path, char
                    **notif_pipe_path*/
) {
  // close pipes and unlink pipe files
  if (write_all(req_fd, "2", 2) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: disconnect\n", response[1]); N
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  char message[45];
  snprintf(message, sizeof(message),
           "Server returned %d for operation: disconnect\n", response[1]);
  if (write_all(1, message, 45) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  close(req_fd);
  close(resp_fd);
  close(notif_fd);

  return 0;
}

// POSSIVEL ERRO AQUI nos nao estamos a fazer if quando escrevemos server
// returned..

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  char connection_output[3];
  connection_output[0] = '3';
  connection_output[1] = *key;
  connection_output[2] = '\0';
  if (write_all(req_fd, connection_output, strlen(key) + 1) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: subscribe\n", response[1]); NAO
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  char message[44];
  snprintf(message, sizeof(message),
           "Server returned %d for operation: subscribe\n", response[1]);
  if (write_all(1, message, 44) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (response[1] == '1') {
    return 1;
  }

  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  if (write_all(req_fd, "4" + *key, strlen(key) + 1) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[2];
  read_all(resp_fd, response, 2, NULL);
  // printf("Server returned %c for operation: unsubscribe\n", response[1]); N
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  char message[46];
  snprintf(message, sizeof(message),
           "Server returned %d for operation: unsubscribe\n", response[1]);
  if (write_all(1, message, 46) == -1) {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (response[1] == '1') {
    return 1;
  }
  // pipe
  return 0;
}
