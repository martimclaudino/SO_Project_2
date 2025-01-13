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
                int *notif_fd_main, int *req_fd_main, int *resp_fd_main)
{
  // create pipes and connect
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  if (mkfifo(req_pipe_path, 0777) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0777) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0777) < 0)
  {
    perror("Failed to create named pipe");
    return 1;
  }

  int server_fd;
  if ((server_fd = open(server_pipe_path, O_WRONLY)) < 0)
  {
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

  printf("req: %s\n", req_pipe_path);
  printf("resp: %s\n", resp_pipe_path);
  printf("notif: %s\n", notif_pipe_path);
  printf("msg: %s\n", msg);

  // Send the message to the server
  if (write_all(server_fd, msg, 121) == -1)
  {
    fprintf(stderr, "Failed to send message to server\n");
    return 1;
  }

  // Opening the pipes
  if ((resp_fd = open(resp_pipe_path, O_RDONLY)) < 0)
  {
    perror("Failed to open response pipe");
    return 1;
  }

  if ((notif_fd = open(notif_pipe_path, O_RDONLY)) < 0)
  {
    perror("Failed to open notification pipe");
    return 1;
  }

  if ((req_fd = open(req_pipe_path, O_WRONLY)) < 0)
  {
    perror("Failed to open request pipe");
    return 1;
  }

  *notif_fd_main = notif_fd;
  *req_fd_main = req_fd;
  *resp_fd_main = resp_fd;
  printf("opened pipes\n");
  char server_connection_result[3];

  read_all(resp_fd, server_connection_result, 3, NULL);

  char message[43];
  snprintf(message, sizeof(message),
           "Server returned %s for operation: connect\n",
           server_connection_result);
  if (write_all(1, message, 43) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (server_connection_result[1] == '1')
  { // Failed to connect
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return 1;
  }
  close(server_fd);
  return 0;
}

int kvs_disconnect()
{
  // close pipes and unlink pipe files
  if (write_all(req_fd, "2", 1) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  char response[3];
  if (read_all(resp_fd, response, 3, NULL) == -1)
  {
    perror("Failed to read from response pipe");
    return 1;
  }

  // printf("Server returned %c for operation: disconnect\n", response[1]); N
  // SEI QUAL MANEIRA E PARA FAZER, COPILOT DISSE A 1º
  char message[46];
  snprintf(message, sizeof(message),
           "Server returned %s for operation: disconnect\n", response);
  if (write_all(1, message, 46) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  close(req_fd);
  close(resp_fd);
  close(notif_fd);

  return 0;
}

int kvs_subscribe(const char *key)
{
  // send subscribe message to request pipe and wait for response in response
  char subscrition[41] = {0};
  char key2[40] = {0};
  memcpy(key2, key, 40);
  memcpy(subscrition, "3", 1);
  memcpy(subscrition + 1, key2, 40);

  printf("subscrition: %s\n", subscrition);
  if (write_all(req_fd, subscrition, 41) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  // Wait for response
  char server_subscription_result[3];
  if (read_all(resp_fd, server_subscription_result, 3, NULL) == -1)
  {
    perror("Failed to read from response pipe");
    return 1;
  }

  char message[45];
  snprintf(message, sizeof(message),
           "Server returned %s for operation: subscribe\n",
           server_subscription_result);
  if (write_all(1, message, 44) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (server_subscription_result[1] == '0')
  {
    return 1;
  }
  return 0;
}

int kvs_unsubscribe(const char *key)
{
  // send unsubscribe message to request pipe and wait for response in response
  printf("key to unsub: %s\n", key);
  char unsubscrition[41] = {0};
  char key2[40] = {0};
  memcpy(key2, key, 40);
  memcpy(unsubscrition, "4", 1);
  memcpy(unsubscrition + 1, key, 40);
  printf("key que é passada: %s\n", unsubscrition);

  if (write_all(req_fd, unsubscrition, 41) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  char server_unsubscription_result[3];
  if (read_all(resp_fd, server_unsubscription_result, 3, NULL) == -1)
  {
    perror("Failed to read from response pipe");
    return 1;
  }
  for (int i = 0; i < 3; i++)
  {
    if (server_unsubscription_result[i] == '\0')
      printf("\\0");
    else
      printf("%c", server_unsubscription_result[i]);
  }
  printf("\n");

  char message[47];
  snprintf(message, sizeof(message),
           "Server returned %s for operation: unsubscribe\n",
           server_unsubscription_result);
  if (write_all(1, message, 46) == -1)
  {
    perror("Failed to write to response pipe");
    return 1;
  }
  if (server_unsubscription_result[1] == '1')
  {
    return 1;
  }
  // pipe
  return 0;
}
