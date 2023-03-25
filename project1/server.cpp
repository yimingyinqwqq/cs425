/* 
** server.cpp -- code to run on logging machines
*/

#include "common.cpp"
#include <string>

using std::string;

#define MAX_CLIENTS 50
#define SERVER_PORT "8200"


int file_line_count(FILE* f) {
  int lines = 0;
  while(!feof(f)) {
    int ch = fgetc(f);
    if(ch == '\n') {
      lines++;
    }
  }
  rewind(f);
  return lines;
}

// expecting "grep [OPTIONS] PATTERN FILENAME" from coordinator, FILENAME not optional
// run grep command on the log file on the local machine
// write the result back to the client
void* handle_client(void *fd_ptr) {
  pthread_detach( pthread_self() ); // pthread_join not needed
  int client_fd = *(int *) fd_ptr;
  free(fd_ptr); fd_ptr = NULL;

  char cmd[4096] = {0};
  read_all_from_socket(client_fd, cmd, 4096);   // FIXME: assume that 4096 is big enough

  fprintf(stderr, "executing command [ %s ]...\n", cmd);

  shutdown(client_fd, SHUT_RD);

  // run grep on the log file and output to `temp_output`
  int fd_copy = dup(1);                   // copy stdout
  close(1);                               // close stdout
  FILE* f = fopen("temp_output", "w+");   // open a temporary file to save output

  // execute command along with line count for the match
  system(cmd);

  rewind(f);

  string lc = std::to_string(file_line_count(f));

  // use the first line to tell coordinator the line count
  string msg1 = lc + "\n";
  write_all_to_socket(client_fd, msg1.c_str(), msg1.size());

  // send the output to client
  char buffer[4096] = {0};
  ssize_t count;
  ssize_t all_count = 0;
  while ((count = read(fileno(f), buffer, 4096)) != 0) {
    all_count += write_all_to_socket(client_fd, buffer, count);
  }

  string msg2 = "File line count: " + lc + "\n";
  write_all_to_socket(client_fd, msg2.c_str(), msg2.size());

  shutdown(client_fd, SHUT_WR);
  fclose(f);
  dup2(fd_copy, 1);       // copy stdout back
  unlink("temp_output");
  
  close(client_fd);

  fprintf(stderr, "Sent %zd bytes to the coordinator\n", all_count + msg1.size() + msg2.size());

  return NULL;
}


// run on port SERVER_PORT, listening to the coordinator
// expecting "grep [OPTIONS] PATTERN FILENAME" from the coordinator
// response with exactly the grep output string
int main(int argc, char **argv) {
  if (argc != 1) {
    fprintf(stderr, "usage: ./server\n");
    exit(1);
  }

  int serverSocket = setup_server(SERVER_PORT, MAX_CLIENTS);

  while (1) {
    struct sockaddr_storage clientaddr;
    socklen_t clientaddrsize = sizeof(clientaddr);
    int client_fd = accept(serverSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    int* fd = (int*) malloc(sizeof(int));
    *fd = client_fd;
    pthread_t tid;
    pthread_create(&tid, NULL, handle_client, fd);
  }
  
  return 0;
}
