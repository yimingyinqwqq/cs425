/* 
** coordinator.cpp -- code to run on the coordinator machine
*/

#include "common.cpp"
#include <string>
#include <vector>
#include <utility>

using std::string;
using std::vector;
using std::pair;

#define COORDINATOR_PORT "8000"
#define SERVER_PORT "8200"
#define MAX_CLIENTS 50
#define HOST_FILE_VECTOR vector<pair<string, string>>({ \
  pair<string, string>("172.22.94.58", "vm1.log"), \
  pair<string, string>("172.22.156.59", "vm2.log"), \
  pair<string, string>("172.22.158.59", "vm3.log"), \
  pair<string, string>("172.22.94.59", "vm4.log"), \
  pair<string, string>("172.22.156.60", "vm5.log"), \
  pair<string, string>("172.22.158.60", "vm6.log"), \
  pair<string, string>("172.22.94.60", "vm7.log"), \
  pair<string, string>("172.22.156.61", "vm8.log"), \
  pair<string, string>("172.22.158.61", "vm9.log"), \
  pair<string, string>("172.22.94.61", "vm10.log") \
}) // TODO: CHANGE THIS TO YOUR VM ADDRESSES!

// we define the first line from the server to be line count + \n
// this function return { line_count, string length }
// assume str valid
pair<int, int> parse_first_line(char* str) {
  char* ptr = str;
  int len = 0;
  while (*ptr != '\n') {
    ++ptr;
    ++len;
  }
  *ptr = '\0';
  pair<int, int> res = {atoi(str), len};
  *ptr = '\n';
  return res;
}

// expecting "grep [OPTIONS] PATTERN" from client
int main(int argc, char *argv[])
{
	if (argc != 1) {
    fprintf(stderr, "usage: ./coordinator\n");
    exit(1);
	}

  int coordinatorSocket = setup_server(COORDINATOR_PORT, MAX_CLIENTS);

  while (1) {
    struct sockaddr_storage clientaddr;
    socklen_t clientaddrsize = sizeof(clientaddr);
    int client_fd = accept(coordinatorSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    // listen to client request
    char request[4096] = {0};
    read_all_from_socket(client_fd, request, 4096);  // assume 4096 big enough
    shutdown(client_fd, SHUT_RD);

    // for every request connection, query all server VMs
    int total_lines = 0;
    for (const pair<string, string>& p : HOST_FILE_VECTOR) {
      // connect to server
      int serverfd = connect_to_host(p.first.c_str(), SERVER_PORT);

      if (serverfd == -1) {
        string message = "Failed to connect to server " + p.first + "\n";
        write_all_to_socket(client_fd, message.c_str(), message.size());
        continue;
      }

      fprintf(stderr, "connecting to %s\n", p.first.c_str());

      // add source file option to the grep command before sending to server
      string request_with_line_num = string(request).substr(0, 5) + "-H " + (request + 5);
      string cmd = request_with_line_num + " " + p.second;

      // send request to server
      if (write_all_to_socket(serverfd, cmd.c_str(), cmd.size()) == -1) {
        string message = "Failed to send message to server " + p.first + "\n";
        write_all_to_socket(client_fd, message.c_str(), message.size());
        shutdown(serverfd, SHUT_RDWR);
        close(serverfd);
        continue;
      }

      shutdown(serverfd, SHUT_WR);

      // read from server
      char response[4096] = {0};
      ssize_t read_ret;
      ssize_t total_read = 0, total_send = 0;
      int is_first_line = 1;
      while ((read_ret = read_all_from_socket(serverfd, response, 4096)) != 0) {
        if (read_ret == -1) {
          const char* message = "Incomplete response from server\n";
          write_all_to_socket(client_fd, message, strlen(message));
          break;
        }
        // send back to client
        if (is_first_line) {
          pair<int, int> parsed_result = parse_first_line(response);
          total_lines += parsed_result.first;
          is_first_line = 0;
          total_send += write_all_to_socket(
            client_fd, response + parsed_result.second + 1, read_ret - parsed_result.second - 1
          );
        } else {
          total_send += write_all_to_socket(client_fd, response, read_ret);
        }
        total_read += read_ret;
      }

      fprintf(stderr, "read %zd bytes from server\n", total_read);
      fprintf(stderr, "sent %zd bytes to client\n", total_send);

      shutdown(serverfd, SHUT_RD);
      close(serverfd);
    }

    string msg = "Total line count: " + std::to_string(total_lines) + "\n";
    write_all_to_socket(client_fd, msg.c_str(), msg.size());

    // finish all querying for one client, clean up
    shutdown(client_fd, SHUT_WR);
    close(client_fd);
  }

	return 0;
}

