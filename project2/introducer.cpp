#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include <chrono>
#include <ctime>

#include "common.cpp"

#define INTRODUCER_PORT "8888"
#define COMMUNICATION_PORT 8080
#define INTRODUCER_IP "172.22.94.58" // TODO: CHANGE THIS TO YOUR VM ADDRESS
#define MAX_CLIENTS 50

using std::chrono::system_clock;
using std::string;

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}

	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


int main(int argc, char **argv) {
  if (argc != 1) {
    fprintf(stderr, "Usage: ./introducer");
    exit(1);
  }

  int introducer_socket = setup_server(INTRODUCER_PORT, MAX_CLIENTS); // TCP connection
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);  // UPD introducer connection

  while (1) {
    fprintf(stderr, "waiting for new process to join...\n");

    struct sockaddr_storage clientaddr;
    socklen_t clientaddrsize = sizeof(clientaddr);
    int client_fd = accept(introducer_socket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    char s[INET_ADDRSTRLEN] = {0};
    inet_ntop(clientaddr.ss_family, get_in_addr((struct sockaddr *) &clientaddr), s, sizeof(s));
    fprintf(stderr, "introducer: got connection from %s\n", s);

    // get timestamp where the process has joined
    std::time_t t = system_clock::to_time_t(system_clock::now());
    string time_str = std::ctime(&t); // this includes a '\n' at the end
    time_str.pop_back();
    // std::cerr << time_str << std::endl;

    // send the ip address of new process and current time to process 1
    string message = s + string("\n") + time_str;
    send(client_fd, message.c_str(), message.size(), 0);

    // wait for confirmation from the new process
    char buf[10] = {0};
    recv(client_fd, buf, 10, 0);
    fprintf(stderr, "client confirmation: %s\n", buf);

    close(client_fd);

    // send UDP broadcast of JOIN message to the machine introducer is on
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(COMMUNICATION_PORT);
    servaddr.sin_addr.s_addr = inet_addr(INTRODUCER_IP);

    string join_broadcast = "JOIN\n" + message + "\n";
    sendto(sockfd, join_broadcast.c_str(), join_broadcast.size(), 0, (const struct sockaddr *) &servaddr, sizeof(servaddr));
  }

  return 0;
}
