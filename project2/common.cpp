/* 
** common.cpp -- code to share on all machines
*/

#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/file.h>
#include <time.h>


// error - wrapper for perror
void error(const char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(1);
}

int UDP_server(const char* port) {
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);  // UDP, IPv4
  if (sockfd < 0) 
    error("ERROR opening socket");

  // lets us rerun the server immediately after we kill it
  int optval = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval , sizeof(int));

  // bind server
  struct sockaddr_in serveraddr;
  memset((char *) &serveraddr, 0, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)atoi(port));

  if (bind(sockfd, (struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0) 
    error("ERROR on binding");

  return sockfd;
}


// connect to a server on port
// return socket file discriptor
int setup_server(const char *port, int max_clients) {
  // connect server
  int opt = 1;
  int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  
  setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int));
  setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(int));

  struct addrinfo hints;
  struct addrinfo* res;
  memset(&hints, 0, sizeof(hints));

  hints.ai_family = AF_INET;          // IPv4
  hints.ai_socktype = SOCK_STREAM;    // TCP
  hints.ai_flags = AI_PASSIVE;

  int s = getaddrinfo(NULL, port, &hints, &res);
  if (s != 0) {
    fprintf(stderr, "%s\n", gai_strerror(s)); exit(1);
  }

  if (bind(sock_fd, res->ai_addr, res->ai_addrlen) != 0) {
    perror(NULL); exit(1);
  }

  if (listen(sock_fd, max_clients) != 0) {
    perror(NULL); exit(1);
  }

  freeaddrinfo(res);
  return sock_fd;
}

// this function can be used to connect to the server VMs or the coordinator VM
// return sockfd of the host or -1 on error
int connect_to_host(const char* host, const char* port) {
  int sockfd;  
	struct addrinfo hints, *servinfo, *p;
	int rv;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;        // IPv4
	hints.ai_socktype = SOCK_STREAM;  // TCP

	if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0) {
		// fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return -1;
	}

	// loop through all the results and connect to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
			continue;
		}
		if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			continue;
		}
		break;
	}

	if (p == NULL) {
		// fprintf(stderr, "failed to connect\n");
		return -1;
	}

	freeaddrinfo(servinfo);

  return sockfd;
}

// read from socket to buffer of count bytes, assume buffer big enough
// return number of bytes read, -1 on error
ssize_t read_all_from_socket(int socket, char* buffer, ssize_t count) {
  ssize_t offset = 0;
  ssize_t len = count;    // length of bytes to send

  while (len > 0) {
    ssize_t already_read = read(socket, buffer + offset, len);

    if (already_read == -1 && errno == EINTR)
      continue; 
    if (already_read < 0)
      return -1;
    if (already_read == 0)  // no more to read
      return count - len;

    len -= already_read;
    offset += already_read;
  }

  return count;
}

// write to socket from buffer of count bytes, assume buffer big enough
// return number of bytes write, -1 on error
ssize_t write_all_to_socket(int socket, const char *buffer, ssize_t count) {
  ssize_t offset = 0;
  ssize_t len = count;    // length of bytes to send

  while (len > 0) {
    ssize_t sent = write(socket, buffer + offset, len);

    if (sent == -1 && errno == EINTR)
      continue;
    if (sent < 0)
      return -1;
    if (sent == 0)  // no more to read
      return count - len;

    len -= sent;
    offset += sent;
  }

  return count;
}
