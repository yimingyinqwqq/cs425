#include "node.h"

#define MAX_CLIENTS 50

// error - wrapper for perror
void error(const char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(1);
}

string currentTimeString() {
  std::time_t now = std::time(NULL);
  std::tm * ptm = std::localtime(&now);
  char buffer[32] = {0};
  std::strftime(buffer, 32, "%Y/%m/%d %H:%M:%S", ptm); // %a 
  return string(buffer);
}

time_t stringToTime(string t) {
  struct tm tm;
  strptime(t.c_str(), "%Y/%m/%d %H:%M:%S", &tm);
  time_t res = mktime(&tm);
  return res;
}

// create UPD server, return socket fd
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

// create TCP server, return socket fd
int TCP_server(const char *port) {
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

  if (listen(sock_fd, MAX_CLIENTS) != 0) {
    perror(NULL); exit(1);
  }

  freeaddrinfo(res);
  return sock_fd;
}

// setup UDP client, return sockfd
int UDP_client() {
  return socket(AF_INET, SOCK_DGRAM, 0);
}

// TCP connect client to `host` on `port`, return sockfd
int TCP_connect(const char* host, int port) {
  struct sockaddr_in servaddr, cli;

  // socket create and verification
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1)
    return -1;
  
  bzero(&servaddr, sizeof(servaddr));

  // assign IP, PORT
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = inet_addr(host);
  servaddr.sin_port = htons(port);

  // connect the client socket to server socket
  if (connect(sockfd, (sockaddr*) &servaddr, sizeof(servaddr)) != 0) {
    close(sockfd);
    return -1;
  }

  return sockfd;
}

// send UDP `msg` to `ip` on `port`
void UDP_send(int sockfd, int port, const char* ip, const char* msg) {
  struct sockaddr_in s;

  memset(&s, '\0', sizeof(s));
  s.sin_family = AF_INET;
  s.sin_port = htons(port);
  // s.sin_addr.s_addr = htonl(INADDR_BROADCAST);
  s.sin_addr.s_addr = inet_addr(ip);
  
  sendto(sockfd, msg, strlen(msg), 0, (struct sockaddr *)&s, sizeof(s));
}

// TCP send all content in file to destfd
// caller should take care of closing file and shutdown write to destfd
void sendFileContent(FILE* file, int destfd) {
  char buf[1024];
  int ret;
  //read(fileno(file), buf, 1024)
  while ((ret = fread(buf, 1, 1024, file)) > 0) {
    send(destfd, buf, ret, 0);
  }
}

// TCP receive all content from destfd to file
// caller should take care of closing file and shutdown read to destfd
void receiveFileContent(FILE* file, int destfd) {
  char buf[1024];
  int ret;
  while ((ret = recv(destfd, buf, 1024, 0)) > 0) {
    // write(fileno(file), buf, ret);
    fwrite(buf, 1, ret, file);
  }
}