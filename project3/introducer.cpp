#include "node.h"

#define BUFFER_SIZE 50

void* introducerDriver(void*) {
  int sockfd = UDP_server(INTRODUCER_PORT);

  while (1) {
    fprintf(stderr, "waiting for new process to join...\n");

    // listen for DNS 
    struct sockaddr_in clientaddr;
    socklen_t clientlen = sizeof(clientaddr);

    char buffer[BUFFER_SIZE] = {0};
    recvfrom(sockfd, buffer, BUFFER_SIZE, 0, (struct sockaddr *) &clientaddr, &clientlen);

    if (string(buffer) == "HELLO") {
      // response with here when hear from DNS
      const char* here = "HERE";
      sendto(sockfd, here, strlen(here), 0, (struct sockaddr *) &clientaddr, clientlen);
    } else {  
      // new process join with master ip message
      char clientIp[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &clientaddr.sin_addr, clientIp, INET_ADDRSTRLEN);

      fprintf(stderr, "client %s join with master IP: %s\n", clientIp, buffer); // master ip should just be the introducer Ip

      // send UDP JOIN message to the machine introducer is on
      struct sockaddr_in servaddr;
      memset(&servaddr, 0, sizeof(servaddr));
      servaddr.sin_family = AF_INET;
      servaddr.sin_port = htons(std::stoi(COMMUNICATION_PORT));
      servaddr.sin_addr.s_addr = inet_addr(buffer);

      // JOIN\n<IP>\n<timestamp>\n
      string join_broadcast = "JOIN\n" + string(clientIp) + "\n" + currentTimeString() + "\n";
      sendto(sockfd, join_broadcast.c_str(), join_broadcast.size(), 0, (const struct sockaddr *) &servaddr, sizeof(servaddr));
    }
  }
}
