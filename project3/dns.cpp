#include "common.cpp"

#define INTRODUCER_PORT "8888"
#define DNS_PORT "7777"
#define WAIT_SEC 0
#define WAIT_MS 100   // timeout for finding master server
#define BUFSIZE 50

static set<string> processesSeen;  // list of ips of all proccesses that has joined
static int dnsSocket;


// return master ip as string if found, "None" otherwise
string findMaster() {
  struct timeval tv;
  tv.tv_sec = WAIT_SEC;
  tv.tv_usec = WAIT_MS;
  setsockopt(dnsSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  string masterIp = "NONE";

  struct sockaddr_in masterAddr;
  socklen_t masterLen = sizeof(masterAddr);

  // multicast to all processes seen to find master, only master will response
  for (string ip : processesSeen) {
    UDP_send(dnsSocket, std::stoi(INTRODUCER_PORT), ip.c_str(), "HELLO");

    // format: [ip]
    char myIpBuf[INET_ADDRSTRLEN] = {0};    // not used
    if (recvfrom(dnsSocket, myIpBuf, INET_ADDRSTRLEN, 0, (struct sockaddr *) &masterAddr, &masterLen) != -1) {
      char masterIpBuf[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &masterAddr.sin_addr, masterIpBuf, INET_ADDRSTRLEN);
      masterIp = string(masterIpBuf);
      break;
    }
  }

  // reset timeout
  tv.tv_sec = 0;
  tv.tv_usec = 0;
  setsockopt(dnsSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  return masterIp;
}

int main(int argc, char **argv) {
  if (argc != 1)
    error("Usage: ./dns");

  dnsSocket = UDP_server(DNS_PORT);

  while (1) {
    struct sockaddr_in clientaddr;
    unsigned int clientlen = sizeof(clientaddr);

    // listen for "WhoIsMaster" message from new process
    char whoIsMaster[BUFSIZE] = {0};
    recvfrom(dnsSocket, whoIsMaster, BUFSIZE, 0, (struct sockaddr *) &clientaddr, &clientlen);

    char processIp[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &clientaddr.sin_addr, processIp, INET_ADDRSTRLEN);
    processesSeen.insert(string(processIp));

    string masterIp = findMaster();
    cout << "DNS found master result: " << masterIp << endl;
    sendto(dnsSocket, masterIp.c_str(), masterIp.size(), 0, (struct sockaddr *)&clientaddr, sizeof(clientaddr));
  }
  
  return 0;
}
