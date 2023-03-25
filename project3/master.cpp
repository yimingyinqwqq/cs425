#include "node.h"
#include <functional>

#define BUFFER_SIZE 1024
#define VM_COUNT 10

extern map<string, set<string>> replica_map;        // map of filename to set of machines' ip addresses
extern vector<tuple<string, string, actions>> membership_list;    // list of <ip, timestamp, actions>
extern pthread_mutex_t list_lock;

static int currentRequestId = 0;
int masterSocket = -1;

int numberOfAliveMachines() {
  pthread_mutex_lock(&list_lock);
  int res = 0;
  for (auto tup : membership_list) {
    if (std::get<2>(tup) == JOIN)
      ++res;
  }
  pthread_mutex_unlock(&list_lock);
  return res;
}

void setSocketToNonBlocking(int sockfd, long sec, long ms) {
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = ms;
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

void setSocketToBlocking(int sockfd) {
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 0;
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

// listen to host in `currHostsSet`, wait a subset of response
// then move on and close all opened servers connections
void waitWriteAck(int masterSocket, int aliveMachineCount, const set<int>& currHostsSet) {
  int writeAckCount = 0;
  // // temporarily set to non-blocking mode
  // setSocketToNonBlocking(masterSocket, 1, 0);
  while (writeAckCount < std::min(W, aliveMachineCount)) {
    for (int hostfd : currHostsSet) {
      char responseBuf[BUFFER_SIZE] = {0};
      int ret = recv(hostfd, responseBuf, BUFFER_SIZE, 0);
      if (ret > 0) {
        // should check requestId
        close(hostfd);
        ++writeAckCount;
        if (writeAckCount >= std::min(W, aliveMachineCount))
          break;
      }
    }
  }
  cout << "all acks back" << endl;
  // // reset back to blocking mode
  // setSocketToBlocking(masterSocket);
  // don't need to wait for the rest of ack
  for (int hostfd : currHostsSet)
    close(hostfd);
}


// find list of ip that the file should store in based on filename
vector<string> findReplicaTargets(string filename) {
  vector<string> res;
  int stored_idx = std::hash<string>{}(filename) % VM_COUNT;
  pthread_mutex_lock(&list_lock);
  int startIdx = stored_idx < membership_list.size() ? stored_idx : 0;
  for (int i = 0; i < std::min(REPLICA_COUNT, (int) membership_list.size()); i++) {
    res.push_back(std::get<0>(membership_list[(startIdx + i) % membership_list.size()]));
  }
  pthread_mutex_unlock(&list_lock);
  return res;
}

// peek to see which machine has the most recent file version, return its ip
string peekFileVersion(string sdfsfilename, int masterSocket, int aliveMachineCount) {
  map<int, string> fdHostMap;  // mapping hostfd to hostIp

  // send PEEK request to all replicates
  for (string hostIp : replica_map[sdfsfilename]) {
    // connect to the given ip adress using node port
    int hostfd = TCP_connect(hostIp.c_str(), std::stoi(SDFS_NODE_PORT));
    fdHostMap.insert({hostfd, hostIp});

    // send the request in the format:
    // [requestId]\n
    // PEEK\n
    // [remote file name]
    string msg = std::to_string(currentRequestId) + "\nPEEK\n" + sdfsfilename;
    send(hostfd, msg.c_str(), msg.size(), 0);
    shutdown(hostfd, SHUT_WR);
  }

  int readAckCount = 0;
  int fileLatestVersion = -1;
  string upToDateHostIp;
  // // temporarily set to non-blocking mode
  // setSocketToNonBlocking(masterSocket, 1, 0);
  while (readAckCount < std::min(R, aliveMachineCount)) {
    for (auto tup : fdHostMap) {
      int hostfd = tup.first;
      char responseBuf[BUFFER_SIZE] = {0};
      // [requestId]\n
      // OK\n
      // [latest-version-number]
      int ret = recv(hostfd, responseBuf, BUFFER_SIZE, 0);
      if (ret > 0) {
        close(hostfd);
        // should check requestId
        ++readAckCount;
        char* responseVersion = strchr(strchr(responseBuf, '\n') + 1, '\n') + 1;
        int v = atoi(responseVersion);
        if (v > fileLatestVersion) {
          fileLatestVersion = v;
          upToDateHostIp = tup.second;
        }
        if (readAckCount >= std::min(R, aliveMachineCount))
          break;
      }
    }
  }
  // // reset back to blocking mode
  // setSocketToBlocking(masterSocket);

  // close all un-closed fd
  for (auto tup: fdHostMap)
    close(tup.first);

  return upToDateHostIp;
}

void* masterDriver(void*) {
  masterSocket = TCP_server(MASTER_PORT);

  while (1) {
    fprintf(stderr, "waiting for client requests...\n");

    setSocketToBlocking(masterSocket);

    struct sockaddr_storage clientaddr;
    socklen_t clientaddrsize = sizeof(clientaddr);

    int clientfd = accept(masterSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);
    if (clientfd == -1) {
      continue;
    }

    setSocketToNonBlocking(masterSocket, 1, 0);

    cout << "[master] accepted client " << clientfd << endl;

    char buffer[BUFFER_SIZE] = {0};
    ssize_t recv_value = recv(clientfd, buffer, BUFFER_SIZE, 0);
    if (recv_value <= 0) {
      close(clientfd);
      continue;
    }

    cout << "recv " << recv_value << " from client " << clientfd << endl;
    
    string line = string(buffer);
    string sdfsfilename;
    ++currentRequestId;

    int aliveMachineCount = numberOfAliveMachines();
    
    //=====================
    // PUT\n
    // [remote file name]\n
    // [file content]
    //=====================
    if (line.substr(0, 4) == "PUT\n") {
      string restOfRequest = line.substr(4);
      sdfsfilename = restOfRequest.substr(0, restOfRequest.find("\n"));

      cout << "[master] PUT " << sdfsfilename << endl;

      // open a temp file to store the incoming file to be put
      FILE * fp = fopen("tempfile", "w+");
      // write id of the request
      string firstline = to_string(currentRequestId) + "\n";
      fwrite(firstline.c_str(), 1, firstline.size(), fp);
      // write the request
      fwrite(buffer, 1, recv_value, fp);
      // receive the rest of the request store all in a temp file
      receiveFileContent(fp, clientfd);
      rewind(fp);

      shutdown(clientfd, SHUT_RD);

      // set of host that master send to
      set<int> currHostsSet;
      vector<string> replicaHosts = findReplicaTargets(sdfsfilename);

      // send PUT request and file content to all replicates
      for (string hostIp : replicaHosts) {
        // connect to the ip address at hashed index and its clockwised neighbors
        int hostfd = TCP_connect(hostIp.c_str(), std::stoi(SDFS_NODE_PORT));

        // [requst Id]\n
        // PUT\n
        // [remote file name]\n
        // [file content]
        sendFileContent(fp, hostfd);
        shutdown(hostfd, SHUT_WR);
        rewind(fp);

        currHostsSet.insert(hostfd);
        replica_map[sdfsfilename].insert(hostIp);
      }
      fclose(fp);

      // wait a subset of write ack
      waitWriteAck(masterSocket, aliveMachineCount, currHostsSet);

      // OK\n
      send(clientfd, "OK\n", 3, 0);
      shutdown(clientfd, SHUT_WR);
      close(clientfd);
    }

    //=====================
    // GET\n
    // [remote file name]
    //=====================
    else if (line.substr(0, 4) == "GET\n") {
      shutdown(clientfd, SHUT_RD);

      // get file name
      sdfsfilename = line.substr(4);
      cout << "[master] GET " << sdfsfilename << endl;

      if (replica_map.find(sdfsfilename) == replica_map.end()) {
        fprintf(stderr, "[master] File not found\n");

        // send File Not Found to the client in the format:
        // NOTFOUND\n
        // [remote file name]
        string msg = "NOTFOUND\n" + sdfsfilename;
        send(clientfd, msg.c_str(), msg.size(), 0);
        close(clientfd);
        continue;
      }

      string upToDateHostIp = peekFileVersion(sdfsfilename, masterSocket, aliveMachineCount);

      // send get request to upToDateHostIp
      // [requestId]
      // GET\n
      // [filename]
      int upToDateHost = TCP_connect(upToDateHostIp.c_str(), std::stoi(SDFS_NODE_PORT));
      string msg = std::to_string(currentRequestId) + "\nGET\n" + sdfsfilename;
      send(upToDateHost, msg.c_str(), msg.size(), 0);
      shutdown(upToDateHost, SHUT_WR);

      // 1. [requestId]\n
      //    OK\n
      //    [content]
      // 2. [requestId]\n
      //    NOTFOUND\n
      char recv_buffer[BUFFER_SIZE] = {0};
      int ret = recv(upToDateHost, recv_buffer, BUFFER_SIZE, 0);

      char* status = strchr(recv_buffer, '\n') + 1;
      if (strncmp(status, "OK\n", 3) == 0) {
        // open a temp file to store the incoming file to send back to client
        // OK\n
        // [content]
        FILE* fp = fopen("tempfile", "w+");
        fwrite(status, 1, ret - (status - recv_buffer), fp);
        receiveFileContent(fp, upToDateHost);

        // send response back to client
        rewind(fp);
        sendFileContent(fp, clientfd);
        close(clientfd);
        fclose(fp);
      } else {
        // NOTFOUND\n
        // [remote file name]
        string response = "NOTFOUND\n" + sdfsfilename;
        send(clientfd, response.c_str(), response.size(), 0);
        close(clientfd);
      }

      close(upToDateHost);
    }

    //============
    // DELETE\n
    // [filename]
    //============
    else if (line.substr(0, 7) == "DELETE\n") {
      shutdown(clientfd, SHUT_RD);

      // get file name
      sdfsfilename = line.substr(7);

      cout << "[master] DELETE " << sdfsfilename << endl;

      // first find if file exists
      if (replica_map.find(sdfsfilename) == replica_map.end()) {
        fprintf(stderr, "[master] File not found\n");

        // send File Not Found to the client in the format
        // NOTFOUND\n
        // [remote file name]
        string msg = "NOTFOUND\n" + sdfsfilename;
        send(clientfd, msg.c_str(), msg.size(), 0);
        close(clientfd);
        continue;
      }

      set<int> currHostsSet;

      // send DELETE request to all replicates
      for (string hostIp : replica_map[sdfsfilename]) {
        // connect to the ip address at hashed index and its clockwised neighbors
        int hostfd = TCP_connect(hostIp.c_str(), std::stoi(SDFS_NODE_PORT));

        // send the request in the format
        // [requestId]\n
        // DELETE\n
        // [remote file name]
        string msg = std::to_string(currentRequestId) + "\nDELETE\n" + sdfsfilename;
        send(hostfd, msg.c_str(), msg.size(), 0);
        shutdown(hostfd, SHUT_WR);
        currHostsSet.insert(hostfd);
      }

      // wait a subset of write ack
      waitWriteAck(masterSocket, aliveMachineCount, currHostsSet);

      // eraze the file record in replica_map
      replica_map.erase(sdfsfilename);

      // respond to client
      string msg = "OK\n";
      send(clientfd, msg.c_str(), msg.size(), 0);
      close(clientfd);
    }

    //====================
    // LS\n
    // [remote file names]
    //====================
    else if (line.substr(0, 3) == "LS\n") {
      shutdown(clientfd, SHUT_RD);

      // get file name
      sdfsfilename = line.substr(3);

      cout << "[master] LS " << sdfsfilename << endl;

      // first find if file exists
      if (replica_map.find(sdfsfilename) == replica_map.end()) {
        fprintf(stderr, "[master] File not found\n");

        // send File Not Found to the client in the format
        // NOTFOUND\n
        // [remote file name]
        string msg = "NOTFOUND\n" + sdfsfilename;
        send(clientfd, msg.c_str(), msg.size(), 0);
        close(clientfd);

      } else {
        // send OK to the client in the format - OK\n<list of all ip addresses who has the file>
        string msg = "OK\n";
        for (string hostIp : replica_map[sdfsfilename]) {
          msg += hostIp;
          msg += "\n";
        }
        send(clientfd, msg.c_str(), msg.size(), 0);
        close(clientfd);
      }
    }

    //=====================  
    // GET-VERSIONS\n
    // [remote file name]\n
    // [num of versions]
    //=====================
    else if (line.substr(0, 13) == "GET-VERSIONS\n") {
      // get file name
      string restOfRequest = line.substr(13);
      sdfsfilename = restOfRequest.substr(0, restOfRequest.find("\n"));
      string numVersions = restOfRequest.substr(restOfRequest.find("\n") + 1, 1); // assume numVersions <= 5

      cout << "[master] GET-VERSIONS " << sdfsfilename << " (" << numVersions << ")" << endl;

      if (replica_map.find(sdfsfilename) == replica_map.end()) {
        fprintf(stderr, "[master] File not found\n");

        // send File Not Found to the client in the format - NOTFOUND\n<remote file name>
        string msg = "OK\nFILE NOT FOUND";
        send(clientfd, msg.c_str(), msg.size(), 0);
        close(clientfd);
        continue;
      }

      fprintf(stderr, "[master] File found\n");

      string upToDateHostIp = peekFileVersion(sdfsfilename, masterSocket, aliveMachineCount);

      int hostfd = TCP_connect(upToDateHostIp.c_str(), std::stoi(SDFS_NODE_PORT));
      // GET-VERSIONS\n
      // [filename]\n
      // [num-versions]
      string msg = to_string(currentRequestId) + "\nGET-VERSIONS\n" + sdfsfilename + "\n" + numVersions;
      send(hostfd, msg.c_str(), msg.size(), 0);
      shutdown(hostfd, SHUT_WR);

      // [requestId]\n
      // OK\n
      // [content]
      char recv_buffer[BUFFER_SIZE] = {0};
      int ret = recv(hostfd, recv_buffer, BUFFER_SIZE, 0);

      char* content = strchr(strchr(recv_buffer, '\n') + 1, '\n') + 1;

      // open a temp file to store the incoming file to be get
      FILE * fp = fopen("tempfile", "w+");
      fwrite(content, 1, ret - (content - recv_buffer), fp);
      receiveFileContent(fp, hostfd);
      close(hostfd);

      // send file back to client
      rewind(fp);
      send(clientfd, "OK\n", 3, 0);
      sendFileContent(fp, clientfd);
      close(clientfd);

      fclose(fp);
    }

    close(clientfd);
  }

  return NULL;
}

#define FAILURE_DETECTION_INTERVAL 2
void* replicateFileDuringFailureMonitor(void*) {
  pthread_mutex_lock(&list_lock);
  vector<tuple<string, string, actions>> prev_membership_list = membership_list; // previous membership list
  pthread_mutex_unlock(&list_lock);

  while (1) {
    bool flag_membership_list_change = false;

    pthread_mutex_lock(&list_lock);

    for (int i = 0; i < membership_list.size(); ++i) {
      // if we detect some member fails
      if (std::get<2>(prev_membership_list[i]) == JOIN && std::get<2>(membership_list[i]) != JOIN) {
        string hostIp = std::get<0>(membership_list[i]);
        int hostfd = TCP_connect(hostIp.c_str(), std::stoi(SDFS_NODE_PORT));

        // find all the files on the failed machine
        set<string> files_set; // set of files on failed machine to be replicated
        for (auto & map_iter : replica_map) {
          set<string> ip_address_set = map_iter.second;
          if (ip_address_set.find(hostIp) != ip_address_set.end()) {
            files_set.insert(map_iter.first);
          }
        }

        for (auto & file : files_set) {
          // replicate the file content of the failed machine into calculated alive ones
          // first send GET request to one of the alive machine that has the file
          
          FILE *fp;
          
          
          // then send the PUT request
          // [requst Id]\n
          // PUT\n
          // [remote file name]\n
          // [file content]
          sendFileContent(fp, hostfd);
          shutdown(hostfd, SHUT_WR);
          rewind(fp);
        }

        // TODO: deal with adding and removing info on replica_map
        replica_map["somename"].insert(hostIp);
        
        close(hostfd);
      }
    }
    pthread_mutex_unlock(&list_lock);



    sleep(FAILURE_DETECTION_INTERVAL);
  }

  return NULL;
}

// broadcast replica_map updates to all sdfsprocess if there is change on replica_map
// action is PUT or DELETE
// filename is the file to be put or deleted
// ipAddress is the ip address where the file is put or deleted
void broadcastReplicaMap(string action, string filename, string ipAddress) {
  // send replica_map update request in the format:
  // PUT/DELETE\n
  // [filename]\n
  // [ipAddress]

  // convert replica_map into string
  string msg = action + "\n" + filename + "\n" + ipAddress;

  // send to all alive member in the membership_list
  pthread_mutex_lock(&list_lock);
  for (auto membership : membership_list) {
    string hostIP = std::get<0>(membership);
    int hostfd = TCP_connect(hostIP.c_str(), std::stoi(METADATA_UPDATE_PORT));
    send(hostfd, msg.c_str(), msg.size(), 0);
    close(hostfd);
  }
  pthread_mutex_unlock(&list_lock);
}
