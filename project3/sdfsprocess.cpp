#include "node.h"

#define BUFFER_SIZE 1024

// filename (not including folder) -> set of versions, versions start from 1
map<string, set<int>> localFileVersions;

// map of filename to set of machines' ip addresses
map<string, set<string>> replica_map;        

extern vector<tuple<string, string, actions>> membership_list;    // list of <ip, timestamp, actions>
extern pthread_mutex_t list_lock;
extern pthread_cond_t list_cv;
extern string g_MyIp;
extern string g_MasterIp;
extern pthread_mutex_t masterIp_lock;
extern pthread_t masterProgramThreads[2];
extern bool masterProgramRunning;
extern int masterSocket;

// convert ip address to long int, and this is the hashed value (unique)
// exp-ecting `ip` to be "" or ipv4 format
long ipToLong(string ip) {
  if (ip == "")
    return -1;
  int a, b, c, d;
  sscanf(ip.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d);
  char s[20] = {0};
  sprintf(s, "%d%d%d%d", a, b, c, d);
  return atol(s);
}

// return 0 if not yet have any versions, else latest version number
int latestVersion(string filename) {
  if (localFileVersions.find(filename) == localFileVersions.end() || localFileVersions[filename].empty())
    return 0;
  else
    return *(--localFileVersions[filename].end());
}

// return string example: "SDFS_FOLDER/filename_v1"
string makeFileVersionPath(string filename, int version) {
  return string(SDFS_FOLDER) + "/" + filename + "_v" +  to_string(version);
}

bool checkMasterHasFailed(string masterIp) {
  bool res = false;
  pthread_mutex_lock(&list_lock);
  for (tuple<string, string, actions>& tup : membership_list) {
    if (std::get<0>(tup) == masterIp && std::get<2>(tup) != JOIN) {
      res = true;
      break;
    }
  }
  pthread_mutex_unlock(&list_lock);
  return res;
}

// pause master program for a while for leader election
// set g_MasterIp to -1 and cancel master thread if running
// assuming masterIp lock is already aquired
void pauseMasterProgram() {
  g_MasterIp = -1;
  close(masterSocket);
  masterSocket = -1;
  if (masterProgramRunning) {
    pthread_cancel(masterProgramThreads[0]);
    pthread_cancel(masterProgramThreads[1]);
    masterProgramRunning = false;
    cout << "[sdfsprocess] canceled all master threads" << endl;
  }
}

// assuming masterIp lock is already aquired
void startMasterProgram() {
  g_MasterIp = g_MyIp;
  if (!masterProgramRunning) {
    pthread_create(&masterProgramThreads[0], NULL, introducerDriver, NULL);
    pthread_create(&masterProgramThreads[1], NULL, masterDriver, NULL);
    masterProgramRunning = true;
    cout << "[sdfsprocess] started all master threads" << endl;
  } 
}

// select leader with highest value in the system
void* leaderElection(void*) {
  // periodically check if leader has failed
  // send out election message if leader failed
  int electionSocket = TCP_server(ELECTION_PORT);

  // set timeout for the socket
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(electionSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  struct sockaddr_storage clientaddr;
  socklen_t clientaddrsize = sizeof(clientaddr);

  bool duringElection = false;
  string prevElectionIp = "";     // previous sent election ip
  bool isLastHopInLeaderSelection = false;

  bool firstTimeJoin = true;      // true if I newly joined the system

  while (1) {
    // check if the membership list has already broadcast to me after my join
    pthread_mutex_lock(&list_lock);
    if (membership_list.empty()) {
      pthread_mutex_unlock(&list_lock);
      continue;
    }
    pthread_mutex_unlock(&list_lock);

    // I am in, start participate in leader election
    int peer_fd = accept(electionSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    char buffer[BUFFER_SIZE] = {0};
    int ret = recv(peer_fd, buffer, BUFFER_SIZE, 0);
    shutdown(peer_fd, SHUT_RD);

    // get next hop ip address
    pthread_mutex_lock(&list_lock);
    string nextHopIp = find_alive_target_ip(1);
    pthread_mutex_unlock(&list_lock);

    if (nextHopIp == "") {
      // current process is the only process left in the system
      pthread_mutex_lock(&masterIp_lock);
      if (g_MasterIp != g_MyIp) {
        cout << "I am the only process in the system" << endl;
        startMasterProgram();
      }
      pthread_mutex_unlock(&masterIp_lock);

      continue;
    }

    if (ret == -1) {
      // did not receive election information from others
      // check master status and potentially start election
      
      pthread_mutex_lock(&masterIp_lock);
      if (!duringElection && (checkMasterHasFailed(g_MasterIp) || firstTimeJoin)) {
        cout << (firstTimeJoin ? g_MyIp + " first time join" : "detected leader failure: " + g_MasterIp) + ", start election" << endl;

        // send election message to first successor
        int nextHopSocket = TCP_connect(nextHopIp.c_str(), std::stoi(ELECTION_PORT));

        if (nextHopSocket != -1) {
          // invalidate current master before new master is selected
          pauseMasterProgram();

          // ELECTION\n
          // [myIpAddress]
          string election_msg = "ELECTION\n" + g_MyIp;
          send(nextHopSocket, election_msg.c_str(), election_msg.size(), 0);
          close(nextHopSocket);
          duringElection = true;
          prevElectionIp = g_MyIp;
          isLastHopInLeaderSelection = false;
          firstTimeJoin = false;

          // cout << "send " << election_msg << " to " << nextHopIp << endl;
        }
      }
      pthread_mutex_unlock(&masterIp_lock);

    } else {  // ret != -1
      // in election round and election continues
      int nextHopSocket = TCP_connect(nextHopIp.c_str(), std::stoi(ELECTION_PORT));

      // ELECTION\n
      // [ip]
      if (strncmp("ELECTION\n", buffer, 9) == 0) {
        // cout << "recv election msg: " <<  buffer << endl;

        // master election happening
        // invalidate current master before new master is selected
        pthread_mutex_lock(&masterIp_lock);
        pauseMasterProgram();
        pthread_mutex_unlock(&masterIp_lock);

        duringElection = true;
        firstTimeJoin = false;
        isLastHopInLeaderSelection = false;

        string recvIp = string(buffer + 9);
        
        // compare myIp, recvIp, and prevElectionIp, forward the largest one if not already done so
        if (ipToLong(g_MyIp) > ipToLong(prevElectionIp) && ipToLong(g_MyIp) >= ipToLong(recvIp)) {
          // ELECTION\n
          // [G_MyIp]
          string election_msg = "ELECTION\n" + g_MyIp;
          send(nextHopSocket, election_msg.c_str(), election_msg.size(), 0);
          prevElectionIp = g_MyIp;
          // cout << "forward elected msg " << election_msg << " to " << nextHopIp << endl;
        } else if (ipToLong(recvIp) > ipToLong(prevElectionIp) && ipToLong(recvIp) >= ipToLong(g_MyIp)) {
          // ELECTION\n
          // [recvIp]
          string election_msg = "ELECTION\n" + recvIp;
          send(nextHopSocket, election_msg.c_str(), election_msg.size(), 0);
          prevElectionIp = recvIp;
          // cout << "forward elected msg " << election_msg << " to " << nextHopIp << endl;
        } else if (recvIp == prevElectionIp) {
          // ELECTED\n
          // [recvIp]
          string election_msg = "ELECTED\n" + recvIp;
          send(nextHopSocket, election_msg.c_str(), election_msg.size(), 0);
          isLastHopInLeaderSelection = true;
          // cout << "election back, send " << election_msg << " to " << nextHopIp << endl;
        } else {  
          // myIp == prevElectionIp is the largest of the three
          // do nothing, I have already forward the right ip (my election)
        }
      }

      // ELECTED\n
      // [ip]
      else if (strncmp("ELECTED\n", buffer, 8) == 0) {
        // only the process who init this message should get this message twice
        // all processes who got this message are done with the election this time
        // cout << "got elected notice: " << buffer << endl;

        string electedIp = string(buffer + 8);

        pthread_mutex_lock(&masterIp_lock);
        g_MasterIp = electedIp;
        pthread_mutex_unlock(&masterIp_lock);

        if (electedIp == g_MyIp) {
          cout << "i am master" << endl;
          
          pthread_mutex_lock(&masterIp_lock);
          startMasterProgram();
          pthread_mutex_unlock(&masterIp_lock);
        } else {
          if (masterProgramRunning) {
            pauseMasterProgram();
            // error(("non-master process " + g_MyIp + " running master program").c_str());
          }
        }

        if (!isLastHopInLeaderSelection) {
          // ELECTED\n
          // [electedIp]
          string election_msg = "ELECTED\n" + electedIp;
          send(nextHopSocket, election_msg.c_str(), election_msg.size(), 0);
          // cout << "forward elected msg " << election_msg << " to " << nextHopIp << endl;
        }

        // all set on my part, reinit these field
        duringElection = false;
        prevElectionIp = "";
        isLastHopInLeaderSelection = false;
      }

      close(nextHopSocket);
    }
  }
}

void* sdfsCommandHandler(void*) {
  int serverSocket = TCP_server(SDFS_NODE_PORT);

  struct sockaddr_storage clientaddr;
  socklen_t clientaddrsize = sizeof(clientaddr);
  int ret;

  while (1) {
    int client_fd = accept(serverSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    char buffer[BUFFER_SIZE] = {0};
    // [requestId]\n
    // ...
    ret = recv(client_fd, buffer, BUFFER_SIZE, 0);
    if (ret <= 0) {
      close(client_fd);
      continue;
    }

    // cout << "[process] recv: " << string(buffer) << endl;

    char* IdLinebreak = strchr(buffer, '\n');
    *IdLinebreak = '\0';
    string requestId = string(buffer);
    char* fullRequest = IdLinebreak + 1;
    
    // GET\n
    // [filename]
    if (strncmp(fullRequest, "GET\n", 4) == 0) {
      shutdown(client_fd, SHUT_RD);

      string filename = string(fullRequest + 4);
      string fileWithVersionPath = makeFileVersionPath(filename, latestVersion(filename));

      FILE* f = fopen(fileWithVersionPath.c_str(), "r");
      if (!f) {
        // [requestId]\n
        // NOTFOUND\n
        string msg = requestId + "\nNOTFOUND\n";
        send(client_fd, msg.c_str(), msg.size(), 0);
      } else {
        // [requestId]\n
        // OK\n
        // [content]
        string msg = requestId + "\nOK\n";
        send(client_fd, msg.c_str(), msg.size(), 0);
        sendFileContent(f, client_fd);
        fclose(f);
      }
      
      close(client_fd);
    }

    // PEEK\n
    // [filename]
    else if (strncmp(fullRequest, "PEEK\n", 5) == 0) {
      shutdown(client_fd, SHUT_RD);
      string filename = string(fullRequest + 5);

      // [requestId]\n
      // OK\n
      // [latest-version-number]
      string msg = requestId + "\nOK\n" + to_string(latestVersion(filename));
      send(client_fd, msg.c_str(), msg.size(), 0);
      close(client_fd);
    }

    // PUT\n
    // filename\n
    // [content]
    else if (strncmp(fullRequest, "PUT\n", 4) == 0) {
      char* newline = strchr(fullRequest + 4, '\n');
      *newline = '\0';
      string filename = string(fullRequest + 4);

      int version = latestVersion(filename);
      if (version == 0)
        version = 1;
      else
        version++;

      string fileWithVersionPath = makeFileVersionPath(filename, version);

      localFileVersions[filename].insert(version);

      FILE* f = fopen(fileWithVersionPath.c_str(), "w+");
      write(fileno(f), newline + 1, ret - (newline + 1 - buffer));
      while ((ret = recv(client_fd, buffer, BUFFER_SIZE, 0)) > 0) {
        write(fileno(f), buffer, ret);
      }
      shutdown(client_fd, SHUT_RD);

      // [requestId]\n
      // OK\n
      string msg = requestId + "\nOK\n";
      send(client_fd, msg.c_str(), msg.size(), 0);
      close(client_fd);

      fclose(f);
    }

    // DELETE\n
    // [filename]
    else if (strncmp(fullRequest, "DELETE\n", 7) == 0) {
      string filename = string(fullRequest + 7);

      for (int version : localFileVersions[filename]) {
        string fileWithVersionPath = makeFileVersionPath(filename, version);
        unlink(fileWithVersionPath.c_str());
      }
      localFileVersions.erase(filename);

      // [requestId]\n
      // OK\n
      string msg = requestId + "\nOK\n";
      send(client_fd, msg.c_str(), msg.size(), 0);
      close(client_fd);
    }

    // GET-VERSIONS\n
    // [filename]\n
    // [num-versions]
    else if (strncmp(fullRequest, "GET-VERSIONS\n", 13) == 0) {
      shutdown(client_fd, SHUT_RD);

      char* newline = strchr(fullRequest + 13, '\n');
      *newline = '\0';
      string filename = string(fullRequest + 13);
      int numVersions = atoi(newline + 1);

      if (localFileVersions.find(filename) == localFileVersions.end()) {
        // [requestId]\n
        // OK\n
        // FILE NOT FOUND
        string msg = requestId + "\nOK\n" + "FILE NOT FOUND";
        send(client_fd, msg.c_str(), msg.size(), 0);
      } else {
        // [requestId]\n
        // OK\n
        // [content]
        string msg = requestId + "\nOK\n";
        send(client_fd, msg.c_str(), msg.size(), 0);

        auto versionIter = --localFileVersions[filename].end();
        for (int i = 1; i <= std::min(numVersions, (int)localFileVersions[filename].size()); ++i) {
          char versionDelimiters[128] = {0};
          sprintf(versionDelimiters, "----------------------------\n-----%dst Latest Version-----\n----------------------------\n\n", i);
          send(client_fd, versionDelimiters, strlen(versionDelimiters), 0);

          string fileWithVersionPath = makeFileVersionPath(filename, *versionIter);
          FILE* f = fopen(fileWithVersionPath.c_str(), "r");
          sendFileContent(f, client_fd);
          fclose(f);
          --versionIter;
        }
      }

      close(client_fd);
    }

    else {
      cout << "Invalid command from master" << endl;
    }
  }
}

void* sdfsReplicaMapUpdateHandler(void*) {
  int serverSocket = TCP_server(METADATA_UPDATE_PORT);

  struct sockaddr_storage clientaddr;
  socklen_t clientaddrsize = sizeof(clientaddr);
  int ret;

  while (1) {
    int client_fd = accept(serverSocket, (struct sockaddr *) &clientaddr, &clientaddrsize);

    char buffer[BUFFER_SIZE] = {0};
    int ret = recv(client_fd, buffer, BUFFER_SIZE, 0);
    if (ret <= 0) {
      continue;
    }

    string request = string(buffer);

    int pos = request.find("\n");
    string action = request.substr(0, pos);
    request.erase(0, pos + 1);

    pos = request.find("\n");
    string filename = request.substr(0, pos);
    request.erase(0, pos + 1);

    pos = request.find("\n");
    string ipAddress = request.substr(0, pos);

    if (action == "PUT") {
      replica_map[filename].insert(ipAddress);
    } else if (action == "DELETE") {
      // assume nothing goes wrong
      replica_map[filename].erase(ipAddress);
    }
    
    close(client_fd);
  }

  return NULL;
}

void sdfsProcessDriver() {
  pthread_t threads[2];
  pthread_create(&threads[0], NULL, sdfsCommandHandler, NULL);
  pthread_create(&threads[1], NULL, leaderElection, NULL);
}
