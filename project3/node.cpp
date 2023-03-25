#include "node.h"

#define BUFFER_SIZE 1024

string g_MyIp;        // will not change once assigned
string g_MasterIp;    // may change
pthread_mutex_t masterIp_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_t masterProgramThreads[2];  // threads to run master programs if I am the master
bool masterProgramRunning = false;

extern map<string, set<int>> localFileVersions;


string getMyIpAddress() {
  const char* google_dns_server = "8.8.8.8";
  int dns_port = 53;

  struct sockaddr_in serv;
  int sock = socket(AF_INET, SOCK_DGRAM, 0);

  memset(&serv, 0, sizeof(serv));
  serv.sin_family = AF_INET;
  serv.sin_addr.s_addr = inet_addr(google_dns_server);
  serv.sin_port = htons(dns_port);

  connect(sock, (const struct sockaddr*)&serv, sizeof(serv));

  struct sockaddr_in name;
  socklen_t namelen = sizeof(name);
  getsockname(sock, (struct sockaddr*)&name, &namelen);

  char buffer[80];
  const char* p = inet_ntop(AF_INET, &name.sin_addr, buffer, 80);
  if (p == NULL)
    error(strerror(errno));

  close(sock);
  return string(buffer);
}

// folder name must not have trailing '/'
// only remove files in the folder
void clearFolder(const char* folderName) {
  DIR* d = opendir(folderName);
  struct dirent* dir;
  if (d) {
    while ((dir = readdir(d)) != NULL) {
      if (strcmp(dir->d_name, ".") == 0 || strcmp(dir->d_name, "..") == 0)
        continue;
      string filePath = string(folderName) + "/" + dir->d_name;
      unlink(filePath.c_str());
    }
    closedir(d);
  }
}

// set up everything for a node (one sdfs server) when it first join
void initSdfsNode() {
  // initialize the SDFS folder first
  mkdir(SDFS_FOLDER, 0700);
  clearFolder(SDFS_FOLDER);

  // 1. get self ip
  // 2. find master ip from DNS server
  //    if not found, run as master and run `introducer`, `master`, JOIN itself
  // 3. then run `process` to maintain membership list, and `sdfsprocess` to handle leader election and master command
  int init_sockfd = UDP_client();
  g_MyIp = getMyIpAddress();

  // ask DNS for master ip
  UDP_send(init_sockfd, std::stoi(DNS_PORT), DNS_IP, "WhoIsMater");

  struct sockaddr_in masterAddr;
  socklen_t masterLen = sizeof(masterAddr);
  // format: [master ip]
  char masterIpBuf[INET_ADDRSTRLEN] = {0};
  recvfrom(init_sockfd, masterIpBuf, INET_ADDRSTRLEN, 0, (struct sockaddr *) &masterAddr, &masterLen);
  string receivedIp = string(masterIpBuf);

  if (receivedIp == "NONE") {
    pthread_mutex_lock(&masterIp_lock);
    g_MasterIp = g_MyIp;
    pthread_mutex_unlock(&masterIp_lock);
    // run as introducer and master
    pthread_create(&masterProgramThreads[0], NULL, introducerDriver, NULL);
    pthread_create(&masterProgramThreads[1], NULL, masterDriver, NULL);
    masterProgramRunning = true;
    sleep(1); // make sure introducer program launch complete
  } else {
    pthread_mutex_lock(&masterIp_lock);
    g_MasterIp = receivedIp;
    pthread_mutex_unlock(&masterIp_lock);
  }

  // run membership list and failure detector
  processDriver(g_MyIp);   // maintain membership list and run failure detector
  sdfsProcessDriver();

  // JOIN myself by confirming setup completed with introducer, including master ip in the message
  // format: [master ip]
  pthread_mutex_lock(&masterIp_lock);
  UDP_send(init_sockfd, std::stoi(INTRODUCER_PORT), g_MasterIp.c_str(), g_MasterIp.c_str());
  cout << "initialization completed, master is: " << g_MasterIp << endl;
  pthread_mutex_unlock(&masterIp_lock);

  close(init_sockfd);
}

/**
 * Split the given command into a vector, return the empty vector if incorrect num of arguments
 */
vector<string> split_command_line_argument(string line, int arg_count) {
  vector<string> commands;
  
  std::istringstream ss(line);
  string command;

  // seperate all arguments
  while (getline(ss, command, ' ')) {
    commands.push_back(command);
  }

  // we expect a different number of commands
  if (commands.size() != arg_count) {
    commands.clear();
  }

  return commands;
}

void sdfsClientRequestHandler(string line) {
  if (line == "")
    return;

  pthread_mutex_lock(&masterIp_lock);
  int masterSocket = TCP_connect(g_MasterIp.c_str(), std::stoi(MASTER_PORT));
  pthread_mutex_unlock(&masterIp_lock);

  struct timeval tv;
  tv.tv_sec = 30;
  tv.tv_usec = 0;
  setsockopt(masterSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  //===============================
  // put <local file> <remote file>
  //===============================
  if (line.substr(0, 4) == "put ") {
    vector<string> commands = split_command_line_argument(line, 3);
    if (commands.empty()) {
      fprintf(stderr, "format: put localfilename sdfsfilename\n");
      close(masterSocket);
      return;
    }

    string localfilename = commands[1];
    string sdfsfilename = commands[2];
    fprintf(stderr, "===== PUT %s to %s =====\n", localfilename.c_str(), sdfsfilename.c_str());

    // open the file to be put
    FILE* fp = fopen(localfilename.c_str(), "r");
    if (fp == NULL) {
      fprintf(stderr, "%s Not Found\n", localfilename.c_str());
      close(masterSocket);
      return;
    }

    // send PUT request to master in the format:
    // PUT\n
    // [remote file name]\n
    // [file content]
    string msg = "PUT\n" + sdfsfilename + "\n";
    send(masterSocket, msg.c_str(), msg.size(), 0);
    sendFileContent(fp, masterSocket);
    shutdown(masterSocket, SHUT_WR);

    // receive message and requested file content in the format:
    // OK\n
    char recv_buffer[BUFFER_SIZE] = {0};
    int recvret = recv(masterSocket, recv_buffer, BUFFER_SIZE, 0);
    shutdown(masterSocket, SHUT_RD);

    if (recvret > 0) {
      fprintf(stdout, "%s PUT Succeeded\n", sdfsfilename.c_str());
    } else {
      fprintf(stdout, "Request timed out, please try again\n");
    }

    fclose(fp);
  } 
  
  //===============================
  // get <remote file> <local file>
  //===============================
  else if (line.substr(0, 4) == "get ") {
    vector<string> commands = split_command_line_argument(line, 3);
    if (commands.empty()) {
      fprintf(stderr, "format: get sdfsfilename localfilename\n");
      close(masterSocket);
      return;
    }

    string sdfsfilename = commands[1];
    string localfilename = commands[2];
    fprintf(stderr, "===== GET %s to %s =====\n", sdfsfilename.c_str(), localfilename.c_str());

    // send GET request to master in the format:
    // GET\n
    // [remote file name]
    string msg = "GET\n" + sdfsfilename;
    send(masterSocket, msg.c_str(), msg.size(), 0);
    shutdown(masterSocket, SHUT_WR);

    // receive message and requested file content in the format
    // 1. OK\n
    //    [remote file content]
    // 2. NOTFOUND\n
    //    [remote file name]
    char recv_buffer[BUFFER_SIZE] = {0};
    int recvret = recv(masterSocket, recv_buffer, 3, 0);

    if (recvret > 0) {
      if (string(recv_buffer) == "OK\n") {
        // receive the file
        FILE * fp = fopen(localfilename.c_str(), "w+");
        receiveFileContent(fp, masterSocket);
        fclose(fp);
        fprintf(stdout, "%s Get Succeeded\n", sdfsfilename.c_str());
      } else {
        fprintf(stdout, "%s Not Found\n", sdfsfilename.c_str());
      }
    } else {
      fprintf(stdout, "Request timed out, please try again\n");
    }
  
    shutdown(masterSocket, SHUT_RD);
  } 

  //=====================
  // delete <remote file>
  //=====================
  else if (line.substr(0, 7) == "delete ") {
    vector<string> commands = split_command_line_argument(line, 2);
    if (commands.empty()) {
      fprintf(stderr, "format: delete sdfsfilename\n");
      close(masterSocket);
      return;
    }

    string sdfsfilename = commands[1];
    fprintf(stderr, "===== DELETE %s =====\n", sdfsfilename.c_str());

    // send DELETE request to master in the format:
    // DELETE\n
    // [remote file name]
    string msg = "DELETE\n" + sdfsfilename;
    send(masterSocket, msg.c_str(), msg.size(), 0);
    shutdown(masterSocket, SHUT_WR);

    // receive response in the format:
    // OK\n
    // [filename]
    char recv_buffer[BUFFER_SIZE] = {0};
    int recvret = recv(masterSocket, recv_buffer, BUFFER_SIZE, 0);
    shutdown(masterSocket, SHUT_RD);

    if (recvret > 0) {
      fprintf(stdout, "%s DELETE Succeeded\n", sdfsfilename.c_str());
    } else {
      fprintf(stdout, "Request timed out, please try again\n");
    }
  }

  //====================================
  // ls <remote file> : all vm addresses
  //====================================
  else if (line.substr(0, 3) == "ls ") {
    vector<string> commands = split_command_line_argument(line, 2);
    if (commands.empty()) {
      fprintf(stderr, "format: ls sdfsfilename\n");
      close(masterSocket);
      return;
    }

    string sdfsfilename = commands[1];
    fprintf(stdout, "===== LIST %s =====\n", sdfsfilename.c_str());

    // send LS request to master in the format:
    // LS\n
    // [remote file names]
    string msg = "LS\n" + sdfsfilename;
    send(masterSocket, msg.c_str(), msg.size(), 0);
    shutdown(masterSocket, SHUT_WR);

    // receive message and requested file content in the format
    // 1. OK\n
    //    [list of ip addresses]
    // 2. NOTFOUND\n
    //    [remote file name]
    char recv_buf[4] = {0};
    int recvret = recv(masterSocket, recv_buf, 3, 0);

    if (recvret > 0) {
      char recv_buffer[BUFFER_SIZE] = {0};
      if (string(recv_buf) == "OK\n") {
        // receive the list
        recv(masterSocket, recv_buffer, BUFFER_SIZE, 0);
        fprintf(stdout, "%s", recv_buffer);
      } else {
        fprintf(stderr, "%s Not Found\n", sdfsfilename.c_str());
      }
    } else {
      fprintf(stdout, "Request timed out, please try again\n");
    }

    shutdown(masterSocket, SHUT_RD);
  } 
  
  //=======================================
  // store : all files stored at current vm
  //=======================================
  else if (line == "store") {
    fprintf(stdout, "===== STORE =====\n");
    for (auto tup : localFileVersions) {
      cout << tup.first << endl;
    }
  }

  //=======================================================
  // get-versions <remote file> <num-versions> <local file>
  //=======================================================
  else if (line.substr(0, 13) == "get-versions ") {
    vector<string> commands = split_command_line_argument(line, 4);
    if (commands.empty()) {
      fprintf(stderr, "format: get-versions sdfsfilename num-versions localfilename\n");
      close(masterSocket);
      return;
    }

    string sdfsfilename = commands[1];
    string num_version = commands[2];
    string localfilename = commands[3];
    fprintf(stderr, "===== GET-VERSIONS %s to %s =====\n", sdfsfilename.c_str(), localfilename.c_str());

    // send GET-VERSIONS request to master in the format:
    // GET-VERSIONS\n
    // [remote file name]\n
    // [num of versions]
    string msg = "GET-VERSIONS\n" + sdfsfilename + "\n" + num_version;
    send(masterSocket, msg.c_str(), msg.size() + 4, 0);
    shutdown(masterSocket, SHUT_WR);

    // receive message and requested file content in the format
    // OK\n
    // [remote file content]
    char recv_buffer[BUFFER_SIZE] = {0};
    int recvret = recv(masterSocket, recv_buffer, 3, 0);

    if (recvret > 0) {
      FILE * fp = fopen(localfilename.c_str(), "w+");
      receiveFileContent(fp, masterSocket);
      fclose(fp);
      fprintf(stderr, "%s GET-VERSIONS Complete\n", sdfsfilename.c_str());
    } else {
      fprintf(stdout, "Request timed out, please try again\n");
    }

    shutdown(masterSocket, SHUT_RD);
  }

  else {
    cout << "(invalid command)" << endl;
  }

  close(masterSocket);
}

// take in a command from stdin `line`, TCP send it to master
// wait for response and handle it
void sdfsClientHandler() {
  // 4. start reading command from stdin
  for (string line; std::getline(std::cin, line);) {
    // debug membership list and failure detector
    if (processDebugger(line)) {
      continue;
    }

    if (line == "list_master") {
      pthread_mutex_lock(&masterIp_lock);
      fprintf(stderr, "===== Master ID is %s =====\n", g_MasterIp.c_str());
      pthread_mutex_unlock(&masterIp_lock);
      continue;
    }

    sdfsClientRequestHandler(line);
  }
}

/* entry point of the entire program */
int main(int argc, char **argv) {
  //=================
  // init a SDFS node
  //=================
  initSdfsNode();

  //============================
  // Start of the client program
  //============================
  sdfsClientHandler();
  return 0;
}
