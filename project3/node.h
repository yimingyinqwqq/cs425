#pragma once

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
#include <sys/types.h> 
#include <stdbool.h>
#include <sys/stat.h>
#include <dirent.h> 

#include <vector>
#include <utility>
#include <tuple>
#include <string>
#include <set>
#include <map>
#include <algorithm>
#include <iostream>
#include <chrono>
#include <ctime>
#include <locale>
#include <iomanip>
#include <sstream>

using std::chrono::system_clock;
using std::to_string;
using std::vector;
using std::pair;
using std::tuple;
using std::string;
using std::set;
using std::map;
using std::cout;
using std::cerr;
using std::endl;

#define DNS_IP "172.22.94.58"       // TODO: CHANGE THIS TO YOUR VM ADDRESS
#define DNS_PORT "7777"             // port for DNS server
#define ELECTION_PORT "7070"        // port for leader election
#define INTRODUCER_PORT "8888"      // port for introducer server
#define COMMUNICATION_PORT "8080"   // port for membership list communication, failure detector
#define MASTER_PORT "9999"          // port for client and master communicate
#define SDFS_NODE_PORT "9090"       // port for sdfs node and master server communicate
#define METADATA_UPDATE_PORT "9191" // port for master to send updates to all alive members
#define SDFS_FOLDER "./sdfs"         // folder to put all files
#define MONITOR_COUNT 3
#define W 3
#define R 2
#define REPLICA_COUNT 4

enum actions { LEAVE, JOIN, FAILED };

//============
// process.cpp
//============
/**
 * Run failure detector and handle membership list
 */
void processDriver(string myIpAddress);

/**
 * Print helpful debugging information based on input line
 * return true if command executed successfully, else return false
 * 
 * list_mem -- print full membership list
 * neighbor -- list all neighbors of the current process
 */
bool processDebugger(string line);

/**
 * Find the ip address of the p-th ALIVE neighbor (1-indexed), return "" if not found
 * assume list lock aquired
 */
string find_alive_target_ip(int p);


//===============
// introducer.cpp
//===============
/**
 * Response to DNS, add new process to the group membership list
 * Introducer and master should be the same process
 */
void* introducerDriver(void*);


//================
// sdfsprocess.cpp
//================
/**
 * Response to master command, do leader election when master failed
 * or new node join
 */
void sdfsProcessDriver();


//===========
// master.cpp
//===========
/**
 * Master server that receive request from client, and communicate to other backend servers
 */
void* masterDriver(void*);


//===========
// common.cpp
//===========
/**
 * exit(1) with msg
 */
void error(const char *msg);

/**
 * Return current time as a string in format `YYYY/MM/DD hh:mm:ss`
 */
string currentTimeString();

/**
 * Convert time string in format `YYYY/MM/DD hh:mm:ss` to time_t
 */
time_t stringToTime(string t);

/**
 * Create UPD server, return socket fd
 */
int UDP_server(const char* port);

/**
 * Create TCP server, return socket fd
 */
int TCP_server(const char *port);

/**
 * Setup UDP client, return sockfd
 */
int UDP_client();

/**
 * TCP connect client to `host` on `port`, return sockfd
 */
int TCP_connect(const char* host, int port);

/**
 * Send UDP `msg` to `ip` on `port`
 */
void UDP_send(int sockfd, int port, const char* ip, const char* msg);

/* TCP send all content in file to destfd
 * caller should take care of closing file and shutdown write to destfd
 */
void sendFileContent(FILE* file, int destfd);

/* TCP receive all content from destfd to file
 * caller should take care of closing file and shutdown read to destfd
 */
void receiveFileContent(FILE* file, int destfd);