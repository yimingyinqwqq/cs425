#include "common.cpp"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdbool.h>

#include <vector>
#include <utility>
#include <tuple>
#include <string>
#include <set>
#include <algorithm>
#include <iostream>
#include <chrono>
#include <ctime>
#include <locale>
#include <iomanip>
#include <sstream>

using std::chrono::system_clock;
using std::vector;
using std::pair;
using std::tuple;
using std::string;
using std::set;
using std::cout;
using std::cerr;
using std::endl;

#define BUFSIZE 1024
#define COMMUNICATION_PORT "8080"
#define INTRODUCER_PORT "8888"
#define INTRODUCER_IP "172.22.94.58" // TODO: CHANGE THIS TO YOUR VM ADDRESS
#define BUFFER_SIZE 4096
#define MONITOR_COUNT 3
#define PING_ACK_TIMEOUT 2
#define BROADCAST_UPDATE_INTERVAL 2


enum actions { LEAVE, JOIN, FAILED };
static string machine_ip;         // ip of the current machine
static vector<tuple<string, string, actions>> membership_list;    // list of <ip, timestamp, actions>
static pthread_mutex_t list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t list_cv = PTHREAD_COND_INITIALIZER;
static set<string> ack_set;       // used to keep track of acknowledged machine ip
static pthread_mutex_t ack_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ack_cv = PTHREAD_COND_INITIALIZER;
static int sockfd;                // current process socket fd
static FILE* log_file;
static pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t file_cv = PTHREAD_COND_INITIALIZER;



actions string_to_actions(const char* action) {
  if (strcmp(action, "JOIN") == 0)
    return JOIN;
  else if (strcmp(action, "LEAVE") == 0)
    return LEAVE;
  else  // "FAILED"
    return FAILED;
}

string actions_to_string(actions act) {
  if (act == JOIN)
    return "JOIN";
  else if (act == LEAVE)
    return "LEAVE";
  else  // FAILED
    return "FAILED";
}

// assume list lock already aquired
int find_machine_ip_index() {
  // find self location
  int machine_ip_idx = -1;
  for (int i = 0; i < membership_list.size(); ++i) {
    if (std::get<0>(membership_list[i]) == machine_ip) {
      machine_ip_idx = i;
      break;
    }
  }
  return machine_ip_idx;
}

// assume list lock aquired
// find the ip address of the p-th ALIVE neighbor
string find_alive_target_ip(int p) {
  // find self location
  int n = membership_list.size();
  int machine_ip_idx = find_machine_ip_index();

  if (machine_ip_idx == -1 || p > n - 1)
    return "";

  for (int i = 1; i <= n - 1; ++i) {
    if (std::get<2>(membership_list[(machine_ip_idx + i) % n]) == JOIN) {
      p -= 1;
      if (p == 0)
        return std::get<0>(membership_list[(machine_ip_idx + i) % n]);
    }
  }

  return "";
}

// /**
//  * Return p-th neighbour target ip address (1-indexed) or empty string if index out of bound
//  * - assume mutex lock for the list is already aquired
//  */
// string find_target_ip(int p) {
//   int n = membership_list.size();
//   if (p > n - 1)
//     return "";
  
//   // find self location
//   int machine_ip_idx = find_machine_ip_index();

//   if (machine_ip_idx == -1)
//     return "";

//   if (machine_ip_idx + p < n) {
//     assert(machine_ip_idx + p >= 0);
//     assert(std::get<0>(membership_list[machine_ip_idx + p]) != machine_ip);
//     return std::get<0>(membership_list[machine_ip_idx + p]);
//   }
//   else if (machine_ip_idx + p - n < n) {
//     assert(machine_ip_idx + p - n >= 0);
//     assert(std::get<0>(membership_list[machine_ip_idx + p - n]) != machine_ip);
//     return std::get<0>(membership_list[machine_ip_idx + p - n]);
//   } else {
//     return ""; 
//   }
// }

// assume list lock already obtained
void send_all_neighbor_msg(string msg) {
  for (int i = 1; i <= std::min((int) membership_list.size() - 1, MONITOR_COUNT); ++i) {
    string target_ip = find_alive_target_ip(i);

    if (target_ip == "")
      continue;

    struct sockaddr_in serveraddr;
    memset((char *) &serveraddr, 0, sizeof(serveraddr));

    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(std::stoi(COMMUNICATION_PORT));
    serveraddr.sin_addr.s_addr = inet_addr(target_ip.c_str());

    sendto(sockfd, msg.c_str(), msg.size(), 0, (struct sockaddr *) &serveraddr, sizeof(serveraddr));
    // cout << msg << " --> " << std::get<0>(membership_list[tmp_ip_idx + i]) << endl;
  }
  return;
}

/**
 * update ip status, log to file if action change, otherwise only update timestamp
 */ 
void update_ip_status(string target_ip, actions act) {
  // get current timestamp
  std::time_t t = system_clock::to_time_t(system_clock::now());
  string time_str = std::ctime(&t); // this includes a '\n' at the end
  time_str.pop_back();

  // find the target_ip and remove from membership list
  pthread_mutex_lock(&list_lock);
  for (int i = 0; i < membership_list.size(); ++i) {
    actions original_act = std::get<2>(membership_list[i]);

    if (std::get<0>(membership_list[i]) == target_ip) {
      // change status
      membership_list[i] = {target_ip, time_str, act};

      if (act != original_act) {
        // write to log file
        string log_info = "FAILED " + target_ip + " " + time_str + "\n";
        pthread_mutex_lock(&file_lock);
        write(fileno(log_file), log_info.c_str(), log_info.size());
        pthread_mutex_unlock(&file_lock);
      }

      break;
    }
  }
  pthread_mutex_unlock(&list_lock);

  return;
}

/**
 * Remove old entry if found, push back the new one
 * Only invoked in vm1 (introducer)
 */
void new_process_join(string ip_address, string timestamp) {
  bool found = false;
  bool status_change = false;
  pthread_mutex_lock(&list_lock);
  for (int i = 0; i < membership_list.size(); ++i) {
    if (std::get<0>(membership_list[i]) == ip_address) {
      found = true;
      if (std::get<2>(membership_list[i]) != JOIN) {  // target ip rejoined after 20s
        status_change = true;
        membership_list[i] = {ip_address, timestamp, JOIN}; // update status
      }
      break;
    }
  }
  
  if (!found) {
    // push the new member to membership list
    membership_list.push_back({ip_address, timestamp, JOIN});
    // wake up other waiting thread
    pthread_cond_broadcast(&list_cv);
  }
  pthread_mutex_unlock(&list_lock);

  if (!found || status_change) {
    // write to log file
    string log_info = "JOIN " + ip_address + " " + timestamp + "\n";
    pthread_mutex_lock(&file_lock);
    write(fileno(log_file), log_info.c_str(), log_info.size());
    pthread_mutex_unlock(&file_lock);
  }
}

/** 
 * update membership list when needed
 */
void compare_and_update_memlist(const vector<tuple<string, string, actions>>& received_list) {
  pthread_mutex_lock(&list_lock);
  
  for (int i = 0; i < std::min(received_list.size(), membership_list.size()); ++i) {
    assert(std::get<0>(received_list[i]) == std::get<0>(membership_list[i]));

    // timestamp equal, expect two tuple to be totally the same
    if (std::get<1>(received_list[i]) == std::get<1>(membership_list[i])) {
      continue;
    }

    std::time_t t1, t2;
    struct std::tm tm1 = {0}, tm2 = {0};
    std::istringstream ss1(std::get<1>(received_list[i]));
    std::istringstream ss2(std::get<1>(membership_list[i]));

    ss1 >> std::get_time(&tm1, "%a %b %d %H:%M:%S %Y");
    ss2 >> std::get_time(&tm2, "%a %b %d %H:%M:%S %Y");
    t1 = mktime(&tm1);
    t2 = mktime(&tm2);

    if (t1 > t2) {
      // only update if action is different
      if (std::get<2>(received_list[i]) == std::get<2>(membership_list[i])) {
        continue;
      }

      // be marked as FAILED or LEAVE while current machine still alive
      if (std::get<0>(received_list[i]) == machine_ip && std::get<2>(received_list[i]) == FAILED) {
        // get current timestamp
        std::time_t t = system_clock::to_time_t(system_clock::now());
        string time_str = std::ctime(&t); // this includes a '\n' at the end
        time_str.pop_back();
        membership_list[i] = {machine_ip, time_str, JOIN};
        continue;
      }
      // the received_list[i] is newer, update our membership list
      membership_list[i] = {std::get<0>(received_list[i]), std::get<1>(received_list[i]), std::get<2>(received_list[i])};
      // write to log file
      string log_info = actions_to_string(std::get<2>(received_list[i])) + " " + std::get<0>(received_list[i]) + " " + std::get<1>(received_list[i]) + "\n";
      pthread_mutex_lock(&file_lock);
      write(fileno(log_file), log_info.c_str(), log_info.size());
      pthread_mutex_unlock(&file_lock);
    }
  }

  if (membership_list.size() < received_list.size()) {
    for (int i = membership_list.size(); i < received_list.size(); ++i) {
      membership_list.push_back({std::get<0>(received_list[i]), std::get<1>(received_list[i]), std::get<2>(received_list[i])});
      // write to log file
      string log_info = actions_to_string(std::get<2>(received_list[i])) + " " + std::get<0>(received_list[i]) + " " + std::get<1>(received_list[i]) + "\n";
      pthread_mutex_lock(&file_lock);
      write(fileno(log_file), log_info.c_str(), log_info.size());
      pthread_mutex_unlock(&file_lock);
    }
    pthread_cond_broadcast(&list_cv);
  }

  pthread_mutex_unlock(&list_lock);
}

/**
 * Ping neighbours, detect failure and notify the `broadcast_list_change` thread
 * - each monitor process takes care of the p-th neighbour clockwise (1-indexed, RHS in the list or wrap around)
 */
void* failure_monitor(void* assigned_pos) {
  int p = *((int*) assigned_pos);
  free(assigned_pos);

  // ping ack here
  while (1) {
    // fprintf(stderr, "call in %d\n", p);

    // get target ip
    // maybe avoid PINGing failed machine
    pthread_mutex_lock(&list_lock);
    string target_ip = find_alive_target_ip(p);
    while (target_ip == "") {
      pthread_cond_wait(&list_cv, &list_lock);
      target_ip = find_alive_target_ip(p);
    }
    pthread_mutex_unlock(&list_lock);

    // fprintf(stderr, "target ip %s\n", target_ip.c_str());

    // ping target ip
    struct sockaddr_in serveraddr;
    memset((char *) &serveraddr, 0, sizeof(serveraddr));

    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(std::stoi(COMMUNICATION_PORT));
    serveraddr.sin_addr.s_addr = inet_addr(target_ip.c_str());

    sendto(sockfd, "PING", 4, 0, (struct sockaddr *) &serveraddr, sizeof(serveraddr));

    // sleep for a while to wait for response
    sleep(PING_ACK_TIMEOUT);

    // alive responded ip will be add to `ack_set` (handled by `ping_ack_update_listener`)
    // check if ack comes back
    pthread_mutex_lock(&ack_lock);
    int is_alive = (ack_set.find(target_ip) != ack_set.end());
    if (is_alive) {
      ack_set.erase(target_ip);
      update_ip_status(target_ip, JOIN);
    } else {
      update_ip_status(target_ip, FAILED);
    }
    pthread_mutex_unlock(&ack_lock);
  }
}

/**
 * Listen and respond to other machines' pings, acks, and membership list updates
 */
void* ping_ack_update_listener(void*) {
  while (1) {
    struct sockaddr_in clientaddr;
    unsigned int clientlen = sizeof(clientaddr);

    // fprintf(stderr, "listening to ping or updates\n");

    char buf[BUFSIZE] = {0};
    // expecting format: [action]\n[ip]\n[time]\n
    recvfrom(sockfd, buf, BUFSIZE, 0, (struct sockaddr *) &clientaddr, &clientlen);

    // check the request type: PING, ACK, JOIN or UPDATE, behave coorespondingly
    // if PING, send ACK
    // if ACK, add client ip to `ack_set`
    // if JOIN, add <ip, timestamp, "JOIN"> to membership list if ip is new 
    //    otherwise delete old one and pushback new one
    // if UPDATE, compare and update local membership list, notify `broadcast_list_change`
    //    thread by adding element to `updates`

    if (strstr(buf, "PING") - buf == 0) {
      // ====
      // PING
      // ====
      string message = "ACK\n" + machine_ip + "\n";
      sendto(sockfd, message.c_str(), message.size(), 0, (struct sockaddr *) &clientaddr, clientlen);

    } else if (strstr(buf, "ACK\n") - buf == 0) {
      // =====
      // ACK\n
      // IP\n
      // =====
      char* newline = strchr(buf + 4, '\n');
      *newline = '\0';
      pthread_mutex_lock(&ack_lock);
      ack_set.insert(string(buf + 4));
      pthread_mutex_unlock(&ack_lock);

    } else if (strstr(buf, "JOIN\n") - buf == 0) {  // only vm1 (introducer) will entire this block
      // ===========
      // JOIN\n
      // IP\n
      // timestamp\n
      // ===========
      char* newline1 = strchr(buf + 5, '\n');
      *newline1 = '\0';
      char* newline2 = strchr(newline1 + 1, '\n');
      *newline2 = '\0';
      char *ip = buf + 5, *time = newline1 + 1;
      new_process_join(string(ip), string(time));  // only introducer will send JOIN message

    } else if (strstr(buf, "UPDATE\n") - buf == 0) {
      // ====================
      // UPDATE\n
      // <ip1,t1,ACTION1>\n
      // <ip2,t2,ACTION2>\n
      // ...
      // ====================
      vector<tuple<string, string, actions>> received;
      string s = string(buf + 7);
      string delimiter = "\n";

      size_t pos = 0;
      while ((pos = s.find(delimiter)) != string::npos) {
        string tmp = s.substr(0, pos);
        char line[tmp.size() + 1];
        strcpy(line, tmp.c_str());
        char* comma1 = strchr(line, ',');
        *comma1 = '\0';
        char* comma2 = strchr(comma1 + 1, ',');
        *comma2 = '\0';

        char *ip = line, *timestamp = comma1 + 1, *action = comma2 + 1;
        actions act = string_to_actions(action);
        received.push_back({string(ip), string(timestamp), act});
        s.erase(0, pos + delimiter.length());
      }

      compare_and_update_memlist(received);

    } else {
      error("ERROR! action not found!\n");
    }
  }

  return NULL;
}

/**
 * Gossip full membership list to neighbors in at fixed period
 */
void* broadcast_list_updates(void*) {
  while (1) {
    pthread_mutex_lock(&list_lock);

    // send the full membership list to neighors
    string msg = "UPDATE\n";
    for (auto& tup : membership_list) {
      msg += (std::get<0>(tup) + "," + std::get<1>(tup) + "," + actions_to_string(std::get<2>(tup)) + "\n");
    }
    send_all_neighbor_msg(msg);

    pthread_mutex_unlock(&list_lock);

    sleep(BROADCAST_UPDATE_INTERVAL);
  }

  return NULL;
}

int main(int argc, char **argv) {
  if (argc != 1) {
    error("Usage: ./process");
  }

  bool havejoin = false;
  for (string line; std::getline(std::cin, line);) { 
    if (!havejoin) {
      if (line == "join") {
        /**
         * Initial Setup
         */
        // TCP connect to introducer first
        int introducer_fd = connect_to_host(INTRODUCER_IP, INTRODUCER_PORT);
        if (introducer_fd == -1) {
          error("ERROR connecting to introducer");
        }

        // read ip address of the new process and time from introducer
        char buffer[BUFFER_SIZE] = {0};
        // expecting format: [ip]\n[time]\n
        recv(introducer_fd, buffer, BUFFER_SIZE, 0);
        char* newline = strchr(buffer, '\n');
        *newline = '\0';
        // use the response from introducer to set self machine_ip
        machine_ip = string(buffer);

        /**
         * create the log file
         */
        log_file = fopen("vm.log", "w");

        /**
         * Open Communication
         */
        // open socket for furthur ping ack and failure detection
        sockfd = UDP_server(COMMUNICATION_PORT);

        // detect neighbor failure by periodically pinging neighbours
        for (size_t i = 1; i <= MONITOR_COUNT; ++i) {
          int* idx = (int*) malloc(sizeof(int));
          *idx = i;
          pthread_t tid;
          pthread_create(&tid, NULL, failure_monitor, (void*) idx);
          pthread_detach(tid);
        }

        pthread_t threads[2];
        // listen and respond to ping, ack, and membership updates
        pthread_create(&threads[0], NULL, ping_ack_update_listener, NULL);
        pthread_detach(threads[0]);
        // broadcast membership list updates when possible
        // share updates obtained by `failure_monitor` and `ping_ack_update_listener`
        pthread_create(&threads[1], NULL, broadcast_list_updates, NULL);
        pthread_detach(threads[1]);


        /**
         * Complete Setup
         */
        // tell introducer that new machine is ready for further communication
        send(introducer_fd, "CONFIRM", 7, 0);
        close(introducer_fd);

        havejoin = true;
      }

    } else {
      if (line == "list_self") {
        fprintf(stderr, "===== Self ID is %s =====\n", machine_ip.c_str());
      } 
      
      else if (line == "list_mem") {
        fprintf(stderr, "===== The membership list is as follows =====\n");
        pthread_mutex_lock(&list_lock);
        for (int i = 0; i < membership_list.size(); ++i) {
          if (std::get<2>(membership_list[i]) == JOIN) {
            fprintf(stderr, "%d. %s\n", i + 1, std::get<0>(membership_list[i]).c_str());
          }
        }
        pthread_mutex_unlock(&list_lock);
      } 

      else if (line == "neighbor") {
        cerr << "======== neighbors =========" << endl;
        pthread_mutex_lock(&list_lock);
        for (int i = 1; i <= MONITOR_COUNT; i++) {
          cerr << find_alive_target_ip(i) << endl;
        }
        pthread_mutex_unlock(&list_lock);
      }
      
      else if (line == "leave") {
        exit(0);
      }
    }
  }

  pthread_exit(NULL);
}
