/* 
** client.cpp -- code to run on the querying machine
*/

#include "common.cpp"
#include <string>

#define COORDINATOR_HOST "172.22.94.58" // VM 01
#define COORDINATOR_PORT "8000"

using std::string;


// ./client grep [OPTIONS] PATTERN
int main(int argc, char *argv[])
{
	if (argc < 3) {
    fprintf(stderr, "Usage: ./client grep [OPTIONS] PATTERN\n");
		fprintf(stderr, "Use '\\' to escape quotation marks\n");
		fprintf(stderr, "For example: ./client grep \\'^Hello\\'\n");
    exit(1);
	}

	int sockfd;  
	struct addrinfo hints, *servinfo, *p;
	int rv;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;				// IPv4
	hints.ai_socktype = SOCK_STREAM;	// TCP

	if ((rv = getaddrinfo(COORDINATOR_HOST, COORDINATOR_PORT, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		exit(1);
	}

	// loop through all the results and connect to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
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
		fprintf(stderr, "client: failed to connect\n");
		exit(1);
	}

	freeaddrinfo(servinfo); // all done with this structure

  // send request
	string input = "";
	for (int i = 1; i < argc; i++) {
		input += argv[i]; input += " ";
	}

	if (write_all_to_socket(sockfd, input.c_str(), input.size()) == -1) {
		fprintf(stderr, "client: failed to send request\n");
		exit(1);
	}

	// fprintf(stderr, "message sent: [%s]\n", input.c_str());
	shutdown(sockfd, SHUT_WR);

	// pipe response to a file
	FILE* response_file = fopen("response.txt", "w+");

	size_t numbytes;
	// size_t total_bytes = 0;
  char buf[4096] = {0};
	while ((numbytes = read_all_from_socket(sockfd, buf, 4096)) != 0) {
		write(fileno(response_file), buf, numbytes);
		memset(buf, 0, numbytes);
		// total_bytes += numbytes;
	}

	// fprintf(stderr, "read %zd bytes from coordinator\n", total_bytes);

	fclose(response_file);
	shutdown(sockfd, SHUT_RD);
	close(sockfd);

	return 0;
}

