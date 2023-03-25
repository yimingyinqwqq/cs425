all: server client coordinator

server: server.cpp common.cpp
	g++ -g -std=c++11 server.cpp -o server -lpthread

client: client.cpp common.cpp
	g++ -g -std=c++11 client.cpp -o client -lpthread

coordinator: coordinator.cpp common.cpp
	g++ -g -std=c++11 coordinator.cpp -o coordinator -lpthread

test: test.cpp
	g++ -g -std=c++11 test.cpp -o test -lpthread

clean:
	rm server client coordinator test

.PHONY: all clean
