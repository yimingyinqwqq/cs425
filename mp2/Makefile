all: process introducer

process: process.cpp common.cpp
	g++ -g -std=c++11 process.cpp -o process -lpthread

introducer: introducer.cpp common.cpp
	g++ -g -std=c++11 introducer.cpp -o introducer

clean:
	rm process introducer

.PHONY: all clean
