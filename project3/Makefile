all: node dns

node: node.h node.cpp process.cpp introducer.cpp common.cpp master.cpp sdfsprocess.cpp
	g++ -g -std=c++11 node.cpp process.cpp introducer.cpp common.cpp master.cpp sdfsprocess.cpp -o node -lpthread

dns: dns.cpp
	g++ -g -std=c++11 dns.cpp -o dns -lpthread

data:
	wget https://www.cs.upc.edu/~nlp/wikicorpus/raw.en.tgz -O wikicorpus.txt

clean:
	rm node dns

.PHONY: all clean
