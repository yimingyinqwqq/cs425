# CS425-MP1

This project implements distributed grep command, where a client can grep a specific pattern on multiple servers.

# contributors
zhiheng hua
yiming yin

## Getting started
Type "make" in the terminal to make all targets. Then for server usage, use "./server", for coordinator usage, use "./coordinator" (often we assign VM01 as the coordinator, so modify COORDINATOR_HOST in client.cpp if you want a difference VM to be the coordinator), and for client usage, use "./client grep [OPTIONS] PATTERN" (e.g. "./client grep -R www.hicks"). IMPORTANT: To save time from outputting in stdout, instead of checking the output from stdout, we decide to store the output in a file called "response.txt" on the VM where client or test has been run.

For testing purposes, type "make test" in the terminal. Use "./test" to check whether all tests in test.cpp have passed. The folder desired_output is used in the test.cpp to verify whether our program runs as intended. For those tests, only all first five VMs should run "./server" in order to simulate failures on the last five machines. Alternatively you can use Control-C on the last five machines, given it has be done quick enough. No clients should be run, since the test cases will call "./client ...".
