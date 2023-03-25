/* 
** test.cpp -- unit tests
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <assert.h>

#include <algorithm>
#include <numeric>
#include <string>

using std::string;

void test_case_generator(int num, string grep_pattern) {
    assert(num >= 0 && num <= 6);

    string grep_cmd = string("./client grep ") + grep_pattern;
    system(grep_cmd.c_str());

    // create string and file for "diff". Note -w -B is added to remove differences for blank lines and white spaces
    string cmd = string("diff -w -B desired_output/test") + std::to_string(num) + "_desired_output.txt response.txt";

    int fd_copy = dup(1);                    // copy stdout
    close(1);                                // close stdout
    FILE* f = fopen("diff_output", "w+");    // open a temporary file to save output

    system(cmd.c_str());

    rewind(f);

    char buffer[10] = {0};

    // compare two files: one from grep, and one from the desired output
    if (read(fileno(f), buffer, 1) == 0) {
        fprintf(stderr, "TEST %d PASSED!\n", num);
    } else {
        fprintf(stderr, "TEST %d FAILED!\n", num);
    }

    unlink("diff_output");
    dup2(fd_copy, 1);                        // copy stdout back
}


int main(int argc, char *argv[]) {
    //
    // ---------- TEST 1: check a frequent pattern ----------
    //
    
    test_case_generator(1, "POST");

    //
    // ---------- TEST 2: check a less frequent pattern ----------
    //
    
    test_case_generator(2, "235");

    //
    // ---------- TEST 3: check a infrequent pattern ----------
    //

    test_case_generator(3, "http://www.hicks.com");

    //
    // ---------- TEST 4: check a regular expression ----------
    //
    
    test_case_generator(4, ".9:12:4[0-6]");

    //
    // ---------- TEST 5: check a regular expression with opions ----------
    //
    
    test_case_generator(5, "-n -C 2 .9:12:4[0-6]");
    
    return 0;
}