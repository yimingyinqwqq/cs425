#!/bin/bash
mkdir testfiles

for i in 1 2 3 4 5 6 7 8 9 10
do
    dd if=/dev/urandom of=testfiles/testfile$i bs=1048576 count=2
    perl -i -pe 's/\0/1/g' testfiles/*
done