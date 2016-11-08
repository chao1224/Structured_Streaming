#!/usr/bin/env bash

#wget http://pages.cs.wisc.edu/~akella/CS838/F16/assignment2/split-dataset.tar.gz
#tar -xvf split-dataset.tar.gz
cd split-dataset

for k in $( seq 1 1127 )
do
    hadoop fs -put $k.csv /tweets/
    echo $k.csv
    sleep 5
done