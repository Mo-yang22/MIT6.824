#!/usr/bin/env bash

for ((i=1; i<=10; i++))
do
    echo "Running test $i"
    go test -run 2A
done
