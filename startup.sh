#!/bin/bash

# need to run this command below
# chmod +x startup.sh

# Start the coordinator
./coordinator -p 9090 &

sleep 2

# Start the tsd processes
./tsd -c 1 -s 1 -h localhost -k 9090 -p 10000 &
./tsd -c 2 -s 1 -h localhost -k 9090 -p 10001 &
./tsd -c 3 -s 1 -h localhost -k 9090 -p 10002 &


