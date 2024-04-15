#!/bin/bash

# need to run this command below
# chmod +x stop.sh

# Stop the coordinator
pkill -f "./coordinator"

# Stop tsd processes
pkill -f "./tsd"
