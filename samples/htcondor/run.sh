#!/bin/bash

./execute_stepchain.sh request_psets.tar.gz job*.json &
PID=$!
prmon --pid $PID --filename prmon.txt --json-summary prmon.json
