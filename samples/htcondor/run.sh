#!/bin/bash
# Meke sure there is something to transfer back
touch prmon.txt prmon.json

# Run stepchain in background so we can monitor it with prmon
./execute_stepchain.sh request_psets.tar.gz job*.json &
PID=$!

# Profile memory usage; || true so prmon failure (e.g. not found) does not abort the job
source submit_env.sh
setup_cmsset
prmon --pid $PID --filename prmon.txt --json-summary prmon.json || true

# Wait for stepchain and exit with its status (wait returns immediately if process already exited)
wait $PID
exit $?
