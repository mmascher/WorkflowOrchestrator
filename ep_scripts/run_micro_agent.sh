#!/bin/bash
# Micro agent wrapper: event_splitter -> create_stepchain_jdl -> condor_submit -> MAM (daemon mode)
#
# Expects in CWD: request.json, splitting.json, PSets/, sitelist.txt
# Uses X509_USER_PROXY for proxy. Creates event_splitter_out/, stepchain.jdl, then submits.
# MAM runs in daemon mode (no --once) tailing the condor log.
#
# Usage: run_micro_agent.sh [WO_DIR]
# WO_DIR: WorkflowOrchestrator repo root (default: parent of ep_scripts)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WO_DIR="${1:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# WMCore.zip: in job sandbox at WO_DIR (copied by submitter)
WMCORE_ZIP="${WO_DIR}/WMCore.zip"
export PYTHONPATH="${WO_DIR}/src/python:${WMCORE_ZIP}:${PYTHONPATH:-}"

PROXY="${X509_USER_PROXY:-/tmp/x509up_u$(id -u)}"
REQUEST_JSON="request.json"
SPLITTING_JSON="splitting.json"
PSETS_DIR="PSets"
SITELIST="sitelist.txt"
EVENT_SPLITTER_OUT="event_splitter_out"
STEPCHAIN_JDL="stepchain.jdl"

mkdir -p log out err results

# 1. Event splitting
python3 "$WO_DIR/src/python/job_splitters/event_splitter.py" \
  --request "$REQUEST_JSON" \
  --splitting "$SPLITTING_JSON" \
  --output-dir "$EVENT_SPLITTER_OUT" \
  --psets "$PSETS_DIR"

# 2. Create JDL
python3 -m micro_agent.create_stepchain_jdl \
  --event-splitter-dir "$EVENT_SPLITTER_OUT/" \
  --request "$REQUEST_JSON" \
  --proxy "$PROXY" \
  --sitelist "$SITELIST" \
  --output-jdl "$STEPCHAIN_JDL"

# 3. Submit stepchain (parse cluster ID from condor_submit output for MAM log path)
SUBMIT_OUT=$(condor_submit "$STEPCHAIN_JDL" 2>&1) || { echo "Failed to submit stepchain.jdl:"; echo "$SUBMIT_OUT"; exit 1; }
CLUSTER_ID=$(echo "$SUBMIT_OUT" | sed -n 's/.*submitted to cluster \([0-9]*\).*/\1/p')
if [ -z "$CLUSTER_ID" ]; then
  echo "Could not parse cluster ID from condor_submit output"
  echo "$SUBMIT_OUT"
  exit 1
fi
echo "Submitted stepchain, cluster $CLUSTER_ID"

# 4. MAM (daemon mode - tails log continuously, no --once)
# Use MAM_IMPL=go to run the Go version instead of Python
LOG_FILE="log/micro_agent_monitor.$CLUSTER_ID"
if [ "${MAM_IMPL}" = "go" ] && [ -x "$WO_DIR/src/go/micro_agent_monitor/micro_agent_monitor" ]; then
  "$WO_DIR/src/go/micro_agent_monitor/micro_agent_monitor" \
    --log "$LOG_FILE" \
    --results-dir results \
    --db micro_agent.db \
    --request "$REQUEST_JSON"
else
  python3 -m micro_agent.micro_agent_monitor \
    --log "$LOG_FILE" \
    --results-dir results \
    --db micro_agent.db \
    --request "$REQUEST_JSON"
fi
