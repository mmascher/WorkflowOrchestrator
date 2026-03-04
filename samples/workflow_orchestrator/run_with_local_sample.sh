#!/bin/bash
# Test the workflow orchestrator flow using LOCAL samples (no ReqMgr).
# Fetches request/splitting/PSets from local cmsunified_task sample,
# then optionally submits a micro agent.
#
# Use this to verify the pipeline without ReqMgr access.
# PSets are used from the sample (skip config cache fetch).

set -e
WO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SAMPLE="$WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792"
WORK_DIR="$WO_DIR/work/test_local_$(date +%s)"
export PYTHONPATH="${WO_DIR}/src/python:${WO_DIR}/samples/htcondor/WMCore.zip:${PYTHONPATH:-}"

mkdir -p "$WORK_DIR"
cp "$SAMPLE/request.json" "$WORK_DIR/"
cp "$SAMPLE/splitting.json" "$WORK_DIR/"
cp -r "$SAMPLE/PSets" "$WORK_DIR/"
cp "$WO_DIR/samples/htcondor/sitelist.txt" "$WORK_DIR/"

echo "Work dir: $WORK_DIR"
echo "Running event_splitter..."
python3 "$WO_DIR/src/python/job_splitters/event_splitter.py" \
  --request "$WORK_DIR/request.json" \
  --splitting "$WORK_DIR/splitting.json" \
  --output-dir "$WORK_DIR/event_splitter_out" \
  --psets "$WORK_DIR/PSets"

echo "Running create_stepchain_jdl..."
python3 -m micro_agent.create_stepchain_jdl \
  --event-splitter-dir "$WORK_DIR/event_splitter_out/" \
  --request "$WORK_DIR/request.json" \
  --proxy "${X509_USER_PROXY:-/tmp/x509up_u$(id -u)}" \
  --sitelist "$WORK_DIR/sitelist.txt" \
  --output-jdl "$WORK_DIR/stepchain.jdl"

echo "Generated $WORK_DIR/stepchain.jdl"
echo "To submit: condor_submit $WORK_DIR/stepchain.jdl"
echo "Or run micro agent: cd $WORK_DIR && $WO_DIR/ep_scripts/run_micro_agent.sh $WO_DIR"
