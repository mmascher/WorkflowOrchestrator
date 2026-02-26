#!/bin/bash
# Run the Micro Agent Monitor (MAM) on a condor submission.
#
# Usage:
#   ./run_monitor.sh [WORK_DIR]
#
# If WORK_DIR is not given, uses the directory containing this script.
# WORK_DIR should contain log/ and results/ (e.g. test_jdl from run_test.sh).
#
# For a one-shot run on existing log/results:
#   python -m micro_agent.micro_agent_monitor --log samples/micro_agent/run.10372180 \
#     --results-dir /path/to/results --db micro_agent.db --once
#
# For daemon mode (tail log, process new events):
#   cd test_jdl && ./run_monitor.sh

set -e
WO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="${1:-$WO_DIR/samples/micro_agent}"
cd "$WORK_DIR"

if [ ! -d log ] || [ ! -d results ]; then
  echo "Error: $WORK_DIR must contain log/ and results/ directories."
  echo "Run from test_jdl after run_test.sh, or point to a dir with log and results."
  exit 1
fi

LOG=$(ls -t log/run.* 2>/dev/null | head -1)
if [ -z "$LOG" ]; then
  echo "Error: No log files (log/run.*) found in $WORK_DIR"
  exit 1
fi

export PYTHONPATH="$WO_DIR/src/python:$PYTHONPATH"
exec python -m micro_agent.micro_agent_monitor \
  --log "$LOG" \
  --results-dir results \
  --db micro_agent.db \
  "$@"
