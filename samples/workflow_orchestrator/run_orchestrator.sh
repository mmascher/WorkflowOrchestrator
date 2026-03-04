#!/bin/bash
# Example: run the Workflow Orchestrator daemon
#
# Prerequisites:
#   - WMCore in PYTHONPATH (or install)
#   - Valid x509 proxy for ReqMgr and config cache
#   - htcondor2 (pip install htcondor)
#   - config/orchestrator.yaml (or --config path)
#
# Usage:
#   ./run_orchestrator.sh
#   ./run_orchestrator.sh --config /path/to/config.yaml -v

set -e
WO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PYTHONPATH="${WO_DIR}/src/python:${WO_DIR}/samples/htcondor/WMCore.zip:${PYTHONPATH:-}"

cd "$WO_DIR"
python3 -m workflow_orchestrator.orchestrator "$@"
