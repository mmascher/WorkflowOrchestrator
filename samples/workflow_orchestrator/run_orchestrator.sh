#!/bin/bash
# Example: run the Workflow Orchestrator daemon
#
# Prerequisites:#   - Valid x509 proxy for ReqMgr and config cache
#   - htcondor2 (pip install htcondor)
#   - config/orchestrator.yaml (or --config path)
# Usage:
#   ./run_orchestrator.sh
#   ./run_orchestrator.sh --config /path/to/config.yaml -v

set -e
WO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PYTHONPATH="${WO_DIR}/src/python:${PYTHONPATH:-}:${WO_DIR}/samples/htcondor/WMCore.zip"
# Suppress SyntaxWarnings from WMCore Lexicon regex patterns (invalid escape sequences)
export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore::SyntaxWarning}"

cd "$WO_DIR"
python3 -m workflow_orchestrator.orchestrator "$@"
