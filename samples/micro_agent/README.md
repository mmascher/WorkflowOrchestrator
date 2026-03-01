# Micro Agent Samples

This directory contains samples and scripts for the Micro Agent workflow.

## Overview

1. **event_splitter** + **create_stepchain_jdl** → produces JDL
2. **condor_submit** → runs jobs on HTCondor
3. **Micro Agent Monitor (MAM)** → watches job log, stores file info in SQLite

## Files

- `run.10372180` – Sample job log file (condor user log format).
- `job_report.10409446.0..json` – Sample framework job report (cluster 10409446, proc 0).

## Micro Agent Monitor (MAM)

MAM parses the condor user log (JDL `Log` macro, e.g. `log/run.<Cluster>`) and:
- Detects `JOB_TERMINATED` events.
- For each successful job, reads `results/job_report.<Cluster>.<Proc>.<N>.json`.
- Extracts file info (input/output) from the framework job report.
- Stores it in a local SQLite database (file-centric, not job-centric).

### Usage

`--request request.json` is required. Only outputs from steps with `KeepOutput==True` are stored.

```bash
# Single pass over existing log (e.g. after jobs finished)
python -m micro_agent.micro_agent_monitor \
  --log samples/micro_agent/run.10372180 \
  --results-dir /path/to/results \
  --db micro_agent.db \
  --request request.json \
  --once

# Daemon mode (from test_jdl after run_test.sh; use your condor cluster ID for the log path)
cd test_jdl
PYTHONPATH=/path/to/WorkflowOrchestrator/src/python python -m micro_agent.micro_agent_monitor \
  --log log/run.10372180 --results-dir results --db micro_agent.db --request request.json

# Logging: save to file (DEBUG) and/or verbose stdout
python -m micro_agent.micro_agent_monitor --log ... --request request.json --log-file micro_agent.log
python -m micro_agent.micro_agent_monitor --log ... --request request.json -v   # DEBUG on stdout
```

### SQLite Schema

- **processed_files**: condor_job_id, lfn, pfn, step_name, events, size, glidein_cmssite (execution site), pnn (storage site), job_exit_code, etc.

MAM stores only output files (no inputs). Only outputs from steps with `KeepOutput==True` (from `--request request.json`) are stored.

### Query

```bash
sqlite3 micro_agent.db "SELECT * FROM processed_files LIMIT 10;"
```

### Tests

```bash
python3 -m unittest tests.test_micro_agent_monitor -v
```
