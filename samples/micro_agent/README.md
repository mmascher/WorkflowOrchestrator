# Micro Agent Samples

This directory contains samples and scripts for the Micro Agent workflow.

## Overview

1. **event_splitter** + **create_stepchain_jdl** → produces JDL
2. **condor_submit** → runs jobs on HTCondor
3. **Micro Agent Monitor (MAM)** → watches job log, stores file info in SQLite

## Files

- `run_test.sh` – Full workflow: split, create JDL, submit. Run from repo root.
- `run_monitor.sh` – Run MAM on a work dir (log/ + results/).
- `run.10372180` – Sample job log file (condor user log format).
- `job_report.10409446.0..json` – Sample framework job report (cluster 10409446, proc 0).

## Micro Agent Monitor (MAM)

MAM parses the condor user log (JDL `Log` macro, e.g. `log/run.<Cluster>`) and:
- Detects `JOB_TERMINATED` events.
- For each successful job, reads `results/job_report.<Cluster>.<Proc>.<N>.json`.
- Extracts file info (input/output) from the framework job report.
- Stores it in a local SQLite database (file-centric, not job-centric).

### Usage

```bash
# Single pass over existing log (e.g. after jobs finished)
python -m micro_agent.micro_agent_monitor \
  --log samples/micro_agent/run.10372180 \
  --results-dir /path/to/results \
  --db micro_agent.db \
  --once

# Daemon mode (from test_jdl after run_test.sh)
cd test_jdl && ../samples/micro_agent/run_monitor.sh .
# run_monitor.sh discovers log/run.* and passes --log
```

### SQLite Schema

- **processed_files**: condor_job_id, lfn, pfn, step_name, role (input/output), events, size, rse (storage site), job_exit_code, etc.

### Query

```bash
sqlite3 micro_agent.db "SELECT * FROM processed_files LIMIT 10;"
```

### Tests

```bash
python3 -m unittest tests.test_micro_agent_monitor -v
```
