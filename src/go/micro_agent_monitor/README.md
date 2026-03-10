# Micro Agent Monitor (MAM) - Go Implementation

Go rewrite of the Micro Agent Monitor for experimentation and comparison with the Python version.

## Overview

Same functionality as the Python `micro_agent_monitor.py`:
- Parses HTCondor user log files for `JOB_TERMINATED` events
- Reads framework `job_report.<Cluster>.<Proc>.<N>.json` files
- Extracts output file info (only from steps with `KeepOutput==True`)
- Stores in SQLite database (same schema as Python version)

## Build

Requires Go 1.21+ and CGO (for SQLite):

```bash
cd src/go/micro_agent_monitor
go mod tidy
go build -o micro_agent_monitor .
```

## Usage

Same CLI as the Python version:

```bash
# Single pass over existing log
./micro_agent_monitor \
  --log samples/micro_agent/run.10372180 \
  --results-dir /path/to/results \
  --db micro_agent.db \
  --request request.json \
  --once

# Daemon mode (tails log continuously)
./micro_agent_monitor \
  --log log/run.10372180 \
  --results-dir results \
  --db micro_agent.db \
  --request request.json

# Options
--log-file FILE   Write log to file
--verbose         Verbose output
--poll N          Poll interval in seconds (default: 10)
```

## Switching Between Python and Go

**Local run** (`run_micro_agent.sh`):
```bash
MAM_IMPL=go ./ep_scripts/run_micro_agent.sh
```

**Workflow Orchestrator** (micro agent on scheduler): add to `config/orchestrator.yaml`:
```yaml
mam_impl: "go"
```
The Go binary is copied to the job sandbox when built. Build before submitting:
```bash
make -C src/go/micro_agent_monitor build
```

Or build and run manually:

```bash
# From WorkflowOrchestrator root
./src/go/micro_agent_monitor/micro_agent_monitor \
  --log log/micro_agent_monitor.$CLUSTER_ID \
  --results-dir results \
  --db micro_agent.db \
  --request request.json
```

## Database Compatibility

The Go version uses the same SQLite schema as the Python version. Databases created by one can be read by the other.

## Testing

The Python tests validate the logic; the Go version produces equivalent results:

```bash
# Python tests (existing)
python3 -m unittest tests.test_micro_agent_monitor -v

# Go: test with sample job_report (cluster 10409446)
# Note: samples/micro_agent/run.10372180 has different cluster IDs; use a log with
# JOB_TERMINATED for 10409446.0, or run with your own log and results.
./micro_agent_monitor \
  --log /path/to/condor.log \
  --results-dir samples/micro_agent \
  --db /tmp/test_go.db \
  --request ../samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json \
  --once
```
