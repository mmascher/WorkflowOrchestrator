# Micro Agent and Micro Agent Monitor (MAM)

## Micro Agent

The **micro agent** is a single HTCondor job that runs on the schedd machine. It orchestrates the full workflow for one request:

1. **event_splitter** — Reads request.json, splitting.json, PSets/; produces job1..jobN.json and request_psets.tar.gz
2. **create_stepchain_jdl** — Builds stepchain.jdl (single JDL for all jobs, num_jobs from event_splitter)
3. **condor_submit** — Submits stepchain to HTCondor (Grid jobs)
4. **MAM** — Runs in daemon mode, tailing the condor user log

### run_micro_agent.sh

Wrapper script. Expects in CWD: request.json, splitting.json, PSets/, sitelist.txt. Creates event_splitter_out/, stepchain.jdl, submits, then starts MAM tailing log/micro_agent_monitor.\<Cluster\>.

### create_stepchain_jdl.py

- Input: event_splitter output dir, request.json, proxy, sitelist
- Output: stepchain.jdl with transfer_input_files, Queue N
- Derives num_jobs, cpus, memory, walltime, REQUIRED_OS from request

## Micro Agent Monitor (MAM)

MAM parses HTCondor user log files and records file-level processing info in SQLite.

### Role

- **Input**: Condor user log (e.g. log/run.\<Cluster\>)
- **Events**: Parses ULOG_JOB_TERMINATED (5); on success, reads framework job report JSON
- **Output**: SQLite DB with file-centric records (LFN, PFN, PNN, step, etc.)
- **Filtering**: Only stores outputs from steps with `KeepOutput==True` (from request.json)

### Modes

- **Daemon** (default): Tails log continuously, full re-read every hour
- **--once**: Single pass over log, then exit

### CondorLogParser

- Parses event lines: `NNN (Cluster.Proc.Subproc) YYYY-MM-DD HH:MM:SS message`
- Extracts job report path from JOB_TERMINATED event
- Reads JSON, extracts output file info, builds LFNs via `build_lfn_for_file` (utils.py)

### SQLite Schema

- `processed_files` — File-centric: request_name, step, lfn, pfn, pnn, etc.

## execute_stepchain.sh (Worker)

Runs on Grid worker nodes. One invocation per job:

- Input: request_psets.tar.gz, jobN.json
- Unpacks tarball, runs cmsRun for each step with precomputed tweaks
- Steps 2+ chain from previous step output (file:../stepN/Output.root)
- After last step: stage_out, create_report (aggregate FrameworkJobReport)
- Output: prmon, report JSON transferred back

## stage_out.py

Transfers output files from worker to site storage element using WMCore StageOutMgr. Used by execute_stepchain.sh after the last step.

## create_report.py

Aggregates FrameworkJobReport XML from stepchain cmsRun into single JSON. Merges stageout results (PFN, PNN) into output file records.
