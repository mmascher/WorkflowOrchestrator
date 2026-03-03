# HTCondor Sample Submission

Sample HTCondor JDL and wrapper script for submitting StepChain jobs. Two submission modes are provided:

1. **One job per site** (`job.jdl`) — useful for validating StepChain execution across the Grid. Iterates over sites in `sitelist.txt`. Uses `job1.json` for all sites (same job run on each site for validation).
2. **JDL workflow (Queue N)** — Single JDL with `Queue from seq 1 N`; one Condor job per event_splitter job. Uses `job$(Index).json` (1-based). Simpler than DAGMan.

## Directory Setup

Before submitting, create the required output directories:

```bash
mkdir test_sites
cd test_sites
mkdir -p log out err results
```

| Directory | Purpose |
|-----------|---------|
| `log/`    | HTCondor log files |
| `out/`    | Job stdout |
| `err/`    | Job stderr |
| `results/`| Transferred-back output tarballs, job reports, prmon memory profiles |

## Input Files

Copy the necessary files into this directory:

1. **scripts from the repo**:

   ```bash
   WO_DIR=<path_to_WorkflowOrchestrator>
   cp "$WO_DIR/ep_scripts/execute_stepchain.sh" .
   cp "$WO_DIR/ep_scripts/submit_env.sh" .
   cp "$WO_DIR/ep_scripts/stage_out.py" .
   cp "$WO_DIR/ep_scripts/create_report.py" .
   cp "$WO_DIR/src/python/micro_agent/utils.py" .
   cp "$WO_DIR/samples/htcondor/WMCore.zip" .

   cp "$WO_DIR/samples/htcondor/job.jdl" .
   cp "$WO_DIR/samples/htcondor/run.sh" .
   cp "$WO_DIR/samples/htcondor/sitelist.txt" .

   ```

2. **event_splitter output** — place it in an `event_splitter_out/` subdirectory:
```
export PYTHONPATH=`pwd`/WMCore.zip
$WO_DIR/src/python/job_splitters/event_splitter.py   --request $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json   --splitting $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/splitting.json   --psets $WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/PSets/   --output-dir event_splitter_out
Generated 18073 jobs: job1..job18073.json, request_psets.tar.gz in event_splitter_out
```

See the [event_splitter README](../../src/python/job_splitters/README.md) for more details about the splitter.

The final layout should look like:

```text
htcondor/
├── execute_stepchain.sh
├── submit_env.sh
├── stage_out.py
├── create_report.py
├── utils.py
├── WMCore.zip
├── event_splitter_out/
│   ├── job1.json
│   ├── job2.json
│   ├── ...
│   └── request_psets.tar.gz
├── log/
├── out/
├── err/
├── results/
├── job.jdl
├── run.sh
└── sitelist.txt
```

## Submission

### One job per site

```bash
condor_submit job.jdl
```

This queues one job per site in `sitelist.txt`. Each job transfers `execute_stepchain.sh`, `submit_env.sh`, `stage_out.py`, `WMCore.zip`, the corresponding `job$(Process).json`, and `request_psets.tar.gz` to the worker node, runs the StepChain, and transfers the output tarball back into `results/`.

### JDL workflow (Queue N)

Single JDL with `Queue from seq 1 N`; derives num_jobs, request_cpus, Memory, walltime from request.json (avoids listdir on Ceph). Uses $(Process) for output paths.

1. **Create JDL** (proxy, sitelist, and request.json required). Run this on the submit machine before submitting; no WMCore needed:

   ```bash
   "$WO_DIR/src/python/micro_agent/create_stepchain_jdl.py" \
     --event-splitter-dir event_splitter_out/ \
     --request "$WO_DIR/samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json" \
     --proxy /tmp/x509up_u$(id -u) \
     --sitelist sitelist.txt
   ```

   num_jobs, request_cpus, Memory, and walltime are derived from request.json (TotalEstimatedJobs, Multicore, Memory, TimePerEvent, Step1.EventsPerJob).

   This generates `stepchain.jdl`.

2. **Submit the JDL:**

   ```bash
   condor_submit stepchain.jdl
   ```

**Retry behavior:** If `run.sh` fails (e.g. CVMFS or site issues), the job is retried on a different machine (up to 3 times by default). Use `--max-retries` to change the job-level retry count.
