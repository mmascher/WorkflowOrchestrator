# HTCondor Sample Submission

Sample HTCondor JDL and wrapper script for submitting **one job per site** (not the full simulation). This is useful for validating that the StepChain execution works across the Grid.

The JDL iterates over the sites listed in `sitelist.txt` and submits one job to each.

## Directory Setup

Before submitting, create the required output directories:

```bash
mkdir -p log out err results
```

| Directory | Purpose |
|-----------|---------|
| `log/`    | HTCondor log files |
| `out/`    | Job stdout |
| `err/`    | Job stderr |
| `results/`| Transferred-back output tarballs |

## Input Files

Copy the following files into this directory:

1. **`execute_stepchain.sh`**, **`submit_env.sh`**, and **`stage_out.py`** from `ep_scripts/`:

   ```bash
   cp ../../ep_scripts/execute_stepchain.sh .
   cp ../../ep_scripts/submit_env.sh .
   cp ../../ep_scripts/stage_out.py .
   ```

2. **`WMCore.zip`** — Pre-packaged WMCore libraries for the worker. Must be present in this directory (provided in the repo).

3. **event_splitter output** — place it in an `event_splitter_out/` subdirectory. This must contain:
   - `job0.json`, `job1.json`, …, `job<NSites-1>.json` (one per site)
   - `request_psets.tar.gz`

   See the [event_splitter README](../../src/python/job_splitters/README.md) for how to produce these files.

The final layout should look like:

```text
htcondor/
├── execute_stepchain.sh
├── submit_env.sh
├── stage_out.py
├── WMCore.zip
├── event_splitter_out/
│   ├── job0.json
│   ├── job1.json
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

```bash
condor_submit job.jdl
```

This queues one job per site in `sitelist.txt`. Each job transfers `execute_stepchain.sh`, `submit_env.sh`, `stage_out.py`, `WMCore.zip`, the corresponding `job$(Process).json`, and `request_psets.tar.gz` to the worker node, runs the StepChain, and transfers the output tarball back into `results/`.
