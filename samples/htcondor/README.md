# HTCondor Sample Submission

Sample HTCondor JDL and wrapper script for submitting **one job per site** (not the full simulation). This is useful for validating that the StepChain execution works across the Grid before launching the complete set of jobs.

The JDL iterates over the sites listed in `sitelist.txt` and submits one job to each, assigning `Process` values from 0 to NSites-1.

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

1. **`execute_stepchain.sh`** and **`submit_env.sh`** from `ep_scripts/`:

   ```bash
   cp ../../ep_scripts/execute_stepchain.sh .
   cp ../../ep_scripts/submit_env.sh .
   ```

2. **EventSplitter output** — place it in an `event_splitter_out/` subdirectory. This must contain:
   - `job0.json`, `job1.json`, …, `job<NSites-1>.json` (one per site)
   - `request_psets.tar.gz`

   See the [EventSplitter README](../../src/python/JobSplitters/README.md) for how to produce these files.

The final layout should look like:

```text
htcondor/
├── execute_stepchain.sh
├── submit_env.sh
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

This queues one job per site in `sitelist.txt`. Each job transfers `execute_stepchain.sh`, `submit_env.sh`, the corresponding `job$(Process).json`, and `request_psets.tar.gz` to the worker node, runs the StepChain, and transfers the output tarball back into `results/`.
