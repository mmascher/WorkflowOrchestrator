# Execute Point Scripts

## execute_stepchain.sh

Runs a **single** StepChain job: given the request tarball and one `jobN.json`, it executes the full chain of steps (cmsRun 1 through N) on the machine where it is invoked. Intended to be used as the **job executable when submitting to the Grid**: transfer `request_psets.tar.gz` and one `jobN.json` per job, then call this script with those two arguments.

**Usage:**

```bash
execute_stepchain.sh <request_psets.tar.gz> <job_N.json>
```

- **request_psets.tar.gz** — Tarball with `request.json` and `PSets/` (produced by event_splitter with `--psets`).
- **job_N.json** — Per-job file from event_splitter (contains `job_index` and `tweaks` for each step).

The script extracts the tarball, reads the step count and step config from `request.json`, and for each step sets up the CMSSW release, applies the precomputed tweak from `jobN.json`, and runs cmsRun. Steps 2+ read input from the previous step’s output (e.g. `file:../step1/RAWSIMoutput.root`).

## stage_out.py

Intended for use on the **worker node** to transfer files from the worker to the **site storage element**, using WMCore `StageOutMgr` and the site's storage config. Can be tested locally with the `--lfn` and `--local` options.

**Requirements:** `SITECONFIG_PATH` or `WMAGENT_SITE_CONFIG_OVERRIDE` must be set (e.g. to the site's SITECONF path, such as `/cvmfs/cms.cern.ch/SITECONF/T2_CH_CERN`).

**Usage:**

```bash
stage_out.py --lfn /store/.../file.root --local ./output.root
```

Repeat `--lfn` and `--local` for multiple files (same order). Optional: `--retries` and `--retry-pause` (defaults: 3 and 600). There is also a `--request` / `--work-dir` mode that discovers files to stage from a stepchain request (used internally by `execute_stepchain.sh`).

**Example** (e.g. on lxplus from the WorkflowOrchestrator root):

```bash
cmssw-cc7
export PYTHONPATH=/path/to/WMCore/src/python
export SITECONFIG_PATH=/cvmfs/cms.cern.ch/SITECONF/T2_CH_CERN
source ep_scripts/submit_env.sh
setup_local_env
setup_cmsset
setup_python_comp

ep_scripts/stage_out.py --lfn /store/temp/user/you.abc123/destfile --local sourcefile
```

## DAG generator (submit-time)

**create_stepchain_dag.py** (in `src/python/micro_agent/`) generates HTCondor DAG and submit files from event_splitter output. See [samples/htcondor/README.md](../samples/htcondor/README.md) for the full DAG workflow and usage.

**postjob.py** — DAG POST script run after each node job completes. Placeholder for user logic. Default behavior: exits 1 on first invocation (triggers 6h DEFER retry), exits 0 on second. Receives `$JOB` (node name) as first argument. Copied to the DAG directory by `create_stepchain_dag.py`.