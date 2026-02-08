# Execute Point Scripts

## execute_stepchain.sh

Runs a **single** StepChain job: given the request tarball and one `jobN.json`, it executes the full chain of steps (cmsRun 1 through N) on the machine where it is invoked. Intended to be used as the **job executable when submitting to the Grid**: transfer `request_psets.tar.gz` and one `jobN.json` per job, then call this script with those two arguments.

**Usage:**

```bash
execute_stepchain.sh <request_psets.tar.gz> <job_N.json>
```

- **request_psets.tar.gz** — Tarball with `request.json` and `PSets/` (produced by EventSplitter with `--psets`).
- **job_N.json** — Per-job file from EventSplitter (contains `job_index` and `tweaks` for each step).

The script extracts the tarball, reads the step count and step config from `request.json`, and for each step sets up the CMSSW release, applies the precomputed tweak from `jobN.json`, and runs cmsRun. Steps 2+ read input from the previous step’s output (e.g. `file:../step1/RAWSIMoutput.root`).

## stageout.py

Intended for use on the **worker node** to transfer files from the worker to the **site storage element**, using WMCore `StageOutMgr` and the site's storage config.

**Requirements:** `SITECONFIG_PATH` or `WMAGENT_SITE_CONFIG_OVERRIDE` must be set (e.g. to the site's SITECONF path, such as `/cvmfs/cms.cern.ch/SITECONF/T2_CH_CERN`).

**Usage:**

```bash
stageout.py --lfn /store/.../file.root --local ./output.root
```

Repeat `--lfn` and `--local` for multiple files (same order). Optional: `--retries` and `--retry-pause` (defaults: 3 and 600).

**Example** (e.g. in a Singularity/worker environment with `ep_scripts/submit_env.sh`):

```bash
export PYTHONPATH=/path/to/WMCore/src/python
export SITECONFIG_PATH=/cvmfs/cms.cern.ch/SITECONF/T2_CH_CERN
source ep_scripts/submit_env.sh
setup_local_env
setup_cmsset
setup_python_comp

ep_scripts/stageout.py --lfn /store/temp/user/you.abc123/WMCore.zip --local CHANGELOG.md
```