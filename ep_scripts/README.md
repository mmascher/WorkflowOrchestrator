# Scripts

## execute_stepchain.sh

Runs a **single** StepChain job: given the request tarball and one `jobN.json`, it executes the full chain of steps (cmsRun 1 through N) on the machine where it is invoked. Intended to be used as the **job executable when submitting to the Grid**: transfer `request_psets.tar.gz` and one `jobN.json` per job, then call this script with those two arguments.

**Usage:**

```bash
execute_stepchain.sh <request_psets.tar.gz> <job_N.json>
```

- **request_psets.tar.gz** — Tarball with `request.json` and `PSets/` (produced by EventSplitter with `--psets`).
- **job_N.json** — Per-job file from EventSplitter (contains `job_index` and `tweaks` for each step).

The script extracts the tarball, reads the step count and step config from `request.json`, and for each step sets up the CMSSW release, applies the precomputed tweak from `jobN.json`, and runs cmsRun. Steps 2+ read input from the previous step’s output (e.g. `file:../step1/RAWSIMoutput.root`).