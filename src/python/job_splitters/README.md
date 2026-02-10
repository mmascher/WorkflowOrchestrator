# event_splitter

Standalone **EventBased** job splitter for StepChain workflows. It uses WMCore’s `DataStructs` and `JobSplitting.SplitterFactory` (no WMBS/DB) to turn a ReqMgr-style request and splitting config into a set of jobs, each with precomputed PSet tweaks for every step. Those tweaks are consumed on the worker by the cmssw-wm-tools (e.g.: `edm_pset_tweak.py`).

## How it works

1. **Input:** ReqMgr-style `request.json` (StepChain, steps, CMSSW versions, RequestNumEvents, EventsPerLumi, etc.) and a `splitting.json` (Production task with EventBased `splitParams`: `events_per_job`, `events_per_lumi`, etc.).

2. **Subscription:** Builds a single “fake” MC file (matching Step1 production: total events, lumis) and a `Subscription` with the Production split algorithm from the splitting JSON.

3. **Splitting:** Calls WMCore’s EventBased splitter with the same parameters WMAgent would use. You get one job group per lumi, each job with a **mask** (FirstEvent, LastEvent, FirstLumi, runAndLumis, etc.).

4. **Tweaks:** For each job and each step, it builds a **tweak** dict in the format expected by `edm_pset_tweak.py`: keys like `process.source.firstEvent`, `process.maxEvents`, `process.source.fileNames`, and output module `fileName`. Step 1 gets a fixed event count; steps 2+ get `maxEvents = -1` and `fileNames = ['file:../stepN/OutputModule.root']` to chain from the previous step.

5. **Output:** A list of job dicts `{ "job_index": N, "tweaks": { "1": {...}, "2": {...}, ... } }`. With `--output-dir` (and optionally `--psets`), it writes `job1.json` … `jobN.json` and a tarball `request_psets.tar.gz` (request + PSets) for the worker.

## Dependencies

- **WMCore** must be on `PYTHONPATH` (e.g. a sibling `WMCore` clone with `src/python`).

## Usage

From the **WorkflowOrchestrator repo root**:

```bash
export PYTHONPATH=<repo_path>/WMCore/src/python

src/python/job_splitters/event_splitter.py \
  --request samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/request.json \
  --splitting samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/splitting.json \
  --psets samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/PSets/ \
  --output-dir /tmp
```

Example output:

```text
Generated 18073 jobs: job1..job18073.json, request_psets.tar.gz in /tmp
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--request` | yes | Path to ReqMgr-style request JSON. |
| `--splitting` | yes | Path to splitting JSON (must contain a Production entry with EventBased params). |
| `--output-dir` | no | If set, write `job1.json` … `jobN.json` here; with `--psets`, also create `request_psets.tar.gz`. |
| `--psets` | no | Path to PSets directory to pack into `request_psets.tar.gz` (only used when `--output-dir` is set). |

Without `--output-dir`, the script only prints job count and a short preview of the first jobs.

---

## Output files

### `jobN.json`

Each file has:

- **`job_index`** — Job number (1..N).
- **`tweaks`** — One object per step, keyed by step number `"1"`, `"2"`, … Keys are PSet paths in the format used by `edm_pset_tweak.py` (e.g. `customTypeCms.untracked.uint32(...)`). Step 1 sets `maxEvents` and the first output filename; steps 2+ set `fileNames` to the previous step’s output and the next output filename.

Example (`job38.json`, 6-step chain):

```json
{
  "job_index": 38,
  "tweaks": {
    "1": {
      "process.source.firstLuminosityBlock": "customTypeCms.untracked.uint32(38)",
      "process.maxEvents": "customTypeCms.untracked.PSet(input=cms.untracked.int32(830))",
      "process.source.firstEvent": "customTypeCms.untracked.uint32(30711)",
      "process.source.firstRun": "customTypeCms.untracked.uint32(1)",
      "process.RAWSIMoutput.fileName": "customTypeCms.untracked.string('file:RAWSIMoutput.root')"
    },
    "2": {
      "process.source.firstLuminosityBlock": "customTypeCms.untracked.uint32(38)",
      "process.maxEvents": "customTypeCms.untracked.PSet(input=cms.untracked.int32(-1))",
      "process.source.firstEvent": "customTypeCms.untracked.uint32(30711)",
      "process.source.firstRun": "customTypeCms.untracked.uint32(1)",
      "process.source.fileNames": "customTypeCms.untracked.vstring(['file:../step1/RAWSIMoutput.root'])",
      "process.RAWSIMoutput.fileName": "customTypeCms.untracked.string('file:RAWSIMoutput.root')"
    },
    "3": { "..." },
    "4": { "..." },
    "5": { "..." },
    "6": {
      "process.source.firstLuminosityBlock": "customTypeCms.untracked.uint32(38)",
      "process.maxEvents": "customTypeCms.untracked.PSet(input=cms.untracked.int32(-1))",
      "process.source.firstEvent": "customTypeCms.untracked.uint32(30711)",
      "process.source.firstRun": "customTypeCms.untracked.uint32(1)",
      "process.source.fileNames": "customTypeCms.untracked.vstring(['file:../step5/MINIAODSIMoutput.root'])"
    }
  }
}
```

### `request_psets.tar.gz`

Created only when both `--output-dir` and `--psets` are given. Contents:

- **`request.json`** — The same request document passed with `--request`.
- **`PSets/`** — Directory of base PSets per step (`PSet_cmsRun1_*.py`, …).

This tarball is what you pass to `execute_stepchain.sh` together with a single `jobN.json` to run one job on a worker.
