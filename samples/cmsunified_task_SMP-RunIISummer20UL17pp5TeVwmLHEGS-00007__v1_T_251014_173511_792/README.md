# Sample: cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792

This directory holds a real job from the request **cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792**: request payload, splitting config, PSets, and the inputs that Condor transferred for a single worker (execution point) job.

## Request Manager

- **Request (ReqMgr2):**  
  https://cmsweb.cern.ch/reqmgr2/fetch?rid=request-cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792

## Directory layout

| Path | Description |
|------|-------------|
| `request.json` | ReqMgr-style request document (StepChain, steps, CMSSW versions, etc.). |
| `splitting.json` | Splitting configuration (EventBased for Production, etc.). |
| `PSets/` | Base PSets per step (`PSet_cmsRun1_*`, …, `PSet_cmsRun6_*`). |
| `ap_input/` | Inputs for a single Condor job as sent to the execution point (see below). |
| `info` | Raw notes used to build this README (can be removed once README is enough). |

### `ap_input/` — Condor job inputs

Contents of what Condor transfers to the worker for one job:

- **Executable:** `submit_py3.sh` — WMAgent bootstrap script that runs on the execution point.
- **Arguments (Args):**  
  `cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792-Sandbox.tar.bz2` `6956199` `2`  
  (Sandbox tarball name, job ID, and a numeric argument, e.g. retry count).
- **Input files:**  
  - `cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792-Sandbox.tar.bz2`  
  - `JobPackage.pkl`  
  - `Unpacker.py`  

So the Condor command line for this example job is effectively:

```text
submit_py3.sh cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792-Sandbox.tar.bz2 6956199 2
```

---

## Example job from Condor (110919.18)

One representative job was inspected in Condor history:

```text
condor_history 110919.18
 ID     OWNER    SUBMITTED   RUN_TIME        ST COMPLETED   CMD
110919.18 cmst1  1/28 20:11  0+10:01:33  C  1/31 13:48   /data/srv/wmagent/.../submit_py3.sh ... 6956199 2
```

- **Run time:** ~10 h 1 min  
- **Requested CPUs:** 4  
- **CpusUsage:** ~1.80  

### Step timings (Chirp attributes)

| Step   | Elapsed (s) | TotalCPU (s) | CPU efficiency (TotalCPU / (Elapsed × 4)) |
|--------|-------------|--------------|-------------------------------------------|
| cmsRun1 | 33 528     | 68 963       | **~0.51 (51%)**                            |
| cmsRun2 | 551        | 2 051        | ~0.93                                     |
| cmsRun3 | 1 139      | 1 942        | ~0.42                                     |
| cmsRun4 | 171        | 589          | ~0.85                                     |
| cmsRun5 | 63         | 206          | ~0.82                                     |
| cmsRun6 | 32         | 103          | ~0.80                                     |

Total wall time of the six steps: **~9 h 51 min** (35 484 s).

### Note on Step 1 efficiency

**Step 1 (cmsRun1) was particularly inefficient (~50% CPU use) and dominated the job runtime** (33 528 s of 35 484 s). This is the GEN-SIM step (wmLHEGS). Improving Step 1 efficiency would have the largest impact on this workflow.

---

## Improvement with `numcopies` parameter

Two follow-up jobs were run to compare Step 1 (GEN-SIM) execution with different `numcopies` settings. These runs use the fixed `numberOfThreads` tweak (process.options.numberOfThreads = Multicore). Raw Condor history data is in `info2/`.

| Job ID      | Step 1 copies | Step 1 wall time | Total job wall time | Job efficiency |
|-------------|----------------|------------------|----------------------|----------------|
| 10372986.13 | 1              | 26 453 s         | 28 325 s (~7.9 h)    | ~0.50          |
| 10372987.15 | 4              | ~12 091 s*       | 14 552 s (~4.0 h)    | ~0.84          |

\* With 4 copies, Step 1 runs in parallel; the value shown is the longest of the four parallel runs.

Job efficiency = (RemoteUserCpu + RemoteSysCpu) / (CommittedTime × 4).

### Step timings and CPU efficiency (1 copy — job 10372986.13)

| Step   | Elapsed (s) | TotalCPU (s) | CPU efficiency (TotalCPU / (Elapsed × 4)) |
|--------|-------------|--------------|-------------------------------------------|
| cmsRun1 | 26 453     | 54 174       | **~0.51 (51%)**                            |
| cmsRun2 | 419        | 1 420        | ~0.85                                     |
| cmsRun3 | 684        | 1 244        | ~0.45                                     |
| cmsRun4 | 135        | 371          | ~0.69                                     |
| cmsRun5 | 61         | 137          | ~0.56                                     |
| cmsRun6 | 43         | 61           | ~0.35                                     |

### Step timings and CPU efficiency (4 copies — job 10372987.15)

| Step   | Elapsed (s) | TotalCPU (s) | CPU efficiency (TotalCPU / (Elapsed × 4)) |
|--------|-------------|--------------|-------------------------------------------|
| cmsRun1 | 12 091     | 45 369       | **~0.94 (94%)**                            |
| cmsRun2 | 647        | 2 154        | ~0.83                                     |
| cmsRun3 | 1 062      | 1 937        | ~0.46                                     |
| cmsRun4 | 172        | 544          | ~0.79                                     |
| cmsRun5 | 70         | 203          | ~0.72                                     |
| cmsRun6 | 35         | 93           | ~0.67                                     |

**Result:** Using `numcopies=4` for Step 1 reduces total job wall time by **~49%** (from ~7.9 h to ~4.0 h). With the fixed `numberOfThreads` tweak, the 1-copy job now achieves **~51%** Step 1 CPU efficiency (matching the original WMCore production job), while 4 copies reach **~94%**. The GEN-SIM step benefits from parallel execution across multiple processes, improving overall CPU utilization and shortening the dominant phase of the workflow.

---

*README generated from notes in `info`.*
