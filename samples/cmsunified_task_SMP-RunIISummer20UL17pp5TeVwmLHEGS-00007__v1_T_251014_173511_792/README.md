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

**Step 1 (cmsRun1) was particularly inefficient (~50% CPU use) and dominated the job runtime** (33 528 s of 35 484 s). This is the GEN-SIM step (wmLHEGS). Improving Step 1 efficiency or scaling (e.g. event-level parallelism, I/O, or resource requests) would have the largest impact on this workflow.

---

*README generated from notes in `info`.*
