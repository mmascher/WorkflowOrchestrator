# WorkflowOrchestrator — Master Plan

## Goal

Proof of concept for a **Workflow Orchestrator** that could potentially replace WMAgent for the CMS Workflow Management system. The orchestrator manages StepChain workflows end-to-end: from ReqMgr to HTCondor execution and output tracking.

## High-Level Architecture

```
ReqMgr (staged/acquired)  →  Workflow Orchestrator  →  Micro Agent (HTCondor)
                                    │
                                    └── fetches: request, splitting, PSets
                                    └── submits: one micro agent job per request
                                    │
Micro Agent (on schedd)   →  event_splitter  →  create_stepchain_jdl  →  condor_submit  →  MAM
                                    │
                                    └── per-request: job1..jobN.json, request_psets.tar.gz
                                    └── MAM: tails condor log, stores file info in SQLite
```

## Main Components

| Component | Role |
|-----------|------|
| **Workflow Orchestrator** | Daemon: polls ReqMgr for staged StepChain requests, fetches data, submits one micro agent per request via htcondor2 |
| **Micro Agent** | Runs on HTCondor schedd: event_splitter → create_stepchain_jdl → condor_submit → MAM |
| **Event Splitter** | Turns request + splitting into per-job JSONs and request tarball |
| **create_stepchain_jdl** | Generates single JDL for all stepchain jobs |
| **MAM (Micro Agent Monitor)** | Tails condor log, parses JOB_TERMINATED, stores output file info in SQLite |
| **execute_stepchain.sh** | Worker script: runs full stepchain (cmsRun per step) on Grid nodes |
| **stage_out.py** | Transfers output files from worker to site storage |

## Design Principles

- **Decoupled from WMBS**: Event splitter uses WMCore DataStructs/SplitterFactory only; no WMBS database.
- **One micro agent per request**: Each staged request gets one condor job that handles all event_splitter jobs internally.
- **File-centric tracking**: MAM stores output file info (LFN, PFN, PNN) in SQLite, not job-centric state.
- **Reuse existing tools**: cmssw-wm-tools (edm_pset_tweak, etc.), WMCore StageOutMgr, HTCondor.

## Dependencies

- **WMCore** (PYTHONPATH) — ReqMgr, DataStructs, SplitterFactory, StageOutMgr
- **htcondor2** — Remote schedd submission (ID tokens)
- **cmssw-wm-tools** — edm_pset_pickler, edm_pset_tweak, cmssw_handle_nEvents
- **CMS/SCRAM** — On worker nodes for cmsRun

## Repo Structure

```
src/python/workflow_orchestrator/   # WO daemon, request_fetcher, micro_agent_submitter
src/python/job_splitters/          # event_splitter
src/python/micro_agent/            # create_stepchain_jdl, micro_agent_monitor, utils
ep_scripts/                        # run_micro_agent.sh, execute_stepchain.sh, stage_out.py, create_report.py
samples/                           # Real examples (request, splitting, PSets, htcondor)
config/                            # orchestrator.yaml
```
