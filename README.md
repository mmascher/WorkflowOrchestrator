# WorkflowOrchestrator

This project is intended as a proof of concept for the Workflow Orchestrator that could potentially replace WMAgent for the CMS Workflow Management system of CMS.

## Repo structure

The repo is organized around five main areas:

| Location | Role |
|----------|------|
| **`src/python/workflow_orchestrator/`** | Workflow Orchestrator daemon: queries ReqMgr for staged StepChain requests, fetches request/splitting/PSet data, submits micro agents via htcondor2. |
| **`src/python/job_splitters/`** | Event splitter: turns request + splitting config into per-job JSONs and the request tarball. See [job_splitters/README.md](src/python/job_splitters/README.md). |
| **`src/python/micro_agent/`** | **create_stepchain_jdl.py** — generates HTCondor JDL for StepChain submission. **micro_agent_monitor.py** (MAM) — tails condor logs, stores file info in SQLite. |
| **`src/go/micro_agent_monitor/`** | **Go implementation** of MAM — same functionality, for experimentation. Use `MAM_IMPL=go` with `run_micro_agent.sh` to run the Go version. |
| **`ep_scripts/`** | **run_micro_agent.sh** — micro agent wrapper (event_splitter → create_stepchain_jdl → condor_submit → MAM). **execute_stepchain.sh** runs the stepchain on workers. See [ep_scripts/README.md](ep_scripts/README.md). |
| **`samples/`** | Real examples: request, splitting, PSets, and Condor job inputs. [workflow_orchestrator/](samples/workflow_orchestrator/) — run scripts. |

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mmascher/WorkflowOrchestrator)
