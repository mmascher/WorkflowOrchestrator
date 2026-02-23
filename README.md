# WorkflowOrchestrator

This project is intended as a proof of concept for the Workflow Orchestrator that could potentially replace WMAgent for the CMS Workflow Management system of CMS.

## Repo structure

The repo is organized around four main areas:

| Location | Role |
|----------|------|
| **`src/python/job_splitters/`** | Event splitter: turns request + splitting config into per-job JSONs and the request tarball. See [job_splitters/README.md](src/python/job_splitters/README.md). |
| **`src/python/micro_agent/`** | **create_stepchain_jdl.py** — generates HTCondor JDL for StepChain submission (num_jobs, request_cpus, Memory, walltime from request.json). Used before `condor_submit`. |
| **`ep_scripts/`** | Executables used on the worker: **execute_stepchain.sh** runs the full stepchain (request tarball + one `jobN.json`); **stage_out.py** transfers files to the site storage element. See [ep_scripts/README.md](ep_scripts/README.md). |
| **`samples/`** | Real examples: request, splitting, PSets, and Condor job inputs per task. Includes [htcondor/](samples/htcondor/README.md) sample JDL for one-job-per-site Grid validation. |
