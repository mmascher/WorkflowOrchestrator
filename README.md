# WorkflowOrchestrator

## Repo structure

The repo is organized around three main areas:

| Location | Role |
|----------|------|
| **`src/python/job_splitters/`** | Event splitter: turns request + splitting config into per-job JSONs and the request tarball. See [job_splitters/README.md](src/python/job_splitters/README.md). |
| **`ep_scripts/`** | Executables used on the worker: **execute_stepchain.sh** runs the full stepchain (request tarball + one `jobN.json`); **stage_out.py** transfers files to the site storage element. See [ep_scripts/README.md](ep_scripts/README.md). |
| **`samples/`** | Real examples (request, splitting, PSets, and Condor job inputs per task). |