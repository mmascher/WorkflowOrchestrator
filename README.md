# WorkflowOrchestrator

## Repo structure

The repo is organized around three main areas:

| Location | Role |
|----------|------|
| **`src/python/JobSplitters/`** | Event splitter: turns request + splitting config into per-job JSONs and the request tarball. See [JobSplitters/README.md](src/python/JobSplitters/README.md). |
| **`ep_scripts/`** | Executables used on the worker: **execute_stepchain.sh** runs the full stepchain (request tarball + one `jobN.json`); **stageout.py** transfers files to the site storage element. See [ep_scripts/README.md](ep_scripts/README.md). |
| **`samples/`** | Real examples (request, splitting, PSets, and Condor job inputs per task). |