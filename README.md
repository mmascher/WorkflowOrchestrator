# WorkflowOrchestrator

## Repo structure

The repo is organized around three main areas:

| Location | Role |
|----------|------|
| **`src/python/JobSplitters/`** | Event splitter: turns request + splitting config into per-job JSONs and the request tarball. See [JobSplitters/README.md](src/python/JobSplitters/README.md). |
| **`scripts/`** | Executable used on the worker: takes the request tarball and one `jobN.json`, runs the full stepchain. See [scripts/README.md](scripts/README.md). |
| **`samples/`** | Real examples (request, splitting, PSets, and Condor job inputs per task). |