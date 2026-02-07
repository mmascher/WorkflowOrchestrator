# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2025-02-06

### Added

- **Event splitter** (`src/python/JobSplitters/EventSplitter.py`): Standalone EventBased splitter for StepChain workflows. Reads ReqMgr-style request + splitting JSON, uses WMCore DataStructs and SplitterFactory, outputs per-job JSONs (`job1.json` … `jobN.json`) and optional `request_psets.tar.gz` for the worker. See [JobSplitters/README.md](src/python/JobSplitters/README.md).
- **Stepchain executor** (`ep_scripts/execute_stepchain.sh`): Worker script that takes `request_psets.tar.gz` and one `jobN.json`, unpacks the tarball, and runs the full stepchain (cmsRun per step with precomputed PSet tweaks). Suited for Grid submission. See [ep_scripts/README.md](ep_scripts/README.md).
- **Sample** (`samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/`): Real request with `request.json`, `splitting.json`, `PSets/`, and Condor job inputs (`ap_input/`), plus README with ReqMgr link and step timing/efficiency notes.
- **Documentation**: Root README (repo structure), JobSplitters README (splitter usage and output format), ep_scripts README (executor usage), sample README (directory layout and example job metrics).

### Dependencies

- EventSplitter requires WMCore on `PYTHONPATH` (e.g. `../WMCore/src/python`).
- `execute_stepchain.sh` requires `edm_pset_pickler.py`, `edm_pset_tweak.py`, `cmssw_handle_nEvents.py` on PATH (e.g. from cmssw-wm-tools) and a CMS/SCRAM environment (e.g.: the cmssw-el7 for the `SMP-RunIISummer20UL17pp5TeVwmLHEGS` example in the repo).

[0.1.0]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.1.0
