# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2025-03-02

### Added

- **Micro Agent Monitor (MAM)** (`src/python/micro_agent/micro_agent_monitor.py`): Parses HTCondor user logs, detects `JOB_TERMINATED` events, extracts output file info from framework job reports, and stores it in a file-centric SQLite database. Supports daemon mode (tail log) or single-pass (`--once`). Only stores outputs from steps with `KeepOutput==True`. See [samples/micro_agent/README.md](samples/micro_agent/README.md).
- **Job report aggregation** (`ep_scripts/create_report.py`): Aggregates `FrameworkJobReport` XML files from stepchain cmsRun executions into a single JSON report. Merges stageout results (PFN, PNN) into output file records. Runs on the worker after all steps complete; output transferred back with the job.
- **prmon support**: Memory profiling via `prmon` in `run.sh`; `prmon.txt` and `prmon.json` transferred back with job output.
- **Shared utils** (`src/python/micro_agent/utils.py`): `build_lfn`, `build_lfn_for_file`, `load_step_config` for LFN construction and step config; used by stage_out and MAM.
- **EventStreams**: Event splitter and tweaks now take `EventStreams` from request.json (per-step or workload-level).

### Fixed

- Improved error messages in `execute_stepchain.sh`.

## [0.3.0] - 2026-02-21

### Added

- **NumCopies for step 1**: Event splitter and executor support `NumCopies` for the first (often GEN-SIM) step; copies are handled at splitting time and run in parallel, significantly improving CPU utilization and reducing job wall time.
- **create_stepchain_jdl.py**: Single JDL for all event_splitter jobs; derives num_jobs, cpus, memory, walltime, REQUIRED_OS from request.json.
- cmsRun job statistics via `cmssw_handle_condor_status_service` in `execute_stepchain.sh`.
- Debug output: timestamps, env, HTCondor ads, and `[stage_out]`-prefixed stageout messages.

### Changed

- **DAG removed**: Single JDL submission only; `create_stepchain_dag.py` and `postjob.py` removed.
- Refactored num_copies handling; no root files in output tarball; improved cleanup and error handling.
- Retries use `$(Process)` in paths; REQUIRED_OS from SCRAM_ARCH; READMEs simplified.

### Fixed

- stdout/stderr with numcopies; Queue statement in JDL; SITECONF check; tar verbosity in `run.sh`.
- added pset tweak for multicore: everything was single core by default without it

## [0.2.0] - 2026-02-10

### Added

- **Stageout script** (`ep_scripts/stage_out.py`): Transfers output files from the worker node to the site storage element using WMCore `StageOutMgr` and the site's storage config. Supports `--lfn`/`--local` pairs for explicit files or `--request`/`--work-dir` mode for automatic discovery from a stepchain request. See [ep_scripts/README.md](ep_scripts/README.md).
- **HTCondor examples** (`samples/htcondor/`): Sample JDL, wrapper script (`run.sh`), and site list for submitting one StepChain job per site as a Grid validation. Includes a pre-packaged `WMCore.zip` for the worker. See [htcondor/README.md](samples/htcondor/README.md).
- Stageout integrated into `execute_stepchain.sh`: the executor now stages out output files after the last step completes.

### Changed

- **PEP 08 compliance**: Renamed `src/python/JobSplitters/EventSplitter.py` to `src/python/job_splitters/event_splitter.py` and updated all references.
- Removed CRAB-specific references from `submit_env.sh`.

### Fixed

- Fixed `ep_scripts` path resolution in `execute_stepchain.sh`.
- Fixed stageout environment setup to work correctly on the Grid.
- Code style cleanup in `execute_stepchain.sh`.

## [0.1.1] - 2025-02-07

### Fixed

- Change execute_stepchain.sh and make sure it runs on the Grid (was tested on local setup)

## [0.1.0] - 2025-02-06

### Added

- **Event splitter** (`src/python/job_splitters/event_splitter.py`): Standalone EventBased splitter for StepChain workflows. Reads ReqMgr-style request + splitting JSON, uses WMCore DataStructs and SplitterFactory, outputs per-job JSONs (`job1.json` … `jobN.json`) and optional `request_psets.tar.gz` for the worker. See [job_splitters/README.md](src/python/job_splitters/README.md).
- **Stepchain executor** (`ep_scripts/execute_stepchain.sh`): Worker script that takes `request_psets.tar.gz` and one `jobN.json`, unpacks the tarball, and runs the full stepchain (cmsRun per step with precomputed PSet tweaks). Suited for Grid submission. See [ep_scripts/README.md](ep_scripts/README.md).
- **Sample** (`samples/cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792/`): Real request with `request.json`, `splitting.json`, `PSets/`, and Condor job inputs (`ap_input/`), plus README with ReqMgr link and step timing/efficiency notes.
- **Documentation**: Root README (repo structure), job_splitters README (splitter usage and output format), ep_scripts README (executor usage), sample README (directory layout and example job metrics).

### Dependencies

- event_splitter requires WMCore on `PYTHONPATH` (e.g. `../WMCore/src/python`).
- `execute_stepchain.sh` requires `edm_pset_pickler.py`, `edm_pset_tweak.py`, `cmssw_handle_nEvents.py` on PATH (e.g. from cmssw-wm-tools) and a CMS/SCRAM environment (e.g.: the cmssw-el7 for the `SMP-RunIISummer20UL17pp5TeVwmLHEGS` example in the repo).

[0.4.0]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.4.0
[0.3.0]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.3.0
[0.2.0]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.2.0
[0.1.1]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.1.1
[0.1.0]: https://github.com/dmwm/WorkflowOrchestrator/releases/tag/v0.1.0
