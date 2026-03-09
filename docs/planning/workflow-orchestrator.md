# Workflow Orchestrator Daemon

## Role

The Workflow Orchestrator is a daemon that:

1. Polls ReqMgr for staged (or acquired) StepChain requests
2. Fetches request document, splitting config, and PSets for each request
3. Submits one micro agent condor job per request via htcondor2

## Flow

```
loop:
  requests = get_available_requests(reqmgr, status="staged", request_type="StepChain")
  for each request:
    work_dir = work_dir / request_name
    fetch_request_data(reqmgr, request_name, request_doc, work_dir)
      → request.json, splitting.json, PSets/
    submit_micro_agent(work_dir, request_name, request_doc, config)
  sleep(poll_interval)
```

## Components

### orchestrator.py

- `load_config()` — Loads YAML config (reqmgr_url, status, poll_interval, work_dir, htcondor settings)
- `run_orchestrator(config)` — Main loop: poll → fetch → submit → sleep

### request_fetcher.py

- `get_available_requests(reqmgr, status, request_type)` — Returns list of (request_name, request_doc)
- `fetch_request_data(reqmgr, request_name, request_doc, work_dir, cert)` — Fetches splitting from ReqMgr, PSets from Config Cache; writes request.json, splitting.json, PSets/ to work_dir

### micro_agent_submitter.py

- `build_micro_agent_jdl(work_dir, request_name, config)` — Copies ep_scripts, WMCore.zip, utils.py, job_splitters, micro_agent to work_dir; creates micro_agent.jdl (Universe=local, Executable=run_micro_agent.sh)
- `submit_micro_agent(work_dir, request_name, request_doc, config)` — Builds JDL, submits via htcondor2 to remote schedd (ID token auth)

## Config (orchestrator.yaml)

| Key | Description |
|-----|-------------|
| reqmgr_url | ReqMgr2 URL |
| status | Request status to poll (e.g. staged, acquired) |
| request_type | StepChain |
| poll_interval | Seconds between polls |
| work_dir | Base directory for per-request work dirs |
| proxy | X509 proxy path (for ReqMgr, Config Cache) |
| sitelist | Path to sitelist.txt |
| schedd_name | Remote HTCondor schedd |
| collector | HTCondor collector |
| idtoken | Path to HTCondor ID token for schedd auth |

## Notes

- Micro agent runs with Universe=local on the schedd machine; it then submits the actual stepchain jobs (Grid) via condor_submit.
- One micro agent per request: the micro agent handles event splitting and submission of all jobs for that request.
