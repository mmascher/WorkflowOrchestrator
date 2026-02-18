#!/usr/bin/env python3
"""
Generate HTCondor JDL file from event_splitter output.

Runs as a standalone script, typically before any jobs are submitted (e.g. on the
submit machine or in CI). No WMCore dependency—uses a local ScramArch→REQUIRED_OS
mapping. Creates a single JDL with Queue from seq 1 N (1-based: job1.json ...
jobN.json). Derives num_jobs, request_cpus, Memory, walltime, and REQUIRED_OS
from request.json (Multicore, Memory, TimePerEvent, Step1.EventsPerJob,
ScramArch, etc.).

Usage:
  python -m micro_agent.create_stepchain_jdl \\
    --event-splitter-dir event_splitter_out/ \\
    --request request.json \\
    --proxy /tmp/x509up_u$(id -u) \\
    --sitelist sitelist.txt
"""
import argparse
import json
import math
import os

DEFAULT_REQUIRED_OS = "rhel7"

# ScramArch prefix -> HTCondor REQUIRED_OS (mirrors WMCore WMRuntime.Tools.Scram.ARCH_TO_OS)
ARCH_TO_OS = {
    "slc5": ["rhel6"],
    "slc6": ["rhel6"],
    "slc7": ["rhel7"],
    "el8": ["rhel8"],
    "cc8": ["rhel8"],
    "cs8": ["rhel8"],
    "alma8": ["rhel8"],
    "el9": ["rhel9"],
    "cs9": ["rhel9"],
}


def scram_arch_to_required_os(scram_arch=None):
    """Map ScramArch (or list) to HTCondor REQUIRED_OS. Mirrors WMCore BasePlugin.scramArchtoRequiredOS."""
    if not scram_arch:
        return "any"
    if isinstance(scram_arch, str):
        scram_arch = [scram_arch]
    elif not isinstance(scram_arch, (list, tuple)):
        return "any"
    required = set()
    for arch in scram_arch:
        prefix = arch.split("_")[0]
        required.update(ARCH_TO_OS.get(prefix, []))
    return ",".join(sorted(required)) if required else "any"


def read_request(request_path):
    """Read request.json and extract job params. Returns dict or None."""
    if not request_path or not os.path.isfile(request_path):
        return None
    with open(request_path) as f:
        req = json.load(f)
    step1 = req.get("Step1", {})
    request_num_events = step1.get("RequestNumEvents")
    events_per_job = step1.get("EventsPerJob")
    time_per_event = req.get("TimePerEvent")
    num_jobs = req.get("TotalEstimatedJobs")
    if num_jobs is None and request_num_events and events_per_job:
        num_jobs = int(math.ceil(float(request_num_events) / float(events_per_job)))
    walltime_mins = None
    if time_per_event is not None and events_per_job:
        # TimePerEvent in sec/event; add 50% margin for StepChain overhead
        walltime_mins = int(math.ceil(float(time_per_event) * float(events_per_job) * 1.5 / 60))
    scram_arch = req.get("ScramArch")
    required_os = scram_arch_to_required_os(scram_arch) if scram_arch else DEFAULT_REQUIRED_OS

    return {
        "request_cpus": req.get("Multicore", 1),
        "request_memory": req.get("Memory", 1000),
        "num_jobs": num_jobs,
        "walltime_mins": walltime_mins,
        "batch_name": req.get("RequestName") or req.get("_id"),
        "required_os": required_os,
    }


def read_sitelist(sitelist_path):
    """Read sitelist file and return comma-separated sites string."""
    if not os.path.isfile(sitelist_path):
        raise SystemExit(f"Error: sitelist file not found: {sitelist_path}")

    with open(sitelist_path) as f:
        sites = [line.strip() for line in f if line.strip()]

    if not sites:
        raise SystemExit(f"Error: sitelist file is empty: {sitelist_path}")

    return ", ".join(sites)


def write_jdl_file(
    output_path,
    event_splitter_dir,
    proxy_path,
    sites_str,
    executable,
    max_retries,
    num_jobs,
    batch_name=None,
    request_cpus=1,
    request_memory=1000,
    walltime_mins=180,
    required_os="rhel7",
):
    """Write the HTCondor JDL file."""
    retry_requirements = (
        "( (LastRemoteHost =?= undefined) || "
        "(TARGET.Machine =!= split(LastRemoteHost, \"@\")[1]) )"
    )

    batch_line = f'JobBatchName = "{batch_name}"\n\n' if batch_name else ""

    content = f"""Universe   = vanilla

Executable = {executable}

Log        = log/run.$(Cluster)
Output     = out/run.$(Cluster).$(Process).$(NumJobCompletions)
Error      = err/run.$(Cluster).$(Process).$(NumJobCompletions)

should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = execute_stepchain.sh,submit_env.sh,stage_out.py,WMCore.zip,{event_splitter_dir}/job$(Index).json,{event_splitter_dir}/request_psets.tar.gz
transfer_output_files = output.tgz
transfer_output_remaps = "output.tgz = results/output.$(Cluster).$(Process).$(NumJobCompletions).tgz"

{batch_line}x509userproxy = {proxy_path}
use_x509userproxy = True

+DESIRED_Sites = "{sites_str}"

request_cpus = {request_cpus}
request_memory = {request_memory}
+MaxWallTimeMins = {walltime_mins}

+REQUIRED_OS = "{required_os}"

# Retry on different machine when run.sh fails (CERN batch docs pattern)
on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)
max_retries = {max_retries}
requirements = {retry_requirements}

Queue Index from seq 1 {num_jobs} |
"""
    with open(output_path, "w") as f:
        f.write(content)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate HTCondor JDL file from event_splitter output (1-based job indices).",
        epilog="Submit with: condor_submit stepchain.jdl",
    )
    parser.add_argument(
        "--event-splitter-dir",
        required=True,
        help="Path to event_splitter output dir (job1.json, job2.json, ..., request_psets.tar.gz)",
    )
    parser.add_argument(
        "--request",
        required=True,
        help="Path to request.json (derives num_jobs, request_cpus, Memory, walltime)",
    )
    parser.add_argument(
        "--proxy",
        required=True,
        help="Path to x509 user proxy (e.g. $X509_USER_PROXY or /tmp/x509up_$UID)",
    )
    parser.add_argument(
        "--sitelist",
        required=True,
        help="Path to file listing one site per line (for +DESIRED_Sites)",
    )
    parser.add_argument(
        "--output-jdl",
        default="stepchain.jdl",
        help="Output JDL file path (default: stepchain.jdl)",
    )
    parser.add_argument(
        "--executable",
        default="run.sh",
        help="Executable path in JDL (default: run.sh)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Job-level retries on different machine (default: 3)",
    )
    parser.add_argument(
        "--batch-name",
        help="JobBatchName (e.g. request name)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    req_params = read_request(args.request)
    if not req_params:
        raise SystemExit(f"Error: could not read request from {args.request}")

    num_jobs = req_params.get("num_jobs")
    if num_jobs is None:
        raise SystemExit("Error: request.json must have TotalEstimatedJobs or Step1.RequestNumEvents/EventsPerJob")
    if num_jobs <= 0:
        raise SystemExit("Error: num_jobs must be positive")

    request_cpus = req_params.get("request_cpus", 1)
    request_memory = req_params.get("request_memory", 1000)
    walltime_mins = req_params.get("walltime_mins", 180)
    batch_name = args.batch_name or req_params.get("batch_name")
    required_os = req_params.get("required_os", DEFAULT_REQUIRED_OS)

    sites_str = read_sitelist(args.sitelist)

    write_jdl_file(
        output_path=args.output_jdl,
        event_splitter_dir=args.event_splitter_dir,
        proxy_path=args.proxy,
        sites_str=sites_str,
        executable=args.executable,
        max_retries=args.max_retries,
        num_jobs=num_jobs,
        batch_name=batch_name,
        request_cpus=request_cpus,
        request_memory=request_memory,
        walltime_mins=walltime_mins,
        required_os=required_os,
    )

    print(f"Generated JDL with {num_jobs} jobs: {args.output_jdl}")
    print(f"Submit with: condor_submit {args.output_jdl}")


if __name__ == "__main__":
    main()
