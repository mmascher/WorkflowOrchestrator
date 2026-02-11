#!/usr/bin/env python3
"""
Generate HTCondor DAG and submit files from event_splitter output.

Reads job1.json, job2.json, ... from an event_splitter output directory,
creates a DAG file with one node per job, and a parameterized submit file
with retry-on-different-machine support.

Usage:
  python -m job_splitters.create_stepchain_dag \\
    --event-splitter-dir event_splitter_out/ \\
    --proxy /tmp/x509up_u$(id -u) \\
    --sitelist sitelist.txt
"""
import argparse
import os
import re


def discover_job_files(event_splitter_dir):
    """Discover job1.json, job2.json, ... and return sorted list of job indices."""
    if not os.path.isdir(event_splitter_dir):
        raise SystemExit(f"Error: event-splitter-dir is not a directory: {event_splitter_dir}")

    pattern = re.compile(r"^job(\d+)\.json$")
    indices = []
    for name in os.listdir(event_splitter_dir):
        m = pattern.match(name)
        if m:
            indices.append(int(m.group(1)))

    if not indices:
        raise SystemExit(
            f"Error: no job*.json files found in {event_splitter_dir}. "
            "Expected job1.json, job2.json, ..."
        )

    return sorted(indices)


def check_request_psets(event_splitter_dir):
    """Verify request_psets.tar.gz exists."""
    path = os.path.join(event_splitter_dir, "request_psets.tar.gz")
    if not os.path.isfile(path):
        raise SystemExit(
            f"Error: request_psets.tar.gz not found in {event_splitter_dir}"
        )


def read_sitelist(sitelist_path):
    """Read sitelist file and return comma-separated sites string."""
    if not os.path.isfile(sitelist_path):
        raise SystemExit(f"Error: sitelist file not found: {sitelist_path}")

    with open(sitelist_path) as f:
        sites = [line.strip() for line in f if line.strip()]

    if not sites:
        raise SystemExit(f"Error: sitelist file is empty: {sitelist_path}")

    return ", ".join(sites)


def write_dag_file(
    output_path,
    job_indices,
    submit_file_name,
    post_script_name,
    dag_retries=2,
    post_defer_delay_sec=21600,
):
    """Write the DAG file."""
    lines = [
        "# DAG generated from event_splitter output",
        "",
    ]
    for idx in job_indices:
        lines.append(f"JOB Job{idx} {submit_file_name}")
    lines.append("")
    for idx in job_indices:
        lines.append(f'VARS Job{idx} JOB_ID="{idx}"')
    lines.append("")
    for idx in job_indices:
        lines.append(f"RETRY Job{idx} {dag_retries} UNLESS-EXIT 1")
    lines.append("")
    for idx in job_indices:
        lines.append(
            f"SCRIPT DEFER 1 {post_defer_delay_sec} POST Job{idx} {post_script_name} $JOB"
        )

    content = "\n".join(lines) + "\n"
    with open(output_path, "w") as f:
        f.write(content)


def write_submit_file(
    output_path,
    event_splitter_dir,
    proxy_path,
    sites_str,
    executable,
    max_retries,
):
    """Write the HTCondor submit file."""
    # Retry-on-different-machine: exclude last machine on retry
    retry_requirements = (
        "( (LastRemoteHost =?= undefined) || "
        "(TARGET.Machine =!= split(LastRemoteHost, \"@\")[1]) )"
    )

    content = f"""Universe   = vanilla

Executable = {executable}

Log        = log/run.$(Cluster)
Output     = out/run.$(Cluster).$(JOB_ID)
Error      = err/run.$(Cluster).$(JOB_ID)

should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = execute_stepchain.sh,submit_env.sh,stage_out.py,WMCore.zip,{event_splitter_dir}/job$(JOB_ID).json,{event_splitter_dir}/request_psets.tar.gz
transfer_output_files = output.tgz
transfer_output_remaps = "output.tgz = results/output.$(Cluster).$(JOB_ID).tgz"

x509userproxy = {proxy_path}
use_x509userproxy = True

+DESIRED_Sites = "{sites_str}"

request_cpus = 1
request_memory = 1000
+MaxWallTimeMins = 180

+REQUIRED_OS = "rhel7"

# Retry on different machine when run.sh fails (CERN batch docs pattern)
on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)
max_retries = {max_retries}
requirements = {retry_requirements}
"""
    with open(output_path, "w") as f:
        f.write(content)


def copy_post_script(output_dir):
    """Copy postjob.py from ep_scripts to output_dir so DAGMan can find it."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    src = os.path.join(repo_root, "ep_scripts", "postjob.py")
    dst = os.path.join(output_dir, "postjob.py")
    if os.path.isfile(src):
        import shutil

        shutil.copy(src, dst)
        os.chmod(dst, 0o755)
        return dst
    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate HTCondor DAG and submit files from event_splitter output.",
        epilog="Submit with: condor_submit_dag <output-dag>",
    )
    parser.add_argument(
        "--event-splitter-dir",
        required=True,
        help="Path to event_splitter output dir (job1.json, job2.json, ..., request_psets.tar.gz)",
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
        "--output-dag",
        default="stepchain.dag",
        help="Output DAG file path (default: stepchain.dag)",
    )
    parser.add_argument(
        "--output-submit",
        default="job.submit",
        help="Output submit file path (default: job.submit)",
    )
    parser.add_argument(
        "--executable",
        default="run.sh",
        help="Executable path in submit file (default: run.sh)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Job-level retries on different machine (default: 3)",
    )
    parser.add_argument(
        "--post-defer-delay",
        type=int,
        default=21600,
        help="Seconds to wait before retrying POST script after exit 1 (default: 21600 = 6h)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    job_indices = discover_job_files(args.event_splitter_dir)
    check_request_psets(args.event_splitter_dir)
    sites_str = read_sitelist(args.sitelist)

    # Use basename for submit file reference in DAG (user runs from same dir)
    submit_file_name = os.path.basename(args.output_submit)

    write_dag_file(
        args.output_dag,
        job_indices,
        submit_file_name,
        "postjob.py",
        post_defer_delay_sec=args.post_defer_delay,
    )
    output_dir = os.path.dirname(os.path.abspath(args.output_dag)) or "."
    copy_post_script(output_dir)

    write_submit_file(
        output_path=args.output_submit,
        event_splitter_dir=args.event_splitter_dir,
        proxy_path=args.proxy,
        sites_str=sites_str,
        executable=args.executable,
        max_retries=args.max_retries,
    )

    print(f"Generated DAG with {len(job_indices)} jobs: {args.output_dag}")
    print(f"Submit file: {args.output_submit}")
    print(f"Submit with: condor_submit_dag {args.output_dag}")


if __name__ == "__main__":
    main()
