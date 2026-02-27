#!/usr/bin/env python3
"""
Aggregate FrameworkJobReport XML files from stepchain cmsRun executions into a single JSON report.

Discovers job_report.xml files under --work-dir (step1/, step2/, step1/copy0/, etc.),
parses each using WMCore.FwkJobReport.Report, and writes an aggregated JSON with
stepchain-aware structure. Intended to run on the worker node after all steps complete.

Requires: PYTHONPATH including WMCore (e.g. WMCore.zip from ep_scripts).
"""
import argparse
import json
import os
import re
import sys


def discover_report_paths(work_dir, request_path):
    """
    Discover (step_key, path) pairs for job_report.xml files from a stepchain request.

    Returns list of (step_key, absolute_path) e.g. [("step1", ".../step1/job_report.xml"), ...].
    For step1 with num_copies>1: ("step1_copy0", ".../step1/copy0/job_report.xml"), etc.
    """
    if not os.path.isfile(request_path):
        return []
    with open(request_path) as f:
        req = json.load(f)
    num_steps = req.get("StepChain", 1)
    num_copies = req.get("Step1", {}).get("NumCopies", 1)

    result = []
    for n in range(1, num_steps + 1):
        if n == 1 and num_copies > 1:
            for c in range(num_copies):
                path = os.path.join(work_dir, "step1", "copy%d" % c, "job_report.xml")
                if os.path.isfile(path):
                    result.append(("step1_copy%d" % c, os.path.abspath(path)))
        else:
            path = os.path.join(work_dir, "step%d" % n, "job_report.xml")
            if os.path.isfile(path):
                result.append(("step%d" % n, os.path.abspath(path)))
    return result


def strip_report_step(step_data):
    """Strip 'file:' prefix from pfn and fileName in output file info (CRAB StripReport logic)."""
    if not step_data or "output" not in step_data:
        return
    for output_mod in step_data["output"].values():
        for file_info in output_mod:
            if "pfn" in file_info:
                file_info["pfn"] = re.sub(r"^file:", "", str(file_info["pfn"]))
            if "fileName" in file_info:
                file_info["fileName"] = re.sub(r"^file:", "", str(file_info["fileName"]))


def parse_report(xml_path):
    """
    Parse a FrameworkJobReport XML file and return the step data as a dict.

    Returns dict with step data (input, output, performance, errors, etc.) or None on error.
    """
    # Import here (not at top): WMCore is optional/env-specific (PYTHONPATH/WMCore.zip).
    # Lazy import lets the script run --help, discover paths, and handle "no reports" without
    # WMCore; we only fail when actually parsing a report.
    try:
        from WMCore.FwkJobReport.Report import Report
    except ImportError as e:
        print("[create_report] WMCore not available: %s" % e, file=sys.stderr)
        return None

    try:
        rep = Report("cmsRun")
        rep.parse(xml_path, "cmsRun")
        j = rep.__to_json__(None)
        if "steps" in j and "cmsRun" in j["steps"]:
            step_data = j["steps"]["cmsRun"]
            strip_report_step(step_data)
            return step_data
        return None
    except Exception as e:
        print("[create_report] Failed to parse %s: %s" % (xml_path, e), file=sys.stderr)
        return None


def get_step_exit_code(step_data):
    """Extract exit code from step data (errors, etc.)."""
    if not step_data:
        return None
    for err in step_data.get("errors", []):
        if "exitCode" in err:
            try:
                return int(err["exitCode"])
            except (ValueError, TypeError):
                pass
    return None


def get_step_events(step_data):
    """Sum events from input source in step data."""
    total = 0
    if not step_data or "input" not in step_data or "source" not in step_data["input"]:
        return total
    for src in step_data["input"]["source"]:
        total += int(src.get("events", 0) or src.get("EventsRead", 0) or 0)
    return total


def load_stage_out_results(work_dir):
    """Load stage_out_results.json if present. Returns dict LFN -> {pfn, pnn} or None."""
    path = os.path.join(work_dir, "stage_out_results.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return {s["lfn"]: {"pfn": s.get("pfn"), "pnn": s.get("pnn")} for s in data.get("staged", [])}
    except (json.JSONDecodeError, OSError, KeyError) as e:
        print("[create_report] Failed to read stage_out_results.json: %s" % e, file=sys.stderr)
        return None


def merge_stage_out_into_report(report, staged_by_lfn):
    """Merge staged pfn/pnn into output file records. Match by LFN."""
    if not staged_by_lfn:
        return
    for step_data in report.get("steps", {}).values():
        if not step_data or "output" not in step_data:
            continue
        for file_list in step_data["output"].values():
            for fi in file_list if isinstance(file_list, list) else []:
                lfn = fi.get("lfn") or fi.get("LFN") or fi.get("logicalFileName") or ""
                if lfn and lfn in staged_by_lfn:
                    s = staged_by_lfn[lfn]
                    fi["pfn"] = s.get("pfn") or fi.get("pfn")
                    fi["pnn"] = s.get("pnn")


def main():
    ap = argparse.ArgumentParser(
        description="Aggregate FrameworkJobReport XML files from stepchain into a single JSON report."
    )
    ap.add_argument(
        "--work-dir",
        required=True,
        help="Work directory containing step1/, step2/, ... (or step1/copy0/, ...)",
    )
    ap.add_argument(
        "--request",
        required=True,
        help="Path to request.json (for StepChain, NumCopies)",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: work-dir/job_report.json)",
    )
    args = ap.parse_args()

    work_dir = os.path.abspath(args.work_dir)
    request_path = args.request
    if not os.path.isfile(request_path):
        request_path = os.path.join(work_dir, "request.json")

    output_path = args.output
    if not output_path:
        output_path = os.path.join(work_dir, "job_report.json")
    output_path = os.path.abspath(output_path)

    paths = discover_report_paths(work_dir, request_path)
    if not paths:
        print("[create_report] No job_report.xml files found under %s" % work_dir, file=sys.stderr)
        # Write minimal report so transfer doesn't fail
        report = {
            "steps": {},
            "summary": {
                "jobExitCode": 0,
                "exitCode": 0,
                "totalEvents": 0,
                "stepsRun": [],
            },
        }
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        return 0

    steps = {}
    total_events = 0
    job_exit_code = 0
    steps_run = []

    for step_key, xml_path in paths:
        step_data = parse_report(xml_path)
        if step_data is None:
            print("[create_report] Skipping %s (parse failed)" % step_key, file=sys.stderr)
            continue
        steps[step_key] = step_data
        steps_run.append(step_key)
        total_events += get_step_events(step_data)
        ec = get_step_exit_code(step_data)
        if ec is not None and ec != 0:
            job_exit_code = ec

    report = {
        "steps": steps,
        "summary": {
            "jobExitCode": job_exit_code,
            "exitCode": job_exit_code,
            "totalEvents": total_events,
            "stepsRun": steps_run,
        },
    }

    staged_by_lfn = load_stage_out_results(work_dir)
    if staged_by_lfn is not None:
        merge_stage_out_into_report(report, staged_by_lfn)
    else:
        print("[create_report] No stage_out_results.json (stage-out failed or skipped).", file=sys.stderr)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print("[create_report] Wrote %s (%d steps, %d events)" % (
        output_path, len(steps), total_events
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
