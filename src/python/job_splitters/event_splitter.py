#!/usr/bin/env python3
import argparse
import json
import math
import pprint
import os
import shutil
import tarfile

from WMCore.DataStructs.File import File as DSFile
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.SplitterFactory import SplitterFactory


def _mask_get_max_events(mask):
    """Return max events from mask (LastEvent - FirstEvent + 1) or None."""
    fe = mask.get("FirstEvent")
    le = mask.get("LastEvent")
    if fe is None or le is None:
        return None
    return le - fe + 1


def _mask_get_run_and_lumis(mask):
    """Return runAndLumis dict from mask."""
    return mask.get("runAndLumis", {})


def build_job_tweak_json(
    mask,
    lhe_input=False,
    chain_input_file=None,
    set_output_filename=None,
    output_module_name=None,
):
    """
    Build a dict of PSet parameter key -> value string (customTypeCms.* format)
    for edm_pset_tweak.py (cmssw-wm-tools). Matches WMCore PSetTweaks/WMTweak.makeJobTweak logic.
    """
    tweak = {}

    first_lumi = mask.get("FirstLumi")
    if first_lumi is not None:
        tweak["process.source.firstLuminosityBlock"] = (
            "customTypeCms.untracked.uint32(%s)" % first_lumi
        )

    max_events = _mask_get_max_events(mask)
    if max_events is None:
        max_events = -1
    tweak["process.maxEvents"] = (
        "customTypeCms.untracked.PSet(input=cms.untracked.int32(%s))" % max_events
    )

    first_event = mask.get("FirstEvent")
    if first_event is not None:
        tweak["process.source.firstEvent"] = (
            "customTypeCms.untracked.uint32(%s)" % first_event
        )

    first_run = mask.get("FirstRun")
    if first_run is not None:
        tweak["process.source.firstRun"] = (
            "customTypeCms.untracked.uint32(%s)" % first_run
        )
    else:
        tweak["process.source.firstRun"] = "customTypeCms.untracked.uint32(1)"

    runs = _mask_get_run_and_lumis(mask)
    lumis_to_process = []
    for run, lumi_pairs in runs.items():
        for pair in lumi_pairs:
            if len(pair) == 2:
                lumis_to_process.append(
                    "%s:%s-%s:%s" % (run, pair[0], run, pair[1])
                )
    if lumis_to_process:
        tweak["process.source.lumisToProcess"] = (
            "customTypeCms.untracked.VLuminosityBlockRange(%s)"
            % lumis_to_process
        )

    if chain_input_file:
        tweak["process.source.fileNames"] = (
            "customTypeCms.untracked.vstring([%r])" % chain_input_file
        )
        tweak["process.maxEvents"] = (
            "customTypeCms.untracked.PSet(input=cms.untracked.int32(-1))"
        )

    if set_output_filename and output_module_name:
        tweak["process.%s.fileName" % output_module_name] = (
            "customTypeCms.untracked.string(%r)" % set_output_filename
        )

    return tweak


class DummyWorkflow:
    """
    Minimal workflow object to satisfy DataStructs.Subscription:
      - .name
      - .wfType
      - .task
      - .owner
    """
    def __init__(self, name, task, wfType="Production", owner="local"):
        self.name = name
        self.task = task
        self.wfType = wfType
        self.owner = owner


def build_mc_fileset_from_request(req_doc, step_key="Step1"):
    """
    Build a single MCFakeFile matching the Step1 production settings.

    Assumptions (true for your example):
      - Pure MC production with EventBased splitting.
      - EventsPerJob == EventsPerLumi (one lumi per job).
      - FirstRun = 1, FirstLumi = 1, FirstEvent = 1.
    """
    step = req_doc[step_key]

    total_events = step["RequestNumEvents"]          # 15_000_000
    events_per_lumi = step["EventsPerLumi"]          # 830
    first_event = req_doc.get("FirstEvent", 1)
    first_lumi = req_doc.get("FirstLumi", 1)
    run_number = req_doc.get("RunNumber", 1)

    # Number of lumis to cover all events (ceil for safety)
    n_lumis = int(math.ceil(float(total_events) / float(events_per_lumi)))

    # Build a single fake MC file exactly like production does
    lfn = f"MCFakeFile-{req_doc['PrepID']}"
    f = DSFile(lfn=lfn, size=0, events=total_events, locations=set(["MCFakeSite"]))
    f["first_event"] = first_event
    f["last_event"] = first_event + total_events - 1

    # Add lumi sections to the run
    lumi_list = list(range(first_lumi, first_lumi + n_lumis))
    run = Run(run_number, *lumi_list)
    f.addRun(run)

    # Put this file into a Fileset
    fileset = Fileset(name=f"{step['StepName']}_Files", files={f})
    return fileset


def make_subscription(fileset, split_algo, task_type, workflow_name, task_name):
    """
    Build a DataStructs.Subscription with the desired split algorithm.
    """
    wf = DummyWorkflow(name=workflow_name, task=task_name, wfType=task_type, owner="DATAOPS")
    sub = Subscription(fileset=fileset,
                       workflow=wf,
                       split_algo=split_algo,
                       type=task_type)
    return sub


def generate_eventbased_jobs(request_json_path, splitting_json_path):
    """
    High-level driver:
      - read request + splitting docs
      - build an in-memory subscription for the Step1 production task
      - call EventBased splitting via SplitterFactory
      - return a flat list of job dicts, each with job_index and tweaks
        (PSetTweak JSON per step for edm_pset_tweak on the worker).
    """
    with open(request_json_path) as f:
        req = json.load(f)
    with open(splitting_json_path) as f:
        splitting_info = json.load(f)

    # Require main request keys for a stepchain
    for key in ("Step1", "StepChain", "TimePerEvent", "Memory", "PrepID"):
        if key not in req:
            raise RuntimeError("Request JSON missing required key: %s" % key)
    if "Step1" in req and "StepName" not in req["Step1"]:
        raise RuntimeError("Request Step1 missing StepName")

    # Pick the Step1 / Production entry from the splitting JSON
    prod_split = None
    for entry in splitting_info:
        if entry["taskType"] == "Production":
            prod_split = entry
            break
    if prod_split is None:
        raise RuntimeError("No Production split entry found in splitting JSON")

    split_algo = prod_split["splitAlgo"]           # "EventBased"
    split_params = prod_split["splitParams"]       # contains events_per_job, events_per_lumi, etc.

    # Build fileset and subscription for Step1
    step_key = "Step1"
    task_name = req["Step1"]["StepName"]
    fileset = build_mc_fileset_from_request(req, step_key=step_key)
    subscription = make_subscription(
        fileset=fileset,
        split_algo=split_algo,
        task_type=prod_split["taskType"],          # "Production"
        workflow_name=req.get("RequestName") or req.get("_id", "unknown"),
        task_name=task_name,
    )

    # Performance parameters (time / size / memory) from the request
    performance = {
        "timePerEvent": req["TimePerEvent"],
        "sizePerEvent": req["SizePerEvent"],
        "memoryRequirement": float(req["Memory"]),
    }

    # Instantiate the EventBased factory using DataStructs jobs (no DB / WMBS)
    splitter = SplitterFactory()  # looks up WMCore.JobSplitting.EventBased
    job_factory = splitter(subscription=subscription,
                           package="WMCore.DataStructs",
                           generators=[],
                           limit=0)

    # Call the algorithm with the same parameters WMAgent would use
    job_groups = job_factory(
        events_per_job=split_params["events_per_job"],
        events_per_lumi=split_params["events_per_lumi"],
        include_parents=split_params.get("include_parents", False),
        deterministicPileup=split_params.get("deterministicPileup", False),
        lheInputFiles=split_params.get("lheInputFiles", False),
        performance=performance,
    )

    # Flatten jobs and precompute PSetTweak JSON per job per step (consumed by edm_pset_tweak on the worker).
    # Output: job_index + tweaks only (no mask; worker uses precomputed tweaks).
    num_steps = req.get("StepChain", 1)
    jobs_out = []
    job_id = 1
    for group in job_groups:
        for job in group.jobs:
            mask = job["mask"]
            mask_dict = {
                "inclusivemask": mask.get("inclusivemask", True),
                "FirstEvent": mask["FirstEvent"],
                "LastEvent": mask["LastEvent"],
                "FirstLumi": mask["FirstLumi"],
                "LastLumi": mask["LastLumi"],
                "FirstRun": mask["FirstRun"],
                "LastRun": mask["LastRun"],
                "runAndLumis": mask.get("runAndLumis", {}),
            }
            tweaks = {}
            for step_num in range(1, num_steps + 1):
                next_step_key = "Step%d" % (step_num + 1)
                set_output_filename = None
                output_module_name = None
                if step_num < num_steps:
                    next_config = req.get(next_step_key, {})
                    output_module_name = next_config.get("InputFromOutputModule", "RAWSIMoutput")
                    set_output_filename = "file:%s.root" % output_module_name
                # Step 2+ read from previous step; path is relative to stepN/ dir (../step(N-1)/OutputModule.root)
                chain_input_file = None
                if step_num > 1:
                    step_key = "Step%d" % step_num
                    prev_output_module = req.get(step_key, {}).get("InputFromOutputModule", "RAWSIMoutput")
                    chain_input_file = "file:../step%d/%s.root" % (step_num - 1, prev_output_module)
                tweaks[str(step_num)] = build_job_tweak_json(
                    mask_dict,
                    lhe_input=split_params.get("lheInputFiles", False),
                    chain_input_file=chain_input_file,
                    set_output_filename=set_output_filename,
                    output_module_name=output_module_name,
                )
            jobs_out.append({"job_index": job_id, "tweaks": tweaks})
            job_id += 1

    return jobs_out


def parse_args():
    parser = argparse.ArgumentParser(
        description="Standalone EventBased splitter using WMCore DataStructs only. "
        "With --output-dir, writes job1.json..jobN.json (per job) and, if --psets is given, "
        "request_psets.tar.gz (request.json from --request + PSets/) for the worker.",
    )
    parser.add_argument("--request", required=True, help="Path to ReqMgr-style request JSON")
    parser.add_argument("--splitting", required=True, help="Path to splitting JSON")
    parser.add_argument("--output-dir", help="Directory for job1..jobN.json and request_psets.tar.gz (put PSets/ here for tarball)")
    parser.add_argument("--psets", help="Path to PSets directory to include in request_psets.tar.gz")
    return parser.parse_args()


def write_jobs_to_output_dir(output_dir, request_path, jobs, psets_path=None):
    """Write per-job JSON files and optionally request_psets.tar.gz to output_dir."""
    os.makedirs(output_dir, exist_ok=True)

    # Per-job JSON: job1.json, job2.json, ... (each has job_index + tweaks)
    for job in jobs:
        job_index = job["job_index"]
        with open(os.path.join(output_dir, "job%d.json" % job_index), "w") as f:
            json.dump(job, f, indent=2)

    # Tarball with request.json + PSets/ for the worker (only when --psets is given)
    tarball_path = os.path.join(output_dir, "request_psets.tar.gz")
    if psets_path is not None and os.path.isdir(psets_path):
        with tarfile.open(tarball_path, "w:gz") as tf:
            tf.add(request_path, arcname="request.json")
            tf.add(psets_path, arcname="PSets")
        print(f"Generated {len(jobs)} jobs: job1..job{len(jobs)}.json, request_psets.tar.gz in {output_dir}")
    else:
        print(f"Generated {len(jobs)} jobs: job1..job{len(jobs)}.json in {output_dir} (no --psets, skipped request_psets.tar.gz)")


if __name__ == "__main__":
    args = parse_args()

    jobs = generate_eventbased_jobs(args.request, args.splitting)

    if args.output_dir:
        write_jobs_to_output_dir(args.output_dir, args.request, jobs, psets_path=args.psets)
    else:
        print(f"Created {len(jobs)} jobs")
        for j in jobs[:5]:
            pprint.pprint(j)