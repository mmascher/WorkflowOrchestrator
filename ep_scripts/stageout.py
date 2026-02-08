#!/usr/bin/env python3
"""
Stage-out using WMCore.Storage.StageOutMgr.
Requires: SITECONFIG_PATH env var (e.g. /cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL)
          or WMAGENT_SITE_CONFIG_OVERRIDE pointing to site-local-config.xml

Either pass explicit file pairs (--lfn / --local) or discover from a stepchain request
(--request + --work-dir).
"""
import argparse
import json
import logging
import os
import sys

# WMCore Storage (add src/python to PYTHONPATH)
from WMCore.Storage.StageOutMgr import StageOutMgr

logging.basicConfig(level=logging.INFO)


def discover_files_from_request(request_path, work_dir):
    """
    Discover (lfn, local_path) pairs for steps with KeepOutput=true from a stepchain request.

    Returns list of dicts with 'lfn' and 'local_path'.
    """
    if not os.path.isfile(request_path):
        return []
    with open(request_path) as f:
        req = json.load(f)
    num_steps = req.get("StepChain", 1)
    base = req.get("UnmergedLFNBase", "/store/unmerged")
    result = []
    for n in range(1, num_steps + 1):
        step_key = "Step%d" % n
        step = req.get(step_key, {})
        if not step.get("KeepOutput", False):
            continue
        step_dir = os.path.join(work_dir, "step%d" % n)
        if n < num_steps:
            next_step = req.get("Step%d" % (n + 1), {})
            out_module = next_step.get("InputFromOutputModule", "RAWSIMoutput")
        else:
            out_module = None
            if os.path.isdir(step_dir):
                for f in os.listdir(step_dir):
                    if f.endswith(".root"):
                        out_module = f[:-5]
                        break
        if not out_module:
            continue
        local_path = os.path.join(step_dir, out_module + ".root")
        if not os.path.isfile(local_path):
            continue
        era = step.get("AcquisitionEra", "")
        primary = step.get("PrimaryDataset", "")
        proc = step.get("ProcessingString", "")
        tier = (
            out_module.replace("output", "")
            if out_module.endswith("output")
            else out_module
        )
        lfn = "%s/%s/%s/%s/%s-v3/%s.root" % (
            base, era, primary, tier, proc, out_module
        )
        result.append({"lfn": lfn, "local_path": local_path})
    return result


def stageout_files(file_list, retries=3, retry_pause=600):
    """
    Stage out files using WMCore StageOutMgr.

    file_list: list of dicts, each with:
        - 'LFN': logical file name (e.g. /store/unmerged/.../file.root)
        - 'PFN': local physical path (e.g. step6/NANOAODSIMoutput.root or file:...)
        - 'Checksums': optional dict with checksums

    Returns: list of updated file dicts with 'PFN' (destination), 'PNN', 'StageOutCommand'
    """
    # StageOutMgr loads site-local-config.xml from SITECONFIG_PATH/JobConfig/site-local-config.xml
    # and storage.json automatically (via initialiseSiteConf)
    manager = StageOutMgr()
    manager.numberOfRetries = retries
    manager.retryPauseTime = retry_pause

    staged_files = []
    for file_info in file_list:
        # Prepare file dict (PFN is local path; StageOutMgr will update it to destination PFN)
        fileToStage = {
            'LFN': file_info['lfn'],
            'PFN': file_info['local_path'].replace('file:', ''),  # Remove file: prefix if present
            'PNN': None,
            'StageOutCommand': None,
            'Checksums': file_info.get('checksums'),
        }

        try:
            # Call manager - it will try each stage-out from site config until one succeeds
            result = manager(fileToStage)
            staged_files.append(result)
            print("Staged out: %s -> %s (PNN: %s, Command: %s)" % (
                file_info['local_path'],
                result['PFN'],
                result['PNN'],
                result['StageOutCommand']
            ))
        except Exception as ex:
            print("Stage-out failed for %s: %s" % (file_info['lfn'], ex), file=sys.stderr)
            # Optionally clean up successful transfers if one fails
            manager.cleanSuccessfulStageOuts()
            raise

    return staged_files


def main():
    ap = argparse.ArgumentParser(
        description="Stage out files using WMCore StageOutMgr (requires SITECONFIG_PATH)"
    )
    ap.add_argument(
        "--request",
        metavar="request.json",
        help="Stepchain request JSON; discover files from KeepOutput steps (use with --work-dir)",
    )
    ap.add_argument(
        "--work-dir",
        metavar="DIR",
        help="Work directory containing step1/, step2/, ... (required with --request)",
    )
    ap.add_argument(
        "--lfn",
        action="append",
        help="LFN (repeat for multiple files; use with --local, or use --request instead)",
    )
    ap.add_argument(
        "--local",
        action="append",
        help="Local path (same order as --lfn)",
    )
    ap.add_argument("--retries", type=int, default=3, help="Number of retries (default: 3)")
    ap.add_argument(
        "--retry-pause",
        type=int,
        default=600,
        help="Seconds between retries (default: 600)",
    )
    args = ap.parse_args()

    if args.request is not None:
        if args.work_dir is None:
            ap.error("--work-dir is required when using --request")
        if args.lfn or args.local:
            ap.error("Do not use --lfn/--local with --request")
        file_list = discover_files_from_request(args.request, args.work_dir)
        if not file_list:
            logging.info("No files to stage (no KeepOutput steps or no .root files).")
            return
    else:
        if not args.lfn or not args.local:
            ap.error("Either use --request and --work-dir, or provide --lfn and --local")
        if len(args.lfn) != len(args.local):
            ap.error("Need same number of --lfn and --local")
        file_list = [
            {"lfn": l, "local_path": p}
            for l, p in zip(args.lfn, args.local)
        ]

    if not os.environ.get("SITECONFIG_PATH") and not os.environ.get(
        "WMAGENT_SITE_CONFIG_OVERRIDE"
    ):
        ap.error("SITECONFIG_PATH or WMAGENT_SITE_CONFIG_OVERRIDE must be set")

    stageout_files(file_list, retries=args.retries, retry_pause=args.retry_pause)


if __name__ == "__main__":
    main()
