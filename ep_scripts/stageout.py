#!/usr/bin/env python3
"""
Stage-out using WMCore.Storage.StageOutMgr.
Requires: SITECONFIG_PATH env var (e.g. /cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL)
          or WMAGENT_SITE_CONFIG_OVERRIDE pointing to site-local-config.xml
"""
import argparse
import logging
import os
import sys

# WMCore Storage (add src/python to PYTHONPATH)
from WMCore.Storage.StageOutMgr import StageOutMgr

logging.basicConfig(level=logging.INFO)


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
        description="Stage out files using WMCore StageOutMgr (requires SITECONFIG_PATH)")
    ap.add_argument("--lfn", action="append", required=True, help="LFN (repeat for multiple files)")
    ap.add_argument("--local", action="append", required=True, help="Local path (same order as --lfn)")
    ap.add_argument("--retries", type=int, default=3, help="Number of retries (default: 3)")
    ap.add_argument("--retry-pause", type=int, default=600, help="Seconds between retries (default: 600)")
    args = ap.parse_args()

    if len(args.lfn) != len(args.local):
        ap.error("Need same number of --lfn and --local")

    if not os.environ.get("SITECONFIG_PATH") and not os.environ.get("WMAGENT_SITE_CONFIG_OVERRIDE"):
        ap.error("SITECONFIG_PATH or WMAGENT_SITE_CONFIG_OVERRIDE must be set")

    file_list = [
        {"lfn": l, "local_path": p}
        for l, p in zip(args.lfn, args.local)
    ]

    stageout_files(file_list, retries=args.retries, retry_pause=args.retry_pause)


if __name__ == "__main__":
    main()
