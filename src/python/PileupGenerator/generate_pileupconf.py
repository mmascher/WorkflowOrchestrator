#!/usr/bin/env python3
"""
Standalone pileupconf.json generator.

Queries DBS, MSPileup, and Rucio to build the same pileup configuration JSON
that WMCore's PileupFetcher produces -- without needing WMCore step/task objects.

Usage examples:

    # Minimal -- just a dataset, defaults to prod instance
    python generate_pileupconf.py \\
        --dataset /MinBias_TuneCP5_5TeV-pythia8/.../GEN-SIM

    # Explicit instance and output path
    python generate_pileupconf.py \\
        --dataset /MinBias_.../GEN-SIM \\
        --instance testbed \\
        --output pileupconf.json

    # Override individual service URLs
    python generate_pileupconf.py \\
        --dataset /MinBias_.../GEN-SIM \\
        --instance prod \\
        --dbs-url https://custom-dbs/endpoint

    # Auto-extract from request.json (scans StepN for MCPileup / DataPileup)
    python generate_pileupconf.py \\
        --request-json /path/to/request.json
"""

import argparse
import json
import logging
import sys

# ---------------------------------------------------------------------------
# Instance presets
# ---------------------------------------------------------------------------
INSTANCE_URLS = {
    "prod": {
        "dbs_url": "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader",
        "mspileup_url": "https://cmsweb.cern.ch/ms-pileup/data/pileup",
        "rucio_auth_url": "https://cms-rucio-auth.cern.ch",
        "rucio_host_url": "http://cms-rucio.cern.ch",
    },
    "testbed": {
        "dbs_url": "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader",
        "mspileup_url": "https://cmsweb-testbed.cern.ch/ms-pileup/data/pileup",
        "rucio_auth_url": "https://cms-rucio-auth-int.cern.ch",
        "rucio_host_url": "http://cms-rucio-int.cern.ch",
    },
}

# Mapping from request.json field names to internal pileup type keys
# (mirrors WMCore.WMSpec.WMWorkloadTools.parsePileupConfig)
PILEUP_FIELD_MAP = {
    "MCPileup": "mc",
    "DataPileup": "data",
}

RUCIO_ACCOUNT = "wmcore_pileup"


# ---------------------------------------------------------------------------
# Service query helpers
# ---------------------------------------------------------------------------
def query_dbs(dbs_url, dataset):
    """
    Query DBS for file-level detail and organise results by block.

    Returns a dict:
        {block_name: {"FileList": [lfn, ...],
                      "NumberOfEvents": int,
                      "PhEDExNodeNames": []}}
    """
    from WMCore.Services.DBS.DBSReader import DBSReader

    logging.info("Querying DBS at %s for dataset %s", dbs_url, dataset)
    dbs_reader = DBSReader(dbs_url)

    block_dict = {}
    file_count = 0
    for file_info in dbs_reader.getFileListByDataset(dataset=dataset, detail=True):
        block_name = file_info["block_name"]
        block_dict.setdefault(block_name, {"FileList": [],
                                           "NumberOfEvents": 0,
                                           "PhEDExNodeNames": []})
        block_dict[block_name]["FileList"].append(file_info["logical_file_name"])
        block_dict[block_name]["NumberOfEvents"] += file_info["event_count"]
        file_count += 1

    logging.info("Found %d blocks in DBS for dataset %s with %d files",
                 len(block_dict), dataset, file_count)
    return block_dict


def query_mspileup(mspileup_url, dataset):
    """
    Query MSPileup for pileup document of the given dataset.

    Returns the first matching document dict with keys such as
    ``pileupName``, ``customName``, ``containerFraction``, ``currentRSEs``.
    """
    from WMCore.Services.MSUtils.MSUtils import getPileupDocs

    logging.info("Querying MSPileup at %s for dataset %s", mspileup_url, dataset)
    query_dict = {
        "query": {"pileupName": dataset},
        "filters": ["pileupName", "customName", "containerFraction", "currentRSEs"],
    }
    docs = getPileupDocs(mspileup_url, query_dict, method="POST")
    if not docs:
        raise RuntimeError(f"No MSPileup document found for dataset {dataset}")

    doc = docs[0]
    logging.info(
        "Pileup dataset %s -- customName: %s, currentRSEs: %s, containerFraction: %s",
        doc["pileupName"], doc.get("customName"), doc.get("currentRSEs"),
        doc.get("containerFraction"),
    )
    return doc


def filter_blocks_with_rucio(block_dict, dataset, ms_doc, rucio_auth_url, rucio_host_url):
    """
    Use Rucio to verify which blocks actually exist in the container and
    set ``PhEDExNodeNames`` from the MSPileup ``currentRSEs``.

    Blocks present in DBS but absent from Rucio are removed in-place.
    """
    # If MSPileup reports a customName, use it as the Rucio container with
    # the ``group.wmcore`` scope (container-fraction workflow).
    container = dataset
    scope = "cms"
    custom_name = ms_doc.get("customName")
    if custom_name:
        container = custom_name
        scope = "group.wmcore"

    from WMCore.Services.Rucio.Rucio import Rucio

    logging.info("Querying Rucio (auth=%s, host=%s) for container %s, scope=%s",
                 rucio_auth_url, rucio_host_url, container, scope)

    rucio = Rucio(RUCIO_ACCOUNT,
                  authUrl=rucio_auth_url,
                  hostUrl=rucio_host_url)
    rucio_blocks = rucio.getBlocksInContainer(container=container, scope=scope)
    logging.info("Found %d blocks in Rucio container %s (scope=%s)",
                 len(rucio_blocks), container, scope)

    current_rses = ms_doc.get("currentRSEs", [])
    for block_name in list(block_dict):
        if block_name not in rucio_blocks:
            logging.warning("Block %s present in DBS but not in Rucio -- removing.", block_name)
            block_dict.pop(block_name)
        else:
            block_dict[block_name]["PhEDExNodeNames"] = current_rses

    logging.info("Final pileup for %s: %d blocks after Rucio filtering.", dataset, len(block_dict))


# ---------------------------------------------------------------------------
# High-level driver
# ---------------------------------------------------------------------------
def generate_pileupconf(datasets_by_type, dbs_url, mspileup_url,
                        rucio_auth_url, rucio_host_url):
    """
    Build the full pileup configuration dict.

    :param datasets_by_type: dict mapping pileup type to list of datasets,
        e.g. ``{"mc": ["/Some/Dataset/TIER"]}``
    :param dbs_url: DBS endpoint URL
    :param mspileup_url: MSPileup endpoint URL
    :param rucio_auth_url: Rucio auth URL
    :param rucio_host_url: Rucio host URL
    :returns: dict in the pileupconf.json format:
        ``{pileupType: {blockName: {"FileList": [...], "NumberOfEvents": N,
                                    "PhEDExNodeNames": [RSEs]}}}``
    """
    result = {}
    for pu_type, datasets in datasets_by_type.items():
        block_dict = {}
        for dataset in datasets:
            ds_blocks = query_dbs(dbs_url, dataset)
            ms_doc = query_mspileup(mspileup_url, dataset)
            filter_blocks_with_rucio(ds_blocks, dataset, ms_doc,
                                     rucio_auth_url, rucio_host_url)
            block_dict.update(ds_blocks)
        result[pu_type] = block_dict
    return result


# ---------------------------------------------------------------------------
# request.json extraction
# ---------------------------------------------------------------------------
def extract_pileup_from_request(request_path):
    """
    Scan StepN entries in a request.json for MCPileup / DataPileup fields.

    Returns a dict ``{pileup_type: [dataset, ...]}`` aggregated across all steps.
    """
    with open(request_path) as fh:
        req = json.load(fh)

    datasets_by_type = {}
    # Iterate over Step1, Step2, ... up to StepChain count (or a reasonable upper bound)
    max_steps = req.get("StepChain", req.get("TaskChain", 20))
    for step_num in range(1, max_steps + 1):
        step_key = f"Step{step_num}"
        step = req.get(step_key)
        if step is None:
            # also try TaskN for TaskChain workflows
            step_key = f"Task{step_num}"
            step = req.get(step_key)
        if step is None:
            continue

        for field, pu_type in PILEUP_FIELD_MAP.items():
            ds = step.get(field)
            if ds:
                datasets_by_type.setdefault(pu_type, [])
                if ds not in datasets_by_type[pu_type]:
                    datasets_by_type[pu_type].append(ds)

    if not datasets_by_type:
        raise RuntimeError(
            f"No MCPileup or DataPileup fields found in any step of {request_path}"
        )

    logging.info("Extracted pileup datasets from %s: %s", request_path, datasets_by_type)
    return datasets_by_type


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------
def resolve_urls(args):
    """
    Resolve final service URLs from the ``--instance`` preset plus any
    explicit per-URL overrides on the command line.

    Returns (dbs_url, mspileup_url, rucio_auth_url, rucio_host_url).
    """
    preset = INSTANCE_URLS[args.instance]
    dbs_url = args.dbs_url or preset["dbs_url"]
    mspileup_url = args.mspileup_url or preset["mspileup_url"]
    rucio_auth_url = args.rucio_auth_url or preset["rucio_auth_url"]
    rucio_host_url = args.rucio_host_url or preset["rucio_host_url"]
    return dbs_url, mspileup_url, rucio_auth_url, rucio_host_url


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Standalone pileupconf.json generator. "
        "Queries DBS, MSPileup and Rucio to produce the same JSON that "
        "WMCore PileupFetcher builds, without requiring WMCore step/task objects.",
    )

    # Dataset input (mutually exclusive: explicit dataset OR request.json)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--dataset",
        help="Pileup dataset name (e.g. /MinBias_.../GEN-SIM). "
             "Use with --pileup-type to set the pileup category.",
    )
    input_group.add_argument(
        "--request-json",
        help="Path to a ReqMgr-style request.json. "
             "MCPileup and DataPileup fields will be auto-extracted from StepN/TaskN entries.",
    )

    # Instance shortcut
    parser.add_argument(
        "--instance",
        choices=list(INSTANCE_URLS.keys()),
        default="prod",
        help="Service instance preset (default: prod). "
             "Sets DBS, MSPileup, and Rucio URLs consistently.",
    )

    # Per-URL overrides
    parser.add_argument("--dbs-url", default=None,
                        help="Override DBS URL (takes precedence over --instance).")
    parser.add_argument("--mspileup-url", default=None,
                        help="Override MSPileup URL (takes precedence over --instance).")
    parser.add_argument("--rucio-auth-url", default=None,
                        help="Override Rucio auth URL (takes precedence over --instance).")
    parser.add_argument("--rucio-host-url", default=None,
                        help="Override Rucio host URL (takes precedence over --instance).")

    # Pileup type (only relevant when --dataset is used)
    parser.add_argument(
        "--pileup-type",
        default="mc",
        help="Pileup type key for the output JSON (default: mc). "
             "Only used with --dataset; ignored when --request-json is provided.",
    )

    # Output
    parser.add_argument(
        "--output", "-o",
        default="pileupconf.json",
        help="Output file path (default: pileupconf.json).",
    )

    # Verbosity
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG-level) logging.",
    )

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    # Build datasets_by_type mapping
    if args.request_json:
        datasets_by_type = extract_pileup_from_request(args.request_json)
    else:
        datasets_by_type = {args.pileup_type: [args.dataset]}

    # Resolve service URLs
    dbs_url, mspileup_url, rucio_auth_url, rucio_host_url = resolve_urls(args)

    logging.info("Service URLs -- DBS: %s | MSPileup: %s | Rucio auth: %s | Rucio host: %s",
                 dbs_url, mspileup_url, rucio_auth_url, rucio_host_url)
    logging.info("Pileup datasets to process: %s", datasets_by_type)

    # Generate the pileup configuration
    pileup_conf = generate_pileupconf(
        datasets_by_type,
        dbs_url=dbs_url,
        mspileup_url=mspileup_url,
        rucio_auth_url=rucio_auth_url,
        rucio_host_url=rucio_host_url,
    )

    # Write output
    with open(args.output, "w") as fh:
        json.dump(pileup_conf, fh, indent=2)

    # Summary
    total_blocks = sum(len(blocks) for blocks in pileup_conf.values())
    total_files = sum(
        len(info["FileList"])
        for blocks in pileup_conf.values()
        for info in blocks.values()
    )
    logging.info("Wrote %s -- %d pileup type(s), %d block(s), %d file(s)",
                 args.output, len(pileup_conf), total_blocks, total_files)


if __name__ == "__main__":
    main()
