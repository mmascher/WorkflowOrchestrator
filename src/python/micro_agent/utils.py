"""
Utility functions for the micro_agent workflow.
Includes LFN (Logical File Name) building used by stage_out.py and micro_agent_monitor.py.

Canonical source. Copy to submission directory (alongside stage_out.py) before condor_submit.
"""
import json
import os


def build_lfn(base, era, primary, proc, out_module):
    """
    Build LFN for a workflow output file.
    Format matches stage_out.py:
    {UnmergedLFNBase}/{AcquisitionEra}/{PrimaryDataset}/{tier}/{ProcessingString}-v3/{out_module}.root
    where tier is out_module with trailing "output" stripped (e.g. AODSIMoutput -> AODSIM).

    Args:
        base: UnmergedLFNBase (e.g. /store/unmerged)
        era: AcquisitionEra
        primary: PrimaryDataset
        proc: ProcessingString
        out_module: Output module name (e.g. AODSIMoutput, NANOEDMAODSIMoutput)

    Returns:
        LFN string
    """
    base = (base or "").rstrip("/")
    tier = (
        out_module.replace("output", "")
        if out_module.endswith("output")
        else out_module
    )
    return "%s/%s/%s/%s/%s-v3/%s.root" % (
        base, era, primary, tier, proc, out_module
    )


def load_step_config(request_path, step_name):
    """
    Load step config from request.json for a given step name.
    step_name is from job_report (e.g. step1, step2); request uses Step1, Step2.

    Returns dict with base, era, primary, proc, or None if not found.
    """
    if not request_path or not os.path.isfile(request_path):
        return None
    try:
        with open(request_path) as f:
            req = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if not step_name.startswith("step") or len(step_name) < 5 or not step_name[4:].isdigit():
        return None
    step_num = int(step_name[4:])
    step_key = "Step%d" % step_num
    step = req.get(step_key, {})
    if not step:
        return None
    return {
        "base": req.get("UnmergedLFNBase", "/store/unmerged"),
        "era": step.get("AcquisitionEra", ""),
        "primary": step.get("PrimaryDataset", ""),
        "proc": step.get("ProcessingString", ""),
    }


def build_lfn_for_file(file_info, request_path):
    """
    Build LFN for a file from job_report when lfn is empty.
    file_info must have step_name and module_label; uses request.json for step config.

    Returns the built LFN, or original lfn if already set or config not found.
    """
    lfn = file_info.get("lfn") or file_info.get("LFN") or ""
    if lfn:
        return lfn
    step_name = file_info.get("step_name", "")
    out_module = file_info.get("module_label", "")
    if not out_module:
        return ""
    config = load_step_config(request_path, step_name)
    if not config:
        return ""
    return build_lfn(
        config["base"], config["era"], config["primary"], config["proc"], out_module
    )
