#!/usr/bin/env python3
"""
Build and submit micro agent JDL via htcondor2.
The micro agent submits the stepchain via condor_submit in run_micro_agent.sh.
"""
import logging
import os

logger = logging.getLogger(__name__)

try:
    import htcondor
except ImportError:
    htcondor = None


def _resolve_proxy(config):
    """Resolve proxy path from config (may contain $(id -u))."""
    proxy = config.get("proxy", "") or os.environ.get("X509_USER_PROXY", "")
    if "$(id -u)" in proxy:
        import subprocess
        try:
            uid = subprocess.check_output(["id", "-u"], text=True).strip()
            proxy = proxy.replace("$(id -u)", uid)
        except Exception:
            proxy = "/tmp/x509up_u" + str(os.getuid())
    return proxy or f"/tmp/x509up_u{os.getuid()}"


def _copy_micro_agent_assets(work_dir, wo_dir):
    """Copy ep_scripts, WMCore.zip, utils.py, sitelist to work_dir for the micro agent."""
    import shutil
    ep_scripts = os.path.join(wo_dir, "ep_scripts")
    for name in ["run_micro_agent.sh", "execute_stepchain.sh", "submit_env.sh",
                 "stage_out.py", "create_report.py"]:
        src = os.path.join(ep_scripts, name)
        if os.path.isfile(src):
            shutil.copy2(src, work_dir)

    wmcore_zip = os.path.join(wo_dir, "samples", "htcondor", "WMCore.zip")
    if os.path.isfile(wmcore_zip):
        shutil.copy2(wmcore_zip, work_dir)

    utils_src = os.path.join(wo_dir, "src", "python", "micro_agent", "utils.py")
    if os.path.isfile(utils_src):
        shutil.copy2(utils_src, work_dir)

    sitelist_src = os.path.join(wo_dir, "samples", "htcondor", "sitelist.txt")
    if os.path.isfile(sitelist_src) and not os.path.isfile(os.path.join(work_dir, "sitelist.txt")):
        shutil.copy2(sitelist_src, work_dir)

    # Copy src/python for job_splitters and micro_agent modules
    src_python = os.path.join(work_dir, "src", "python")
    os.makedirs(src_python, exist_ok=True)
    for mod in ["job_splitters", "micro_agent"]:
        src_mod = os.path.join(wo_dir, "src", "python", mod)
        if os.path.isdir(src_mod):
            dst_mod = os.path.join(src_python, mod)
            if os.path.exists(dst_mod):
                shutil.rmtree(dst_mod)
            shutil.copytree(src_mod, dst_mod)


def build_micro_agent_jdl(work_dir, request_name, config):
    """
    Build the micro agent JDL. Copies needed assets to work_dir, then creates JDL.
    Returns path to the JDL file.
    """
    wo_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    _copy_micro_agent_assets(work_dir, wo_dir)

    sitelist = config.get("sitelist", "sitelist.txt")
    sitelist_path = os.path.join(work_dir, sitelist)
    if not os.path.isfile(sitelist_path):
        raise FileNotFoundError(f"Sitelist not found in {work_dir}: {sitelist}")

    proxy = _resolve_proxy(config)

    content = f"""Universe   = vanilla

Executable = run_micro_agent.sh
Arguments  = {work_dir}

Log        = log/run.$(Cluster)
Output     = out/run.$(Cluster).$(Process)
Error      = err/run.$(Cluster).$(Process)

should_transfer_files = YES
when_to_transfer_output = ON_EXIT

x509userproxy = {proxy}
use_x509userproxy = True

request_cpus = 1
request_memory = 1000
+MaxWallTimeMins = 1440

InitialDir = {work_dir}

Queue 1
"""
    jdl_path = os.path.join(work_dir, "micro_agent.jdl")
    with open(jdl_path, "w") as f:
        f.write(content)
    return jdl_path


def submit_micro_agent(work_dir, request_name, request_doc, config):
    """
    Build micro agent JDL and submit via htcondor2.
    Returns True if submission succeeded, False otherwise.
    """
    work_dir = os.path.abspath(work_dir)
    os.makedirs(os.path.join(work_dir, "log"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "out"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "err"), exist_ok=True)

    try:
        jdl_path = build_micro_agent_jdl(work_dir, request_name, config)
    except Exception as e:
        logger.exception("Failed to build micro agent JDL: %s", e)
        return False

    if htcondor is None:
        logger.error("htcondor2 not available")
        return False

    try:
        with open(jdl_path) as f:
            sub = htcondor.Submit(f.read())
        schedd = htcondor.Schedd()
        result = schedd.submit(sub)
        cluster_id = result.cluster()
        logger.info("Submitted micro agent for %s, cluster %s", request_name, cluster_id)
        return True
    except Exception as e:
        logger.exception("Failed to submit micro agent: %s", e)
        return False
