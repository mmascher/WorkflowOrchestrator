#!/usr/bin/env python3
"""
Build and submit micro agent JDL via htcondor2.
The micro agent submits the stepchain via condor_submit in run_micro_agent.sh.
"""
import logging
import os

logger = logging.getLogger(__name__)

try:
    import htcondor2 as htcondor
except ImportError:
    htcondor = None


def _resolve_proxy(config):
    """Get proxy path from config."""
    return config.get("proxy", "") or os.environ.get("X509_USER_PROXY", "") or f"/tmp/x509up_u{os.getuid()}"


def _copy_micro_agent_assets(work_dir, wo_dir):
    """Copy ep_scripts, WMCore.zip, utils.py, sitelist to work_dir for the micro agent."""
    import shutil
    ep_scripts = os.path.join(wo_dir, "ep_scripts")
    for name in ["run_micro_agent.sh", "execute_stepchain.sh", "run.sh", "submit_env.sh",
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

    # Copy Go MAM binary if built (for MAM_IMPL=go)
    go_mam_binary = os.path.join(wo_dir, "src", "go", "micro_agent_monitor", "micro_agent_monitor")
    if os.path.isfile(go_mam_binary):
        go_dst_dir = os.path.join(work_dir, "src", "go", "micro_agent_monitor")
        os.makedirs(go_dst_dir, exist_ok=True)
        shutil.copy2(go_mam_binary, os.path.join(go_dst_dir, "micro_agent_monitor"))
        os.chmod(os.path.join(go_dst_dir, "micro_agent_monitor"), 0o755)


def build_micro_agent_jdl(work_dir, request_name, config):
    """
    Build the micro agent JDL. Copies needed assets to work_dir, then creates JDL.
    Returns path to the JDL file.
    """
    # Project root: src/python/workflow_orchestrator -> go up 3 levels
    wo_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    _copy_micro_agent_assets(work_dir, wo_dir)

    run_script = os.path.join(work_dir, "run_micro_agent.sh")
    if not os.path.isfile(run_script):
        raise FileNotFoundError(f"run_micro_agent.sh not found in {work_dir} after copying assets")

    sitelist = config.get("sitelist", "sitelist.txt")
    sitelist_path = os.path.join(work_dir, sitelist)
    if not os.path.isfile(sitelist_path):
        raise FileNotFoundError(f"Sitelist not found in {work_dir}: {sitelist}")

    proxy = _resolve_proxy(config)
    if not os.path.isfile(proxy):
        raise FileNotFoundError(f"Proxy not found: {proxy}")
    proxy_basename = os.path.basename(proxy)
    proxy_in_work = os.path.join(work_dir, proxy_basename)
    import shutil
    shutil.copy2(proxy, proxy_in_work)  # HTCondor reads proxy from CWD (work_dir) when creating job ad

    # Build transfer_input_files: use relative paths (names only).
    # HTCondor resolves these relative to CWD when spooling; we chdir(work_dir) before submit.
    transfer_items = []
    for name in sorted(os.listdir(work_dir)):
        transfer_items.append(name)
    transfer_input_files = ", ".join(transfer_items)

    # Use Go MAM when config says so and binary was copied
    go_mam = os.path.join(work_dir, "src", "go", "micro_agent_monitor", "micro_agent_monitor")
    env_line = ""
    if config.get("mam_impl") == "go" and os.path.isfile(go_mam):
        env_line = 'environment = "MAM_IMPL=go"\n\n'

    content = f"""Universe   = scheduler

Executable = run_micro_agent.sh
Arguments  = .

Log        = log/micro_agent.$(Cluster)
Output     = out/micro_agent.$(Cluster).$(Process)
Error      = err/micro_agent.$(Cluster).$(Process)

{env_line}transfer_input_files = {transfer_input_files}
should_transfer_files = YES

x509userproxy = {proxy_basename}
use_x509userproxy = True

Queue 1
"""
    jdl_path = os.path.join(work_dir, "micro_agent.jdl")
    with open(jdl_path, "w") as f:
        f.write(content)
    return jdl_path


def _do_submit(sub, schedd_name, collector):
    """Submit to remote schedd. IDTOKENS_FILE must be set for auth."""
    coll = htcondor.Collector(pool=collector)
    location = coll.locate(htcondor.DaemonType.Schedd, name=schedd_name)
    schedd = htcondor.Schedd(location)

    submitResult = schedd.submit(sub, spool=True)
    clusterId = submitResult.cluster()
    schedd.spool(submitResult)

    return clusterId


def submit_micro_agent(work_dir, request_name, request_doc, config):
    """
    Build micro agent JDL and submit via htcondor2.
    Returns True if submission succeeded, False otherwise.
    """
    work_dir = os.path.abspath(work_dir)


    try:
        jdl_path = build_micro_agent_jdl(work_dir, request_name, config)
    except Exception as e:
        logger.exception("Failed to build micro agent JDL: %s", e)
        return False

    if htcondor is None:
        logger.error("htcondor2 not available")
        return False

    if config.get("htcondor_debug"):
        htcondor.set_subsystem("TOOL")
        htcondor.param["TOOL_DEBUG"] = "D_FULLDEBUG, D_SECURITY"
        htcondor.enable_debug()
        htcondor.log(htcondor.LogLevel.FullDebug, "WorkflowOrchestrator: HTCondor debug enabled")

    try:
        with open(jdl_path) as f:
            sub = htcondor.Submit(f.read())

        schedd_name = config["schedd_name"]
        collector = config["collector"]
        idtoken = config["idtoken"]
        os.environ["IDTOKENS_FILE"] = idtoken
        cluster_id = None
        orig_cwd = os.getcwd()
        try:
            os.chdir(work_dir)  # HTCondor resolves Executable relative to CWD when spooling
            cluster_id = _do_submit(sub, schedd_name, collector)
        finally:
            os.chdir(orig_cwd)
            os.environ.pop("IDTOKENS_FILE", None)

        logger.info("Submitted micro agent for %s, cluster %s", request_name, cluster_id)
        return True
    except Exception as e:
        logger.exception("Failed to submit micro agent: %s", e)
        return False
