#!/usr/bin/env python3
"""
Fetch request data from ReqMgr: request document, splitting config, and PSets from reqmgr_config_cache.

Uses getGenericRequestInfo for StepChain requests, getRequestTasks for splitting,
and fetches config files from {ConfigCacheUrl}/{CouchDBName}/{ConfigCacheID}/configFile.
"""
import json
import logging
import os
from operator import itemgetter

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


def _normalize_request_name(name):
    """Strip 'request-' prefix if present."""
    if isinstance(name, str) and name.startswith("request-"):
        return name[len("request-"):]
    return name


def get_available_requests(reqmgr, status="staged", request_type="StepChain"):
    """
    Query ReqMgr for available requests using getGenericRequestInfo.
    Returns list of (request_name, request_doc) sorted by priority (desc) and team.
    """
    query = {
        "status": status,
        "request_type": request_type,
        "detail": True,
    }
    result = reqmgr.getGenericRequestInfo(query)
    if not result:
        return []

    # Flatten: result is list of dicts [{name: doc}, ...]
    requests_list = []
    for item in result:
        if isinstance(item, dict):
            for req_name, req_doc in item.items():
                if isinstance(req_doc, dict):
                    requests_list.append((req_name, req_doc))

    # Sort by RequestPriority desc, then Team
    requests_list.sort(key=itemgetter(0))  # stable
    requests_list.sort(key=lambda x: x[1].get("RequestPriority", 0), reverse=True)
    requests_list.sort(key=lambda x: x[1].get("Team", ""))

    return requests_list


def fetch_splitting(reqmgr, request_name):
    """Fetch splitting config via getRequestTasks. Returns list of task configs."""
    name = _normalize_request_name(request_name)
    try:
        return reqmgr.getRequestTasks(name)
    except Exception as e:
        logger.exception("Failed to fetch splitting for %s: %s", request_name, e)
        raise


def _fetch_config_from_cache(config_cache_url, config_cache_db, config_id, cert=None, timeout=60):
    """
    Fetch config file from reqmgr_config_cache.
    URL: {config_cache_url}/{config_cache_db}/{config_id}/configFile

    Returns the config content as string (Python PSet). Handles both:
    - JSON doc with 'config' or 'configFile' key
    - Raw attachment/content
    """
    base = config_cache_url.rstrip("/")
    url = f"{base}/{config_cache_db}/{config_id}/configFile"
    if requests is None:
        raise RuntimeError("requests library required for fetching config cache")

    kwargs = {"timeout": timeout}
    if cert:
        kwargs["cert"] = cert

    resp = requests.get(url, **kwargs)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")

    if "application/json" in content_type:
        data = resp.json()
        if isinstance(data, dict):
            if "config" in data:
                return data["config"]
            if "configFile" in data:
                return data["configFile"]
        return resp.text

    return resp.text


def _collect_config_cache_ids(request_doc):
    """
    Collect (step_num, step_name, config_id) for each step from request.
    Returns list of (step_num, step_name, config_id).
    """
    step_chain = request_doc.get("StepChain", 1)
    if not isinstance(step_chain, int):
        step_chain = 1

    config_cache_url = request_doc.get("ConfigCacheUrl") or request_doc.get("CouchURL", "")
    config_cache_db = request_doc.get("CouchDBName", "reqmgr_config_cache")

    if not config_cache_url or not config_cache_db:
        return [], config_cache_url, config_cache_db

    entries = []
    for i in range(1, step_chain + 1):
        step_key = f"Step{i}"
        step = request_doc.get(step_key, {})
        if not isinstance(step, dict):
            continue
        config_id = step.get("ConfigCacheID")
        step_name = step.get("StepName", step_key)
        if config_id:
            entries.append((i, step_name, config_id))

    return entries, config_cache_url, config_cache_db


def fetch_psets(request_doc, output_dir, cert=None):
    """
    Fetch PSets from reqmgr_config_cache for each step and write to output_dir/PSets/.
    Filename pattern: PSet_cmsRun{N}_{StepName}.py

    Returns path to PSets directory, or None if no configs could be fetched.
    """
    entries, config_cache_url, config_cache_db = _collect_config_cache_ids(request_doc)
    if not entries:
        logger.warning("No ConfigCacheID found in request")
        return None

    psets_dir = os.path.join(output_dir, "PSets")
    os.makedirs(psets_dir, exist_ok=True)

    for step_num, step_name, config_id in entries:
        filename = f"PSet_cmsRun{step_num}_{step_name}.py"
        out_path = os.path.join(psets_dir, filename)
        try:
            content = _fetch_config_from_cache(
                config_cache_url, config_cache_db, config_id, cert=cert
            )
            with open(out_path, "w") as f:
                f.write(content)
            logger.info("Fetched PSet for step %d: %s", step_num, filename)
        except Exception as e:
            logger.exception("Failed to fetch config %s: %s", config_id, e)
            raise

    return psets_dir


def fetch_request_data(reqmgr, request_name, request_doc, work_dir, cert=None):
    """
    Fetch and persist all data needed for a micro agent: request.json, splitting.json, PSets/.

    Args:
        reqmgr: ReqMgr service instance
        request_name: Request name (for getRequestTasks)
        request_doc: Full request document (from getGenericRequestInfo)
        work_dir: Directory to write files into
        cert: Optional path to x509 cert for config cache fetch

    Returns:
        (request_path, splitting_path, psets_path)
        psets_path may be None if no PSets could be fetched.
    """
    os.makedirs(work_dir, exist_ok=True)

    request_path = os.path.join(work_dir, "request.json")
    with open(request_path, "w") as f:
        json.dump(request_doc, f, indent=2)
    logger.info("Wrote request.json to %s", request_path)

    splitting = fetch_splitting(reqmgr, request_name)
    splitting_path = os.path.join(work_dir, "splitting.json")
    with open(splitting_path, "w") as f:
        json.dump(splitting, f, indent=2)
    logger.info("Wrote splitting.json to %s", splitting_path)

    psets_path = fetch_psets(request_doc, work_dir, cert=cert)

    return request_path, splitting_path, psets_path
