#!/usr/bin/env python3
"""
Workflow Orchestrator daemon: queries ReqMgr for staged StepChain requests,
fetches request/splitting/PSet data, and submits one micro agent condor job per request.
"""
import logging
import os
import time
import yaml

from workflow_orchestrator.request_fetcher import (
    get_available_requests,
    fetch_request_data,
)

logger = logging.getLogger(__name__)


def load_config(config_path=None):
    """Load orchestrator config from YAML. Returns dict."""
    if config_path and os.path.isfile(config_path):
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def run_orchestrator(config, reqmgr=None, submitter=None):
    """
    Main orchestrator loop. Polls ReqMgr for staged StepChain requests,
    fetches data, and submits micro agents.

    Args:
        config: Config dict (from load_config)
        reqmgr: ReqMgr instance (optional, created if None)
        submitter: Micro agent submitter callable (work_dir, request_name, request_doc) -> bool
    """
    from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

    reqmgr_url = config.get("reqmgr_url", "https://cmsweb.cern.ch/reqmgr2")
    status = config.get("status", "staged")
    request_type = config.get("request_type", "StepChain")
    poll_interval = config.get("poll_interval", 300)
    work_dir = os.path.abspath(config.get("work_dir", "work"))
    proxy = config.get("proxy", "")
    cert = config.get("cert") or proxy

    if reqmgr is None:
        reqmgr = ReqMgr(url=reqmgr_url)

    if submitter is None:
        from workflow_orchestrator.micro_agent_submitter import submit_micro_agent
        submitter = lambda wd, name, doc: submit_micro_agent(
            work_dir=wd,
            request_name=name,
            request_doc=doc,
            config=config,
        )

    os.makedirs(work_dir, exist_ok=True)

    while True:
        try:
            requests_list = get_available_requests(
                reqmgr, status=status, request_type=request_type
            )
            if not requests_list:
                logger.info("No requests in status=%s request_type=%s", status, request_type)
            else:
                logger.info("Found %d request(s)", len(requests_list))
                for request_name, request_doc in requests_list:
                    try:
                        req_work_dir = os.path.join(work_dir, request_name.replace("/", "_"))
                        request_path, splitting_path, psets_path = fetch_request_data(
                            reqmgr, request_name, request_doc, req_work_dir, cert=cert
                        )
                        if submitter(req_work_dir, request_name, request_doc):
                            logger.info("Submitted micro agent for %s", request_name)
                        else:
                            logger.warning("Failed to submit micro agent for %s", request_name)
                    except Exception as e:
                        logger.exception("Error processing %s: %s", request_name, e)
        except Exception as e:
            logger.exception("Error in orchestrator loop: %s", e)

        logger.info("Sleeping %d seconds", poll_interval)
        time.sleep(poll_interval)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Workflow Orchestrator daemon")
    parser.add_argument("--config", default="config/orchestrator.yaml", help="Config file path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    )

    config = load_config(args.config)
    run_orchestrator(config)


if __name__ == "__main__":
    main()
