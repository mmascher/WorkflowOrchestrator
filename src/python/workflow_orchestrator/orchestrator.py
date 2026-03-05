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


def run_orchestrator(config):
    """
    Main orchestrator loop. Polls ReqMgr for staged StepChain requests,
    fetches data, and submits micro agents.

    Args:
        config: Config dict (from load_config)
    """
    from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
    from workflow_orchestrator.micro_agent_submitter import submit_micro_agent

    reqmgr_url = config.get("reqmgr_url", "https://cmsweb.cern.ch/reqmgr2")
    status = config.get("status", "staged")
    request_type = config.get("request_type", "StepChain")
    poll_interval = config.get("poll_interval", 300)
    work_dir = os.path.abspath(config.get("work_dir", "work"))
    proxy = config.get("proxy")
    cert = proxy  # for fetch_request_data (config cache)

    header = {}
    if proxy:
        header["cert"] = proxy
        header["key"] = proxy
    reqmgr = ReqMgr(url=reqmgr_url, header=header)

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
                        if submit_micro_agent(work_dir=req_work_dir, request_name=request_name, request_doc=request_doc, config=config):
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
