#!/usr/bin/env python3
"""Tests for workflow orchestrator components."""
import json
import os
import tempfile
import unittest

# Add src to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "python"))

from workflow_orchestrator.request_fetcher import (
    _normalize_request_name,
    _collect_config_cache_ids,
    get_available_requests,
)


class TestRequestFetcher(unittest.TestCase):
    def test_normalize_request_name(self):
        self.assertEqual(_normalize_request_name("request-foo"), "foo")
        self.assertEqual(_normalize_request_name("foo"), "foo")

    def test_collect_config_cache_ids(self):
        sample = os.path.join(
            os.path.dirname(__file__), "..",
            "samples", "cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792",
            "request.json"
        )
        if not os.path.isfile(sample):
            self.skipTest("Sample request.json not found")
        with open(sample) as f:
            doc = json.load(f)
        entries, url, db = _collect_config_cache_ids(doc)
        self.assertGreater(len(entries), 0)
        self.assertEqual(db, "reqmgr_config_cache")
        step_nums = [e[0] for e in entries]
        self.assertEqual(step_nums, list(range(1, len(entries) + 1)))


if __name__ == "__main__":
    unittest.main()
