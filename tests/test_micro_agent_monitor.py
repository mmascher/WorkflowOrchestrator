#!/usr/bin/env python3
"""
Tests for micro_agent.micro_agent_monitor.
"""
import json
import os
import tempfile
import unittest

# Add src/python to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "python"))

from micro_agent.micro_agent_monitor import (
    CondorLogParser,
    FrameworkJobReport,
    FileDB,
    ULOG_JOB_TERMINATED,
)


class TestCondorLogParser(unittest.TestCase):
    """Tests for CondorLogParser."""

    def test_parse_event_valid(self):
        line = "005 (10409446.0.0) 2025-02-26 10:30:00 Job terminated."
        parsed = CondorLogParser.parse_event(line)
        self.assertIsNotNone(parsed)
        event_code, cluster, proc, subproc, ts, msg = parsed
        self.assertEqual(event_code, 5)
        self.assertEqual(cluster, 10409446)
        self.assertEqual(proc, 0)
        self.assertEqual(subproc, 0)
        self.assertIn("2025-02-26", ts)
        self.assertIn("terminated", msg)

    def test_parse_event_invalid(self):
        self.assertIsNone(CondorLogParser.parse_event("not an event"))
        self.assertIsNone(CondorLogParser.parse_event(""))

    def test_iter_events(self):
        log_content = """005 (10409446.0.0) 2025-02-26 10:30:00 Job terminated.
ReturnValue = 0
JOB_Site = "T3_US_FNALLPC"

028 (10409446.0.0) 2025-02-26 10:30:01 Job ad information
ReturnValue = 0

"""
        parser = CondorLogParser(None)
        events = list(parser.iter_events(iter(log_content.splitlines())))
        self.assertEqual(len(events), 1)
        ec, cluster, proc, subproc, ts, msg, extra = events[0]
        self.assertEqual(ec, ULOG_JOB_TERMINATED)
        self.assertEqual(cluster, 10409446)
        self.assertEqual(proc, 0)
        self.assertEqual(extra.get("ReturnValue"), "0")
        self.assertEqual(extra.get("JOB_Site"), "T3_US_FNALLPC")


class TestFrameworkJobReport(unittest.TestCase):
    """Tests for FrameworkJobReport."""

    def test_find_empty_suffix(self):
        """job_report.X.Y..json (double dot, empty NumJobCompletions) should be found."""
        samples_dir = os.path.join(os.path.dirname(__file__), "..", "samples", "micro_agent")
        path = FrameworkJobReport.find(samples_dir, 10409446, 0)
        self.assertIsNotNone(path)
        self.assertIn("job_report.10409446.0..json", path)

    def test_extract_files_sample(self):
        """Extract files from the real sample job report."""
        samples_dir = os.path.join(os.path.dirname(__file__), "..", "samples", "micro_agent")
        path = FrameworkJobReport.find(samples_dir, 10409446, 0)
        self.assertIsNotNone(path)
        files, err = FrameworkJobReport.extract_files(path)
        self.assertIsNone(err)
        self.assertGreater(len(files), 0)
        f = files[0]
        self.assertIn("lfn", f)
        self.assertIn("pfn", f)
        self.assertIn("step_name", f)
        self.assertIn("role", f)
        self.assertIn(f["role"], ("input", "output"))

    def test_find_with_retries(self):
        """When multiple reports exist, pick highest NumJobCompletions."""
        with tempfile.TemporaryDirectory() as d:
            for n in [1, 3, 2]:
                p = os.path.join(d, f"job_report.100.0.{n}.json")
                with open(p, "w") as f:
                    json.dump({"steps": {}}, f)
            path = FrameworkJobReport.find(d, 100, 0)
            self.assertIsNotNone(path)
            self.assertIn("job_report.100.0.3.json", path)

    def test_find_empty_suffix_in_temp_dir(self):
        """Explicit test for job_report.X.Y..json in temp dir."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "job_report.99.1..json")
            with open(p, "w") as f:
                json.dump({"steps": {}}, f)
            path = FrameworkJobReport.find(d, 99, 1)
            self.assertIsNotNone(path)
            self.assertTrue(path.endswith("job_report.99.1..json"))

    def test_extract_files_minimal(self):
        """Minimal job report with input and output."""
        report = {
            "steps": {
                "step1": {
                    "input": {
                        "source": [
                            {"lfn": "/store/foo.root", "pfn": "root://host/store/foo.root", "events": 100}
                        ]
                    },
                    "output": {
                        "output": [
                            {"lfn": "", "pfn": "out.root", "events": 100}
                        ]
                    },
                }
            }
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tf:
            try:
                json.dump(report, tf)
                tf.close()
                files, err = FrameworkJobReport.extract_files(tf.name)
                self.assertIsNone(err)
                self.assertEqual(len(files), 2)
                roles = {f["role"] for f in files}
                self.assertIn("input", roles)
                self.assertIn("output", roles)
            finally:
                os.unlink(tf.name)


class TestFileDB(unittest.TestCase):
    """Tests for FileDB."""

    def test_insert_and_query(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
            tf.close()
            try:
                db = FileDB(tf.name)
                files = [
                    {"lfn": "/store/a.root", "pfn": "root://x/a.root", "step_name": "s1", "role": "input", "events": 10},
                    {"lfn": "", "pfn": "out.root", "step_name": "s1", "role": "output", "events": 10},
                ]
                db.insert_files("10409446.0", files)
                cur = db.conn.execute("SELECT lfn, pfn, role FROM processed_files")
                rows = cur.fetchall()
                self.assertEqual(len(rows), 2)
                db.close()
            finally:
                os.unlink(tf.name)

    def test_process_terminated_job_with_sample(self):
        """Full flow: process_terminated_job with real sample report."""
        samples_dir = os.path.join(os.path.dirname(__file__), "..", "samples", "micro_agent")
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
            tf.close()
            try:
                db = FileDB(tf.name)
                ok, result = db.process_terminated_job(samples_dir, 10409446, 0, return_value=0)
                self.assertTrue(ok)
                self.assertIsInstance(result, int)
                self.assertGreater(result, 0)
                cur = db.conn.execute("SELECT COUNT(*) FROM processed_files")
                self.assertEqual(cur.fetchone()[0], result)
                db.close()
            finally:
                os.unlink(tf.name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
