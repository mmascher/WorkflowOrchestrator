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
    load_keep_output_steps,
    ULOG_JOB_TERMINATED,
)
from micro_agent.utils import build_lfn, build_lfn_for_file, load_step_config


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


    def test_load_keep_output_steps(self):
        """load_keep_output_steps reads KeepOutput from request.json."""
        request_path = os.path.join(
            os.path.dirname(__file__), "..",
            "samples", "cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792",
            "request.json",
        )
        steps = load_keep_output_steps(request_path)
        self.assertIsNotNone(steps)
        self.assertIn("step4", steps)
        self.assertIn("step5", steps)
        self.assertIn("step6", steps)
        self.assertNotIn("step1", steps)

    def test_build_lfn(self):
        """build_lfn produces correct format."""
        lfn = build_lfn(
            "/store/unmerged",
            "RunIISummer20UL17pp5TeVRECO",
            "WplusJets",
            "106X_mc2017_realistic_forppRef5TeV_v3",
            "AODSIMoutput",
        )
        self.assertTrue(lfn.startswith("/store/unmerged/"))
        self.assertIn("AODSIM/", lfn)
        self.assertTrue(lfn.endswith("AODSIMoutput.root"))

    def test_build_lfn_for_file_with_request(self):
        """build_lfn_for_file builds LFN when lfn empty and request provided."""
        request_path = os.path.join(
            os.path.dirname(__file__), "..",
            "samples", "cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792",
            "request.json",
        )
        f = {"lfn": "", "step_name": "step4", "module_label": "AODSIMoutput"}
        built = build_lfn_for_file(f, request_path)
        self.assertTrue(built.startswith("/store/"))
        self.assertIn("AODSIM", built)
        self.assertTrue(built.endswith("AODSIMoutput.root"))

    def test_extract_files_keep_output_only(self):
        """keep_output_steps filters to outputs from those steps."""
        samples_dir = os.path.join(os.path.dirname(__file__), "..", "samples", "micro_agent")
        path = FrameworkJobReport.find(samples_dir, 10409446, 0)
        self.assertIsNotNone(path)
        steps = {"step4", "step5", "step6"}
        files, _ = FrameworkJobReport.extract_files(path, keep_output_steps=steps)
        self.assertEqual(len(files), 3)
        step_names = {f["step_name"] for f in files}
        self.assertEqual(step_names, steps)

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
        """Minimal job report: outputs only."""
        report = {
            "steps": {
                "step1": {
                    "input": {"source": [{"lfn": "/store/foo.root", "pfn": "root://host/store/foo.root"}]},
                    "output": {"output": [{"lfn": "", "pfn": "out.root", "events": 100}]},
                }
            }
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tf:
            try:
                json.dump(report, tf)
                tf.close()
                files, err = FrameworkJobReport.extract_files(tf.name)
                self.assertIsNone(err)
                self.assertEqual(len(files), 1)
                self.assertEqual(files[0]["pfn"], "out.root")
                self.assertEqual(files[0].get("pnn"), "")
            finally:
                os.unlink(tf.name)

    def test_extract_files_with_pnn(self):
        """Extract pfn and pnn from stage-out merged report."""
        report = {
            "steps": {
                "step6": {
                    "output": {
                        "NANOAODSIMoutput": [
                            {"lfn": "/store/unmerged/era/ds/tier/proc-v3/NANOAODSIMoutput.root", "pfn": "root://eos/store/.../file.root", "pnn": "T2_CH_CERN"}
                        ]
                    }
                }
            }
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tf:
            try:
                json.dump(report, tf)
                tf.close()
                files, err = FrameworkJobReport.extract_files(tf.name)
                self.assertIsNone(err)
                self.assertEqual(len(files), 1)
                self.assertEqual(files[0]["pfn"], "root://eos/store/.../file.root")
                self.assertEqual(files[0]["pnn"], "T2_CH_CERN")
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
                    {"lfn": "/store/a.root", "pfn": "root://x/a.root", "pnn": "T2_CH_CERN", "step_name": "s1", "events": 10},
                    {"lfn": "", "pfn": "out.root", "step_name": "s1", "events": 10},
                ]
                db.insert_files("10409446.0", files, glidein_cmssite="T3_US_FNALLPC")
                cur = db.conn.execute("SELECT lfn, pfn, glidein_cmssite, pnn FROM processed_files")
                rows = cur.fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][2], "T3_US_FNALLPC")
                self.assertEqual(rows[0][3], "T2_CH_CERN")
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

    def test_process_terminated_job_builds_lfn_with_request(self):
        """When request_path given, empty LFNs are built from request.json."""
        samples_dir = os.path.join(os.path.dirname(__file__), "..", "samples", "micro_agent")
        request_path = os.path.join(
            os.path.dirname(__file__), "..",
            "samples", "cmsunified_task_SMP-RunIISummer20UL17pp5TeVwmLHEGS-00007__v1_T_251014_173511_792",
            "request.json",
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
            tf.close()
            try:
                db = FileDB(tf.name)
                ok, result = db.process_terminated_job(
                    samples_dir, 10409446, 0, return_value=0, request_path=request_path
                )
                self.assertTrue(ok)
                cur = db.conn.execute("SELECT lfn FROM processed_files WHERE lfn != '' LIMIT 1")
                row = cur.fetchone()
                self.assertIsNotNone(row)
                self.assertTrue(row[0].startswith("/store/"))
                self.assertTrue(row[0].endswith(".root"))
                db.close()
            finally:
                os.unlink(tf.name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
