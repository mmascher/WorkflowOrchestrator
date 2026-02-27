#!/usr/bin/env python3
"""
Micro Agent Monitor (MAM): monitors HTCondor job log files and records file-level
processing information in a local SQLite database.

Parses the condor user log (JDL Log macro, e.g. log/run.<Cluster>) for job events.
On JOB_TERMINATED (success), reads the framework job_report JSON and stores output file
information in SQLite. File-centric, not job-centric.

Usage:
  python -m micro_agent.micro_agent_monitor --log log/run.10372180 --results-dir results --db micro_agent.db
  python -m micro_agent.micro_agent_monitor --log log/run.10372180 --results-dir results --db micro_agent.db --once

Can run in daemon mode (default) tailing the log, or --once for a single pass.
"""
import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time

from micro_agent.utils import build_lfn_for_file

logger = logging.getLogger("micro_agent_monitor")

# Daemon: full re-read interval (seconds). Read only new lines between re-reads.
FULL_REREAD_INTERVAL = 3600  # 1 hour

# HTCondor user log event codes (from Job Event Log Codes)
ULOG_SUBMIT = 0
ULOG_EXECUTE = 1
ULOG_JOB_TERMINATED = 5
ULOG_IMAGE_SIZE = 6
ULOG_JOB_ABORTED = 9
ULOG_JOB_EVICTED = 4
ULOG_JOB_AD = 28


class CondorLogParser:
    """
    Parse HTCondor user log files and framework job reports.
    Event format: NNN (Cluster.Proc.Subproc) YYYY-MM-DD HH:MM:SS message
    Event codes: 0=submit, 1=execute, 5=terminated, 28=job ad (supplements other events).
    """

    def __init__(self, log_path):
        self.log_path = os.path.abspath(log_path) if log_path else None
        self.last_position = 0  # file offset after last iter_log_file run

    @staticmethod
    def parse_event(line):
        """
        Parse a condor user log event header line.
        Returns (event_code, cluster, proc, subproc, timestamp_str, message) or None.
        """
        m = re.match(
            r"^(\d{3})\s+\((\d+)\.(\d+)\.(\d+)\)\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(.*)$",
            line,
        )
        if not m:
            return None
        return (
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
            int(m.group(4)),
            m.group(5),
            m.group(6).strip(),
        )

    @staticmethod
    def _parse_key_value(line):
        """Parse 'Key = Value' line. Returns (key, val) or None."""
        kv = re.match(r"^(\w+)\s*=\s*(.*)$", line.strip())
        if not kv:
            return None
        key, val = kv.group(1), kv.group(2).strip()
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        return (key, val)

    def _read_key_values_until_stop(self, line_iter, extra):
        """
        Read Key=Value lines from line_iter, add to extra dict.
        Stop when we see: next event header, or empty line.
        Returns: next line to process (event header, or first line after empty), or None if exhausted.
        """
        try:
            while True:
                line = next(line_iter)
                if self.parse_event(line) is not None:
                    return line
                if line.strip() == "":
                    try:
                        return next(line_iter)
                    except StopIteration:
                        return None
                parsed = self._parse_key_value(line)
                if parsed:
                    key, val = parsed
                    extra[key] = val
        except StopIteration:
            return None

    def iter_events(self, line_iter):
        """
        Yield (event_code, cluster, proc, subproc, timestamp, message, extra_dict) from line_iter.
        Streams line-by-line. For JOB_TERMINATED (005), merges following 028 (Job ad) into extra.
        """
        line = None
        while True:
            if line is None:
                try:
                    line = next(line_iter)
                except StopIteration:
                    break

            parsed = self.parse_event(line)
            if not parsed:
                line = None
                continue

            event_code, cluster, proc, subproc, timestamp, message = parsed
            extra = {}
            line = self._read_key_values_until_stop(line_iter, extra)

            # 005 (JOB_TERMINATED) is followed by 028 (Job ad) for same job.
            # Absorb 028's attributes (ReturnValue, JOB_Site, etc.) into extra.
            if line is not None:
                next_parsed = self.parse_event(line)
                if (
                    next_parsed is not None
                    and event_code == ULOG_JOB_TERMINATED
                    and next_parsed[0] == ULOG_JOB_AD
                    and (cluster, proc) == (next_parsed[1], next_parsed[2])
                ):
                    line = self._read_key_values_until_stop(line_iter, extra)

            yield (event_code, cluster, proc, subproc, timestamp, message, extra)

    def iter_log_file(self, start_offset=0):
        """
        Yield events from log file. Streams line-by-line.
        If start_offset > 0, seek there and read only new content (incremental).
        Sets self.last_position to file offset when done (for incremental reads).
        """
        if not self.log_path or not os.path.isfile(self.log_path):
            logger.debug("iter_log_file: no file at %s", self.log_path)
            return
        with open(self.log_path, "r", errors="replace") as f:
            if start_offset > 0:
                f.seek(start_offset)
                logger.debug("Reading log from offset %s", start_offset)
            else:
                logger.debug("Parsing log file: %s", self.log_path)
            yield from self.iter_events(f)
            self.last_position = f.tell()


def load_keep_output_steps(request_path):
    """
    Load step names with KeepOutput==True from request.json.
    Request uses Step1, Step2, ...; job_report uses step1, step2, ...
    Returns set of step names (e.g. {"step4", "step5", "step6"}) or None if not found.
    """
    if not request_path or not os.path.isfile(request_path):
        return None
    try:
        with open(request_path) as f:
            req = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    steps = set()
    for i in range(1, 20):
        key = f"Step{i}"
        step_cfg = req.get(key)
        if not step_cfg:
            break
        if step_cfg.get("KeepOutput") is True:
            steps.add(f"step{i}")
    return steps if steps else None


class FrameworkJobReport:
    """
    Framework job report (create_report.py output). Find and extract file info.
    """

    @staticmethod
    def find(results_dir, cluster, proc):
        """
        Find the job_report JSON for (cluster, proc). Uses highest NumJobCompletions
        since that corresponds to the successful run (earlier ones were retries).
        Returns path or None.
        """
        prefix = f"job_report.{cluster}.{proc}."
        best_path, best_n = None, -1
        try:
            for name in os.listdir(results_dir):
                if name.startswith(prefix) and name.endswith(".json"):
                    suffix = name[len(prefix) : -len(".json")]
                    try:
                        n = int(suffix) if suffix else 0
                        if n > best_n:
                            best_n, best_path = n, os.path.join(results_dir, name)
                    except ValueError:
                        pass
        except OSError:
            pass
        return best_path

    @staticmethod
    def extract_files(report_path, keep_output_steps=None):
        """
        Extract output file information from job_report.json.
        Args:
            report_path: Path to job_report JSON.
            keep_output_steps: If set, only include outputs from these step names
                (e.g. {"step4", "step5"}). Used with request.json KeepOutput.
        Returns (list of dicts, error).
        """
        try:
            with open(report_path) as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            return [], str(e)

        steps = report.get("steps", {})
        files = []

        for step_name, step_data in steps.items():
            if not step_data:
                continue

            for mod_name, file_list in step_data.get("output", {}).items():
                if keep_output_steps is not None and step_name not in keep_output_steps:
                    continue
                for fi in file_list if isinstance(file_list, list) else []:
                    lfn = fi.get("lfn") or fi.get("LFN") or fi.get("logicalFileName")
                    pfn = fi.get("pfn") or fi.get("PFN") or fi.get("fileName") or fi.get("physicalFileName")
                    pnn = fi.get("pnn") or fi.get("PNN")
                    events = fi.get("events") or fi.get("EventsWritten")
                    size = fi.get("size")
                    if lfn or pfn:
                        files.append({
                            "lfn": lfn or "",
                            "pfn": pfn or "",
                            "pnn": pnn or "",
                            "step_name": step_name,
                            "events": int(events) if events is not None else None,
                            "size": int(size) if size is not None else None,
                            "module_label": mod_name,
                        })

        return files, None


class FileDB:
    """
    SQLite database for processed files. File-centric schema.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Create schema if not exists."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                condor_job_id TEXT NOT NULL,
                lfn TEXT,
                pfn TEXT,
                step_name TEXT,
                events INTEGER,
                size INTEGER,
                module_label TEXT,
                glidein_cmssite TEXT,
                pnn TEXT,
                job_exit_code INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(condor_job_id, lfn, pfn, step_name)
            );

            CREATE INDEX IF NOT EXISTS idx_files_lfn ON processed_files(lfn);
            CREATE INDEX IF NOT EXISTS idx_files_job ON processed_files(condor_job_id);
            CREATE INDEX IF NOT EXISTS idx_files_pnn ON processed_files(pnn);
        """)
        for col in ("glidein_cmssite", "pnn"):
            try:
                self.conn.execute("ALTER TABLE processed_files ADD COLUMN %s TEXT" % col)
            except sqlite3.OperationalError:
                pass
        self.conn.commit()

    def insert_files(self, condor_job_id, files, job_exit_code=0, glidein_cmssite=None):
        """Insert file records, ignoring duplicates (UNIQUE constraint)."""
        for f in files:
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO processed_files
                       (condor_job_id, lfn, pfn, step_name, events, size, module_label, glidein_cmssite, pnn, job_exit_code)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        condor_job_id,
                        f.get("lfn") or "",
                        f.get("pfn") or "",
                        f.get("step_name") or "",
                        f.get("events"),
                        f.get("size"),
                        f.get("module_label") or "",
                        glidein_cmssite or "",
                        f.get("pnn") or "",
                        job_exit_code,
                    ),
                )
            except sqlite3.IntegrityError:
                pass
        self.conn.commit()

    def process_terminated_job(
        self, results_dir, cluster, proc, return_value, glidein_cmssite=None,
        keep_output_steps=None, request_path=None,
    ):
        """On JOB_TERMINATED: find job_report, extract output files, store in DB. Returns (ok, result)."""
        report_path = FrameworkJobReport.find(results_dir, cluster, proc)
        if not report_path:
            logger.debug("Job %s.%s: no job_report found in %s", cluster, proc, results_dir)
            return False, "job_report not found"
        logger.debug("Job %s.%s: found report %s", cluster, proc, report_path)
        files, err = FrameworkJobReport.extract_files(report_path, keep_output_steps=keep_output_steps)
        if err:
            logger.debug("Job %s.%s: extract_files failed: %s", cluster, proc, err)
            return False, err
        if request_path:
            for f in files:
                if not (f.get("lfn") or f.get("LFN")):
                    built = build_lfn_for_file(f, request_path)
                    if built:
                        f["lfn"] = built
        self.insert_files(f"{cluster}.{proc}", files, job_exit_code=return_value, glidein_cmssite=glidein_cmssite)
        logger.debug("Job %s.%s: stored %d files (exit=%s, glidein_cmssite=%s)", cluster, proc, len(files), return_value, glidein_cmssite)
        return True, len(files)

    def close(self):
        self.conn.close()


class Monitor:
    """
    Micro Agent Monitor: coordinates log parsing and file DB.
    Holds condor log path, results dir, parser, and db; found once at init.
    """

    def __init__(self, log_path, results_dir, db_path, request_path=None):
        self.condor_log_file = os.path.abspath(log_path)
        self.results_dir = os.path.abspath(results_dir)
        self.db = FileDB(os.path.abspath(db_path))
        self.parser = CondorLogParser(self.condor_log_file)
        self.request_path = os.path.abspath(request_path) if request_path else None
        self.keep_output_steps = load_keep_output_steps(self.request_path) if self.request_path else None
        if self.keep_output_steps:
            logger.info("KeepOutput steps from %s: %s", request_path, sorted(self.keep_output_steps))

    def _handle_terminated_job(self, cluster, proc, extra, processed_jobs=None):
        """
        Process a JOB_TERMINATED event. If processed_jobs is provided, skip if already seen.
        Returns (1 if processed, 0 otherwise).
        """
        if processed_jobs is not None:
            key = (cluster, proc)
            if key in processed_jobs:
                logger.debug("Job %s.%s already processed, skipping", cluster, proc)
                return 0
            processed_jobs.add(key)
        return_value = int(extra.get("ReturnValue", -1))
        glidein_cmssite = extra.get("JOB_GLIDEIN_Site") or None
        ok, result = self.db.process_terminated_job(
            self.results_dir, cluster, proc, return_value, glidein_cmssite=glidein_cmssite,
            keep_output_steps=self.keep_output_steps,
            request_path=self.request_path,
        )
        if ok:
            logger.info("Job %s.%s terminated (exit=%s): stored %s files", cluster, proc, return_value, result)
            return 1
        logger.warning("Job %s.%s: %s", cluster, proc, result)
        return 0

    def run_once(self):
        """Single pass over log file."""
        if not os.path.isfile(self.condor_log_file):
            logger.error("Log file not found: %s", self.condor_log_file)
            return 1

        processed = 0
        for ev in self.parser.iter_log_file():
            event_code, cluster, proc, subproc, timestamp, message, extra = ev
            logger.debug("Event %s (cluster=%s.%s.%s): %s", event_code, cluster, proc, subproc, message)
            if event_code == ULOG_JOB_TERMINATED:
                processed += self._handle_terminated_job(cluster, proc, extra)
        logger.info("Processed %s terminated jobs", processed)
        return 0

    def run_daemon(self, poll_interval=10):
        """Poll log file and process new events. Reads only new lines; full re-read every FULL_REREAD_INTERVAL."""
        logger.info("Daemon mode: watching %s (poll every %ss, full re-read every %ss)", self.condor_log_file, poll_interval, FULL_REREAD_INTERVAL)
        last_position = 0
        last_full_reread = time.time()
        processed_jobs = set()

        while True:
            try:
                if not os.path.isfile(self.condor_log_file):
                    logger.debug("Log file not yet present, sleeping %ss", poll_interval)
                    time.sleep(poll_interval)
                    continue
                size = os.path.getsize(self.condor_log_file)
                if size > last_position:
                    do_full = (time.time() - last_full_reread) >= FULL_REREAD_INTERVAL
                    start_offset = 0 if do_full else last_position
                    if do_full:
                        logger.debug("Full re-read (interval %ss elapsed)", FULL_REREAD_INTERVAL)
                        last_full_reread = time.time()
                    else:
                        logger.debug("Reading new content from offset %s", last_position)


                    for ev in self.parser.iter_log_file(start_offset=start_offset):
                        event_code, cluster, proc, subproc, timestamp, message, extra = ev
                        if event_code not in (ULOG_IMAGE_SIZE, ULOG_JOB_AD):
                            logger.debug("Event %s (cluster=%s.%s.%s): %s", event_code, cluster, proc, subproc, message)
                        if event_code == ULOG_JOB_TERMINATED:
                            self._handle_terminated_job(cluster, proc, extra, processed_jobs=processed_jobs)
                    last_position = self.parser.last_position
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                logger.info("Stopping daemon (interrupted)")
                break
            except Exception as e:
                logger.exception("Error: %s", e)
                time.sleep(poll_interval)

        return 0

    def close(self):
        self.db.close()


def setup_logging(log_file=None, verbose=False):
    """
    Configure logging. Stdout gets INFO (relevant); log file gets DEBUG (everything).
    With --verbose, stdout also shows DEBUG.
    """
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    stdout = logging.StreamHandler(sys.stdout)
    stdout.setLevel(logging.DEBUG if verbose else logging.INFO)
    stdout.setFormatter(fmt)
    logger.addHandler(stdout)

    if log_file:
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except OSError as e:
            logger.warning("Could not open log file %s: %s", log_file, e)


def parse_args():
    p = argparse.ArgumentParser(
        description="Micro Agent Monitor: parse condor logs, store file info in SQLite.",
        epilog="Run from the directory containing log/ and results/ (or use absolute paths).",
    )
    p.add_argument(
        "--log",
        required=True,
        help="Path to condor log file (e.g. log/run.10372180)",
    )
    p.add_argument(
        "--results-dir",
        default="results",
        help="Directory containing job_report.<C>.<P>.<N>.json (default: results)",
    )
    p.add_argument(
        "--db",
        default="micro_agent.db",
        help="SQLite database path (default: micro_agent.db)",
    )
    p.add_argument(
        "--request",
        metavar="FILE",
        help="Request.json path; when given, only store outputs from steps with KeepOutput==True",
    )
    p.add_argument(
        "--log-file",
        metavar="FILE",
        help="Write log to file (DEBUG level); stdout keeps INFO",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show DEBUG on stdout (default: INFO only)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Single pass over log, then exit (default: daemon mode)",
    )
    p.add_argument(
        "--poll",
        type=int,
        default=10,
        help="Poll interval in seconds for daemon mode (default: 10)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(log_file=args.log_file, verbose=args.verbose)

    monitor = Monitor(args.log, args.results_dir, args.db, request_path=args.request)
    try:
        if args.once:
            return monitor.run_once()
        return monitor.run_daemon(args.poll)
    finally:
        monitor.close()


if __name__ == "__main__":
    sys.exit(main())
