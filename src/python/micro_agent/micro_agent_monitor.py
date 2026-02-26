#!/usr/bin/env python3
"""
Micro Agent Monitor (MAM): monitors HTCondor job log files and records file-level
processing information in a local SQLite database.

Parses the condor user log (JDL Log macro, e.g. log/run.<Cluster>) for job events.
On JOB_TERMINATED (success), reads the framework job_report JSON and stores information
about processed files (input/output) in SQLite. File-centric, not job-centric.

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

logger = logging.getLogger("micro_agent_monitor")


# HTCondor user log event codes (from Job Event Log Codes)
ULOG_SUBMIT = 0
ULOG_EXECUTE = 1
ULOG_JOB_TERMINATED = 5
ULOG_JOB_ABORTED = 9
ULOG_JOB_EVICTED = 4


class CondorLogParser:
    """
    Parse HTCondor user log files and framework job reports.
    Event format: NNN (Cluster.Proc.Subproc) YYYY-MM-DD HH:MM:SS message
    Event codes: 0=submit, 1=execute, 5=terminated, 28=job ad (supplements other events).
    """

    def __init__(self, log_path):
        self.log_path = os.path.abspath(log_path) if log_path else None

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
                    and next_parsed[0] == 28
                    and (cluster, proc) == (next_parsed[1], next_parsed[2])
                ):
                    line = self._read_key_values_until_stop(line_iter, extra)

            yield (event_code, cluster, proc, subproc, timestamp, message, extra)

    def iter_log_file(self):
        """Yield events from log file. Streams line-by-line."""
        if not self.log_path or not os.path.isfile(self.log_path):
            logger.debug("iter_log_file: no file at %s", self.log_path)
            return
        logger.debug("Parsing log file: %s", self.log_path)
        with open(self.log_path, "r", errors="replace") as f:
            yield from self.iter_events(f)


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
    def extract_files(report_path):
        """
        Extract file information from job_report.json.
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

            for src in step_data.get("input", {}).get("source", []):
                lfn = src.get("lfn") or src.get("LFN") or src.get("logicalFileName")
                pfn = src.get("pfn") or src.get("PFN") or src.get("physicalFileName")
                events = src.get("events") or src.get("EventsRead") or 0
                size = src.get("size")
                module = src.get("module_label", "source")
                if lfn or pfn:
                    files.append({
                        "lfn": lfn or "",
                        "pfn": pfn or "",
                        "step_name": step_name,
                        "role": "input",
                        "events": int(events) if events is not None else None,
                        "size": int(size) if size is not None else None,
                        "module_label": module,
                    })

            for mod_name, file_list in step_data.get("output", {}).items():
                for fi in file_list if isinstance(file_list, list) else []:
                    lfn = fi.get("lfn") or fi.get("LFN") or fi.get("logicalFileName")
                    pfn = fi.get("pfn") or fi.get("PFN") or fi.get("fileName") or fi.get("physicalFileName")
                    events = fi.get("events") or fi.get("EventsWritten")
                    size = fi.get("size")
                    if lfn or pfn:
                        files.append({
                            "lfn": lfn or "",
                            "pfn": pfn or "",
                            "step_name": step_name,
                            "role": "output",
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
                role TEXT,
                events INTEGER,
                size INTEGER,
                module_label TEXT,
                rse TEXT,
                job_exit_code INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(condor_job_id, lfn, pfn, step_name, role)
            );

            CREATE INDEX IF NOT EXISTS idx_files_lfn ON processed_files(lfn);
            CREATE INDEX IF NOT EXISTS idx_files_job ON processed_files(condor_job_id);
            CREATE INDEX IF NOT EXISTS idx_files_rse ON processed_files(rse);
        """)
        self.conn.commit()

    def insert_files(self, condor_job_id, files, job_exit_code=0, rse=None):
        """Insert file records, ignoring duplicates (UNIQUE constraint)."""
        for f in files:
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO processed_files
                       (condor_job_id, lfn, pfn, step_name, role, events, size, module_label, rse, job_exit_code)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        condor_job_id,
                        f.get("lfn") or "",
                        f.get("pfn") or "",
                        f.get("step_name") or "",
                        f.get("role") or "",
                        f.get("events"),
                        f.get("size"),
                        f.get("module_label") or "",
                        rse or "",
                        job_exit_code,
                    ),
                )
            except sqlite3.IntegrityError:
                pass
        self.conn.commit()

    def process_terminated_job(self, results_dir, cluster, proc, return_value, rse=None):
        """On JOB_TERMINATED: find job_report, extract files, store in DB. Returns (ok, result)."""
        report_path = FrameworkJobReport.find(results_dir, cluster, proc)
        if not report_path:
            logger.debug("Job %s.%s: no job_report found in %s", cluster, proc, results_dir)
            return False, "job_report not found"
        logger.debug("Job %s.%s: found report %s", cluster, proc, report_path)
        files, err = FrameworkJobReport.extract_files(report_path)
        if err:
            logger.debug("Job %s.%s: extract_files failed: %s", cluster, proc, err)
            return False, err
        self.insert_files(f"{cluster}.{proc}", files, job_exit_code=return_value, rse=rse)
        logger.debug("Job %s.%s: stored %d files (exit=%s, rse=%s)", cluster, proc, len(files), return_value, rse)
        return True, len(files)

    def close(self):
        self.conn.close()


class Monitor:
    """
    Micro Agent Monitor: coordinates log parsing and file DB.
    Holds condor log path, results dir, parser, and db; found once at init.
    """

    def __init__(self, log_path, results_dir, db_path):
        self.condor_log_file = os.path.abspath(log_path)
        self.results_dir = os.path.abspath(results_dir)
        self.db = FileDB(os.path.abspath(db_path))
        self.parser = CondorLogParser(self.condor_log_file)

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
                return_value = int(extra.get("ReturnValue", -1))
                rse = extra.get("JOB_Site") or extra.get("JOB_GLIDEIN_Site") or None
                ok, result = self.db.process_terminated_job(
                    self.results_dir, cluster, proc, return_value, rse=rse
                )
                if ok:
                    processed += 1
                    logger.info("Job %s.%s terminated (exit=%s): stored %s files", cluster, proc, return_value, result)
                else:
                    logger.warning("Job %s.%s: %s", cluster, proc, result)

        logger.info("Processed %s terminated jobs", processed)
        return 0

    def run_daemon(self, poll_interval=10):
        """Poll log file and process new events. Re-reads when file grows; skips already-processed jobs."""
        logger.info("Daemon mode: watching %s (poll every %ss)", self.condor_log_file, poll_interval)
        last_size = 0
        processed_jobs = set()

        while True:
            try:
                if not os.path.isfile(self.condor_log_file):
                    logger.debug("Log file not yet present, sleeping %ss", poll_interval)
                    time.sleep(poll_interval)
                    continue
                size = os.path.getsize(self.condor_log_file)
                if size > last_size:
                    logger.debug("Log file grew (%s -> %s bytes), re-parsing", last_size, size)
                    for ev in self.parser.iter_log_file():
                        event_code, cluster, proc, subproc, timestamp, message, extra = ev
                        logger.debug("Event %s (cluster=%s.%s.%s): %s", event_code, cluster, proc, subproc, message)
                        if event_code == ULOG_JOB_TERMINATED:
                            key = (cluster, proc)
                            if key in processed_jobs:
                                logger.debug("Job %s.%s already processed, skipping", cluster, proc)
                                continue
                            processed_jobs.add(key)
                            return_value = int(extra.get("ReturnValue", -1))
                            rse = extra.get("JOB_Site") or extra.get("JOB_GLIDEIN_Site") or None
                            ok, result = self.db.process_terminated_job(
                                self.results_dir, cluster, proc, return_value, rse=rse
                            )
                            if ok:
                                logger.info("Job %s.%s: stored %s files", cluster, proc, result)
                            else:
                                logger.warning("Job %s.%s: %s", cluster, proc, result)
                    last_size = size
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

    monitor = Monitor(args.log, args.results_dir, args.db)
    try:
        if args.once:
            return monitor.run_once()
        return monitor.run_daemon(args.poll)
    finally:
        monitor.close()


if __name__ == "__main__":
    sys.exit(main())
