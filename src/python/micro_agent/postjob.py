#!/usr/bin/env python3
"""
DAG POST script run after each node job completes.

Placeholder for user logic. For now: exits 1 on first invocation (triggers
DEFER retry after 6h), exits 0 on second invocation.

Receives $JOB (node name) as first argument from DAG SCRIPT line.
"""
import os
import sys


def main():
    job_name = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    state_file = os.path.join(os.getcwd(), f"postjob_{job_name}_state")

    if os.path.exists(state_file):
        sys.exit(0)
    else:
        with open(state_file, "w") as f:
            f.write("1")
        sys.exit(1)


if __name__ == "__main__":
    main()
