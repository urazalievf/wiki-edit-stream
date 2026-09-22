"""CLI for submitting the SQL jobs."""

from __future__ import annotations

import argparse
import logging
import sys

from ingest.config import load_config

from .runner import SQL_DIR, run_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="submit", description="Run a Flink SQL job")
    parser.add_argument("job", nargs="?", help="job file, e.g. 01_clean.sql")
    parser.add_argument("--list", action="store_true", help="list available jobs")
    parser.add_argument(
        "--dry-run", action="store_true", help="parse without submitting"
    )
    parser.add_argument("--wait", type=float, help="run for N seconds, then cancel")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        stream=sys.stdout,
    )

    if args.list or not args.job:
        for path in sorted(SQL_DIR.glob("*.sql")):
            first = next(
                (
                    line.lstrip("- ").strip()
                    for line in path.read_text().splitlines()
                    if line.startswith("--") and line.strip() != "--"
                ),
                "",
            )
            print(f"{path.name:<28} {first}")
        return 0

    config = load_config()
    result = run_file(args.job, config, dry_run=args.dry_run)

    if args.dry_run or result is None:
        return 0

    job_client = result.get_job_client()
    if job_client is None:
        return 0

    logging.getLogger("flink").info("job submitted: %s", job_client.get_job_id())
    if args.wait:
        import time

        time.sleep(args.wait)
        logging.getLogger("flink").info("cancelling after %.0fs", args.wait)
        job_client.cancel().result()
    else:
        job_client.get_job_execution_result().result()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
