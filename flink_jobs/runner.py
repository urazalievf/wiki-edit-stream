"""Submit a Flink SQL file.

The jobs themselves are plain `.sql` - readable, reviewable, and portable to
`sql-client` or a real cluster without change. This module only handles the
things SQL cannot: locating connector jars, applying the configuration from
`conf/pipeline.yaml`, and substituting `${...}` placeholders so one file runs
against any broker or topic set.

No Python UDFs anywhere. That keeps the job entirely inside the JVM, which
means no Python worker harness, and the same SQL runs on a cluster unchanged.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from ingest.config import REPO_ROOT, Config, load_config

log = logging.getLogger("flink")

SQL_DIR = Path(__file__).resolve().parent / "sql"
LIB_DIR = REPO_ROOT / "lib"

# PyFlink starts a Beam loopback worker for Python UDFs when running locally.
# These jobs have none, so it is skipped - it is the only reason PyFlink would
# need apache-beam at all.
os.environ.setdefault("_python_worker_execution_mode", "process")


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements.

    Quote-aware, and treats `EXECUTE STATEMENT SET BEGIN ... END` as one
    statement - a statement set is how several INSERTs share a single source,
    so splitting it on semicolons would turn one job into several, each
    re-reading the topic.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_string = False
    index = 0

    while index < len(sql):
        char = sql[index]
        if in_string:
            buffer.append(char)
            if char == "'":
                if sql[index + 1 : index + 2] == "'":
                    buffer.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue

        if char == "'":
            in_string = True
            buffer.append(char)
        elif sql[index : index + 2] == "--":
            newline = sql.find("\n", index)
            index = len(sql) if newline == -1 else newline
            continue
        elif char == ";":
            statements.append("".join(buffer))
            buffer = []
        else:
            buffer.append(char)
        index += 1
    statements.append("".join(buffer))

    cleaned = [statement.strip() for statement in statements if statement.strip()]
    return _merge_statement_sets(cleaned)


def _merge_statement_sets(statements: list[str]) -> list[str]:
    merged: list[str] = []
    pending: list[str] | None = None

    for statement in statements:
        upper = statement.upper()
        if pending is None and "STATEMENT SET" in upper and "BEGIN" in upper:
            pending = [statement]
            continue
        if pending is not None:
            pending.append(statement)
            if upper.strip() == "END":
                merged.append(";\n".join(pending[:-1]) + ";\nEND")
                pending = None
            continue
        merged.append(statement)

    if pending is not None:
        raise ValueError("unterminated EXECUTE STATEMENT SET block")
    return merged


def render(sql: str, config: Config) -> str:
    """Substitute ${placeholders} from configuration."""
    values = {
        "bootstrap": config.bootstrap,
        "topic_raw": config.topic("raw"),
        "topic_clean": config.topic("clean"),
        "topic_dlq": config.topic("dlq"),
        "topic_stats": config.topic("stats"),
        "topic_alerts": config.topic("alerts"),
        "max_out_of_orderness": str(config.flink["max_out_of_orderness_s"]),
        "window_minutes": str(config.flink["window_minutes"]),
        # Bounded mode turns the Kafka source into a finite one: it stops at
        # the current end of the topic and emits a final watermark, so every
        # open window closes. Without it a demo never sees its last window,
        # because on an unbounded stream the watermark only advances when new
        # events arrive. Streaming semantics are identical either way.
        "scan_bounded": (
            "'scan.bounded.mode' = 'latest-offset',"
            if os.getenv("WES_BOUNDED") == "1"
            else ""
        ),
    }

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise KeyError(f"unknown placeholder ${{{key}}} in SQL")
        return values[key]

    return re.sub(r"\$\{(\w+)\}", replace, sql)


def connector_jars() -> list[str]:
    jars = sorted(LIB_DIR.glob("*.jar"))
    if not jars:
        raise FileNotFoundError(
            f"no connector jars in {LIB_DIR}; run scripts/fetch-connectors.sh"
        )
    return [path.resolve().as_uri() for path in jars]


def build_table_env(config: Config, streaming: bool = True):
    from pyflink.common import Configuration
    from pyflink.table import EnvironmentSettings, TableEnvironment

    settings = Configuration()
    settings.set_string("pipeline.jars", ";".join(connector_jars()))
    settings.set_string("parallelism.default", str(config.flink["parallelism"]))
    # Checkpointing is what makes the Kafka sink's exactly-once possible; the
    # interval is a latency/throughput trade, not a correctness one.
    settings.set_string(
        "execution.checkpointing.interval",
        f"{config.flink['checkpoint_interval_ms']}ms",
    )
    settings.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    settings.set_string("table.exec.source.idle-timeout", "10s")

    env_settings = (
        EnvironmentSettings.new_instance().in_streaming_mode()
        if streaming
        else EnvironmentSettings.new_instance().in_batch_mode()
    )
    return TableEnvironment.create(env_settings.with_configuration(settings).build())


def run_file(name: str, config: Config | None = None, dry_run: bool = False):
    """Execute every statement in a SQL file, in order."""
    config = config or load_config()
    path = SQL_DIR / (name if name.endswith(".sql") else f"{name}.sql")
    if not path.exists():
        raise FileNotFoundError(f"no such job: {path}")

    statements = split_statements(render(path.read_text(), config))
    log.info("%s: %d statement(s)", path.name, len(statements))

    if dry_run:
        for statement in statements:
            head = statement.strip().splitlines()[0][:90]
            log.info("  would execute: %s ...", head)
        return None

    t_env = build_table_env(config)
    result = None
    for statement in statements:
        head = statement.strip().splitlines()[0][:90]
        log.info("  executing: %s ...", head)
        result = t_env.execute_sql(statement)
    return result
