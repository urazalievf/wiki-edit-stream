"""The SQL runner's statement splitting and placeholder rendering.

Both are pure string handling, so they are tested without Flink or Kafka.
"""

from __future__ import annotations

import pytest

from flink_jobs.runner import SQL_DIR, render, split_statements
from ingest.config import load_config


def test_semicolon_inside_a_string_does_not_split():
    sql = "CREATE TABLE a (x INT) WITH ('key' = 'a;b'); SELECT 1"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert "'a;b'" in statements[0]


def test_line_comments_are_stripped():
    sql = "-- drop table; really\nSELECT 1"
    assert split_statements(sql) == ["SELECT 1"]


def test_statement_set_stays_one_statement():
    """Splitting a statement set would turn one job into several, each
    re-reading the source topic."""
    sql = """
    EXECUTE STATEMENT SET
    BEGIN
    INSERT INTO a SELECT 1;
    INSERT INTO b SELECT 2;
    END;
    """
    statements = split_statements(sql)
    assert len(statements) == 1
    assert statements[0].count("INSERT INTO") == 2
    assert statements[0].rstrip().endswith("END")


def test_unterminated_statement_set_is_rejected():
    with pytest.raises(ValueError, match="unterminated"):
        split_statements("EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT 1;")


def test_render_substitutes_known_placeholders():
    config = load_config()
    rendered = render("bootstrap=${bootstrap} topic=${topic_raw}", config)
    assert config.bootstrap in rendered
    assert "${" not in rendered


def test_render_rejects_unknown_placeholder():
    with pytest.raises(KeyError, match="unknown placeholder"):
        render("${not_a_real_key}", load_config())


def test_window_interval_renders_as_valid_sql():
    """INTERVAL '1 MINUTE' parses as nothing; it must be INTERVAL '1' MINUTE."""
    rendered = render("INTERVAL '${window_minutes}' MINUTE", load_config())
    assert rendered == "INTERVAL '1' MINUTE"


@pytest.mark.parametrize("name", ["01_clean.sql", "02_stats.sql", "03_edit_wars.sql"])
def test_shipped_jobs_render_and_split(name):
    config = load_config()
    statements = split_statements(render((SQL_DIR / name).read_text(), config))
    assert statements, f"{name} produced no statements"
    assert all("${" not in statement for statement in statements)
    assert any(statement.upper().startswith("CREATE TABLE") for statement in statements)
    assert any("INSERT INTO" in statement.upper() for statement in statements)


def test_reserved_column_names_are_backticked():
    """Flink rejects a bare `user`, `type`, `stream` or `comment` as a column
    name - but TIMESTAMP is also a type keyword, so a blanket word search
    gives false positives. This checks the position that actually matters: the
    identifier at the start of a column definition.
    """
    import re

    config = load_config()
    reserved = {
        "user",
        "timestamp",
        "type",
        "stream",
        "comment",
        "namespace",
        "new",
        "old",
        "value",
        "date",
        "time",
    }
    types = r"(STRING|INT|BIGINT|BOOLEAN|DOUBLE|TIMESTAMP|TIMESTAMP_LTZ|ROW|DECIMAL)"
    offenders = []

    for name in ("01_clean.sql", "02_stats.sql", "03_edit_wars.sql"):
        sql = render((SQL_DIR / name).read_text(), config)
        for line in sql.splitlines():
            line = line.split("--")[0]
            match = re.match(rf"\s+(\w+)\s+{types}\b", line, re.IGNORECASE)
            if match and match.group(1).lower() in reserved:
                offenders.append(f"{name}: {line.strip()}")

    assert not offenders, "unquoted reserved column name(s): " + "; ".join(offenders)


def test_shipped_sql_quotes_the_user_column():
    """Positive check: the column exists and is always backticked."""
    config = load_config()
    for name in ("01_clean.sql", "02_stats.sql", "03_edit_wars.sql"):
        sql = render((SQL_DIR / name).read_text(), config)
        assert "`user`" in sql, name
