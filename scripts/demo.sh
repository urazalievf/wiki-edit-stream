#!/usr/bin/env bash
# End-to-end demo: live edits -> Kafka -> Flink SQL -> output topics.
#
# Everything runs concurrently, which is the point. On an unbounded stream a
# window only closes once the watermark passes its end, and the watermark only
# advances when newer events arrive - so the aggregation job needs the
# ingestor to keep feeding it. Running the jobs against a static topic would
# show nothing, and that would be a property of streaming, not a bug.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DURATION="${1:-150}"
PY="$ROOT/.venv/bin/python"
export PYTHONPATH="$ROOT"
if JDK="$ROOT/scripts/find-jdk.sh"; then export JAVA_HOME="$($JDK 2>/dev/null || true)"; fi

mkdir -p .state logs
: > logs/ingest.log; : > logs/clean.log; : > logs/stats.log; : > logs/wars.log

echo "==> broker"
./scripts/kafka.sh start >/dev/null || { echo "broker failed to start"; exit 1; }
./scripts/create-topics.sh >/dev/null
echo "    ready"

# Jobs go up first so nothing that arrives is missed, then the ingestor feeds
# them for slightly longer so the last window has a chance to close.
echo "==> flink jobs (${DURATION}s)"
$PY -m flink_jobs.submit 01_clean.sql    --wait "$DURATION" > logs/clean.log 2>&1 &
CLEAN=$!
$PY -m flink_jobs.submit 02_stats.sql    --wait "$DURATION" > logs/stats.log 2>&1 &
STATS=$!
$PY -m flink_jobs.submit 03_edit_wars.sql --wait "$DURATION" > logs/wars.log 2>&1 &
WARS=$!

echo "==> ingest (live Wikimedia stream)"
$PY -m ingest.main --duration "$((DURATION - 20))" > logs/ingest.log 2>&1 &
INGEST=$!

wait $INGEST; echo "    ingest finished: $(grep -c . logs/ingest.log) log lines"
wait $CLEAN $STATS $WARS 2>/dev/null

echo
echo "==> results"
./scripts/show-offsets.sh
