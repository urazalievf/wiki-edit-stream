#!/usr/bin/env bash
# Create the virtualenv.
#
# apache-flink is installed with --no-deps on purpose: its pinned
# apache-beam range has no wheels for current Python/arm64, and beam is only
# needed to run Python UDFs in a separate process. Every job here is pure SQL,
# so the JVM does all the work and beam is never imported. Its other runtime
# dependencies are listed explicitly in requirements.txt.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m venv .venv
.venv/bin/pip install --upgrade pip setuptools wheel

grep -v '^apache-flink' requirements-dev.txt | grep -v '^-r' > /tmp/wes-deps.txt
.venv/bin/pip install -r /tmp/wes-deps.txt
.venv/bin/pip install --no-deps apache-flink==2.2.0 apache-flink-libraries==2.2.0
.venv/bin/pip install pytest==8.3.3 ruff==0.6.9

echo
echo "installed. next:"
echo "  ./scripts/fetch-connectors.sh"
echo "  make demo"
