#!/usr/bin/env bash
# Print how much data sits in each topic. End offsets, so transaction markers
# from the exactly-once sinks are included in the count.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
if JDK="$("$ROOT/scripts/find-jdk.sh" 2>/dev/null)"; then export JAVA_HOME="$JDK"; fi

for topic in wiki.edits.raw wiki.edits.clean wiki.edits.dlq wiki.stats.per_wiki_1m wiki.alerts.edit_wars; do
    total=$("$KAFKA_HOME/bin/kafka-get-offsets" --bootstrap-server "$BOOTSTRAP" --topic "$topic" 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}')
    printf "  %-28s %s\n" "$topic" "$total"
done
