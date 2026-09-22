#!/usr/bin/env bash
# Create the topics the pipeline uses. Idempotent.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"

if JDK="$("$ROOT/scripts/find-jdk.sh" 2>/dev/null)"; then export JAVA_HOME="$JDK"; fi

create() {
    local topic="$1" partitions="$2" extra="${3:-}"
    "$KAFKA_HOME/bin/kafka-topics" --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists --topic "$topic" \
        --partitions "$partitions" --replication-factor 1 ${extra} >/dev/null
    echo "  $topic (partitions=$partitions)${extra:+ $extra}"
}

echo "creating topics on $BOOTSTRAP"
create wiki.edits.raw 3
create wiki.edits.clean 3
create wiki.edits.dlq 1
# Aggregates are keyed and compacted: only the latest value per key matters.
create wiki.stats.per_wiki_1m 3 "--config cleanup.policy=compact"
create wiki.alerts.edit_wars 1
