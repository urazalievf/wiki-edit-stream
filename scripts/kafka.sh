#!/usr/bin/env bash
# Start, stop and inspect the project-local Kafka broker.
#
# Kafka 4 is KRaft-only, so the log directory must be formatted with a cluster
# id before first start. That is done here automatically and recorded in
# .kafka-data, which is why the broker survives a restart but `reset` wipes it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
CONFIG="$ROOT/conf/kafka/server.properties"
DATA="$ROOT/.kafka-data"
PIDFILE="$ROOT/.kafka-data/broker.pid"
LOGFILE="$ROOT/.kafka-data/broker.log"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"

# Kafka 4 needs Java 17+; prefer a supported JDK if the default is newer.
if [[ -x "$ROOT/scripts/find-jdk.sh" ]]; then
    if JDK="$("$ROOT/scripts/find-jdk.sh" 2>/dev/null)"; then
        export JAVA_HOME="$JDK"
        export PATH="$JAVA_HOME/bin:$PATH"
    fi
fi

bin() { echo "$KAFKA_HOME/bin/$1"; }

case "${1:-}" in
  start)
    if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "broker already running (pid $(cat "$PIDFILE"))"; exit 0
    fi
    mkdir -p "$DATA"
    if [[ ! -f "$DATA/meta.properties" ]]; then
        CLUSTER_ID="$("$(bin kafka-storage)" random-uuid)"
        echo "formatting KRaft storage, cluster id $CLUSTER_ID"
        "$(bin kafka-storage)" format --cluster-id "$CLUSTER_ID" --config "$CONFIG" --standalone >/dev/null
    fi
    cd "$ROOT"
    nohup "$(bin kafka-server-start)" "$CONFIG" > "$LOGFILE" 2>&1 &
    echo $! > "$PIDFILE"
    echo -n "starting broker"
    for _ in $(seq 1 45); do
        if "$(bin kafka-broker-api-versions)" --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
            echo " - ready on $BOOTSTRAP"; exit 0
        fi
        echo -n "."; sleep 1
    done
    echo " - FAILED, see $LOGFILE"; tail -20 "$LOGFILE"; exit 1
    ;;
  stop)
    if [[ -f "$PIDFILE" ]]; then
        kill "$(cat "$PIDFILE")" 2>/dev/null || true
        rm -f "$PIDFILE"
        echo "broker stopped"
    else
        echo "broker not running"
    fi
    ;;
  status)
    if "$(bin kafka-broker-api-versions)" --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
        echo "broker up on $BOOTSTRAP"
    else
        echo "broker down"; exit 1
    fi
    ;;
  topics)
    "$(bin kafka-topics)" --bootstrap-server "$BOOTSTRAP" --list
    ;;
  describe)
    "$(bin kafka-topics)" --bootstrap-server "$BOOTSTRAP" --describe
    ;;
  tail)
    "$(bin kafka-console-consumer)" --bootstrap-server "$BOOTSTRAP" \
        --topic "${2:?usage: kafka.sh tail <topic>}" --max-messages "${3:-5}" --from-beginning
    ;;
  reset)
    "$0" stop || true
    rm -rf "$DATA"
    echo "broker data wiped"
    ;;
  *)
    echo "usage: kafka.sh {start|stop|status|topics|describe|tail <topic> [n]|reset}"; exit 1
    ;;
esac
