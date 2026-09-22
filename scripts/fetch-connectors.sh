#!/usr/bin/env bash
# Download the jars Flink SQL needs into lib/.
#
# Two of them, and the second one is the non-obvious part: the Kafka
# connector's fat jar shades kafka-clients but NOT the compression codecs, so
# a topic written with zstd fails at read time with
# NoClassDefFoundError: com/github/luben/zstd/ZstdOutputStreamNoFinalizer.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="$ROOT/lib"
MAVEN="https://repo1.maven.org/maven2"

FLINK_KAFKA_VERSION="${FLINK_KAFKA_VERSION:-5.0.0-2.2}"
ZSTD_VERSION="${ZSTD_VERSION:-1.5.7-6}"

mkdir -p "$LIB"

fetch() {
    local url="$1" dest="$2"
    if [[ -f "$dest" ]]; then
        echo "  have $(basename "$dest")"; return
    fi
    echo "  fetching $(basename "$dest")"
    curl -fsSL -o "$dest" "$url"
}

echo "connector jars -> $LIB"
fetch "$MAVEN/org/apache/flink/flink-sql-connector-kafka/$FLINK_KAFKA_VERSION/flink-sql-connector-kafka-$FLINK_KAFKA_VERSION.jar" \
      "$LIB/flink-sql-connector-kafka-$FLINK_KAFKA_VERSION.jar"
fetch "$MAVEN/com/github/luben/zstd-jni/$ZSTD_VERSION/zstd-jni-$ZSTD_VERSION.jar" \
      "$LIB/zstd-jni-$ZSTD_VERSION.jar"
