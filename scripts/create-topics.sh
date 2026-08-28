#!/usr/bin/env bash
# Topic creation is explicit on purpose: auto-create gives you one partition and
# whatever the broker defaults happen to be. Partition counts are chosen so the
# key (symbol / pair / region) spreads across partitions for parallel consumption.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

create() {
  local topic="$1" partitions="$2" retention_ms="$3"
  echo "==> ${topic} (partitions=${partitions}, retention=${retention_ms}ms)"
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}" \
    --config "cleanup.policy=delete"
}

echo "waiting for ${BOOTSTRAP} ..."
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

# raw topics keep a week so the batch layer and any replay can reread them
create "commodity.prices.raw" 3 604800000
create "fx.rates.raw"         3 604800000
create "weather.regions.raw"  3 604800000
# derived metrics are cheap to recompute, so they expire sooner
create "commodity.metrics"    3 172800000
# alerts are the interesting history - keep them a month
create "alerts.flagged"       1 2592000000

echo
echo "topics now present:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
