#!/usr/bin/env bash
# Run inside the Kafka container; prefix is mandatory to isolate environments.
set -euo pipefail
prefix=${1:?Usage: init-topics.sh environment-prefix}
[[ "$prefix" =~ ^[a-z][a-z0-9-]{0,79}$ ]] || { echo 'Invalid environment prefix' >&2; exit 1; }
for spec in subscription-changes:1 prices:16 delivery-requests:16; do
  name="$prefix-${spec%:*}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$name" --partitions "${spec#*:}" --replication-factor 1 \
    --config min.insync.replicas=1 --config flush.messages=1 \
    --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes=-1
  # Existing topics are never silently repartitioned or reconfigured.
  description=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$name")
  header=${description%%$'\n'*}
  [[ "$header" == *$'\tPartitionCount: '"${spec#*:}"$'\tReplicationFactor: 1\t'* ]] || {
    echo "Unexpected partitions/RF: $header" >&2; exit 1;
  }
  configs=${header#*Configs: }
  for setting in min.insync.replicas=1 flush.messages=1 cleanup.policy=delete retention.ms=-1 retention.bytes=-1; do
    [[ ",$configs," == *",$setting,"* ]] || { echo "Config drift: $name $setting" >&2; exit 1; }
  done
  printf '%s\n' "$header"
done
